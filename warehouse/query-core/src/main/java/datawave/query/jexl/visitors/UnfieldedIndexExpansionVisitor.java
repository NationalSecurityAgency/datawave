package datawave.query.jexl.visitors;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.data.type.Type;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.EmptyUnfieldedTermExpansionException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.lookups.EmptyIndexLookup;
import datawave.query.jexl.lookups.FieldExpansionIndexLookup;
import datawave.query.jexl.lookups.IndexLookup;
import datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import jline.internal.Preconditions;

/**
 * Visits a Jexl tree, looks for unfielded terms, and replaces them with fielded terms from the index
 */
public class UnfieldedIndexExpansionVisitor extends RegexIndexExpansionVisitor {
    private static final Logger log = LoggerFactory.getLogger(UnfieldedIndexExpansionVisitor.class);

    protected Set<String> expansionFields;
    protected Set<Type<?>> allTypes;

    // The constructor should not be made public so that we can ensure that the executor is setup and shutdown correctly
    protected UnfieldedIndexExpansionVisitor(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper)
                    throws TableNotFoundException, IllegalAccessException, InstantiationException {
        super(config, scannerFactory, helper, null, "FieldNameIndexExpansion");

        this.expansionFields = helper.getExpansionFields(config.getDatatypeFilter());
        if (this.expansionFields == null) {
            this.expansionFields = new HashSet<>();
        }

        this.allTypes = helper.getAllDatatypes();
        this.stage = "field";
    }

    /**
     * Visits the Jexl script, looks for unfielded terms, and replaces them with fielded terms from the index
     *
     * @param config
     *            the query configuration, not null
     * @param scannerFactory
     *            the scanner factory, not null
     * @param helper
     *            the metadata helper, not null
     * @param script
     *            the Jexl script to expand, not null
     * @param <T>
     *            the Jexl node type
     * @return a rebuilt Jexl tree with it's unfielded terms expanded
     * @throws IllegalAccessException
     *             if we fail to retrieve all data types from the metadata helper
     * @throws TableNotFoundException
     *             if we fail to retrieve fields from the metadata helper
     * @throws InstantiationException
     *             if we fail to retrieve all data types from the metadata helper
     */
    public static <T extends JexlNode> T expandUnfielded(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper, T script)
                    throws IllegalAccessException, TableNotFoundException, InstantiationException {
        // if not expanding fields or values, then this is a noop
        if (config.isExpandFields() || config.isExpandValues()) {
            UnfieldedIndexExpansionVisitor visitor = new UnfieldedIndexExpansionVisitor(config, scannerFactory, helper);
            return ensureTreeNotEmpty(visitor.expand(script));
        } else {
            return script;
        }
    }

    private static <T extends JexlNode> T ensureTreeNotEmpty(T script) throws EmptyUnfieldedTermExpansionException {
        if (script.jjtGetNumChildren() == 0) {
            NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.NO_UNFIELDED_TERM_EXPANSION_MATCH);
            log.warn("Empty script", qe);
            throw new EmptyUnfieldedTermExpansionException(qe);
        }
        return script;
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        List<JexlNode> children = visitChildren(node, data);

        switch (children.size()) {
            case 0:
                return null;
            case 1:
                return children.get(0);
            default:
                return JexlNodeFactory.createOrNode(children);
        }
    }

    protected List<JexlNode> visitChildren(JexlNode node, Object data) {
        List<JexlNode> children = new ArrayList<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode newChild = (JexlNode) node.jjtGetChild(i).jjtAccept(this, data);

            // keep the child as long as it's not an empty AND/OR node
            if (newChild != null && !((newChild instanceof ASTOrNode || newChild instanceof ASTAndNode) && newChild.jjtGetNumChildren() == 0)) {
                children.add(newChild);
            }
        }
        return children;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        // ignore already marked expressions
        if (QueryPropertyMarker.findInstance(node).isAnyType()) {
            return node;
        }

        List<JexlNode> children = visitChildren(node, data);

        switch (children.size()) {
            case 0:
                return null;
            case 1:
                return children.get(0);
            default:
                return JexlNodeFactory.createAndNode(children);
        }
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        return buildIndexLookup(node, true, negated, () -> createFieldNameIndexLookup(node));
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        toggleNegation();
        try {
            return buildIndexLookup(node, true, negated, () -> createFieldNameIndexLookup(node));
        } finally {
            toggleNegation();
        }
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        return buildIndexLookup(node, true, negated, () -> createLookup(node));
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        toggleNegation();
        try {
            return buildIndexLookup(node, true, negated, () -> createLookup(node));
        } finally {
            toggleNegation();
        }
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        // handled by BoundedRangeExpansionIterator
        return super.visit(node, data);
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        // handled by BoundedRangeExpansionIterator
        return super.visit(node, data);
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        // handled by BoundedRangeExpansionIterator
        return super.visit(node, data);
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        // handled by BoundedRangeExpansionIterator
        return super.visit(node, data);
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        ASTReferenceExpression ref = (ASTReferenceExpression) super.visit(node, data);
        if (ref.jjtGetNumChildren() == 0) {
            return null;
        } else {
            return ref;
        }
    }

    /**
     * Expand if we have an unfielded identifier
     *
     * @param node
     *            the node to consider
     * @return true if contains an unfielded identifier
     */
    @Override
    protected boolean shouldExpand(JexlNode node) {
        return (!negated || expandUnfieldedNegations) && hasUnfieldedIdentifier(node);
    }

    @Override
    protected IndexLookup createLookup(JexlNode node) {
        try {
            // Using the datatype filter when expanding this term isn't really
            // necessary
            return ShardIndexQueryTableStaticMethods.normalizeQueryTerm(node, config, scannerFactory, expansionFields, allTypes, helper, executor);
        } catch (TableNotFoundException e) {
            throw new DatawaveFatalQueryException(e);
        }
    }

    protected IndexLookup createFieldNameIndexLookup(JexlNode node) {
        String term = (String) JexlASTHelper.getLiteralValue(node);

        Preconditions.checkNotNull(term);
        Preconditions.checkNotNull(expansionFields);

        try {
            // note: if the system has configured 'exp' fields in the metadata table this method call will verify
            // all fields are also indexed. In the event that no expansion fields are configured this will fall back
            // to the full set of indexed fields for the provided datatypes
            Set<String> fields = ShardIndexQueryTableStaticMethods.getIndexedExpansionFields(expansionFields, false, config.getDatatypeFilter(), helper);

            if (fields.isEmpty()) {
                // if no fields match then do not attempt expansion
                return new EmptyIndexLookup(config);
            }

            return new FieldExpansionIndexLookup(config, scannerFactory, term, fields, executor);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
