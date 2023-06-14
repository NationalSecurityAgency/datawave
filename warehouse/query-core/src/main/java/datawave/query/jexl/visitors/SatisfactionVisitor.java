package datawave.query.jexl.visitors;

import com.google.common.collect.Lists;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.util.UniversalSet;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.ContentFunctionsDescriptor;
import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * A visitor that checks the query tree to determine if the query can be satisfied by only looking in the field index. The result of this is passed to the
 * IteratorBuildingVisitor
 *
 */
public class SatisfactionVisitor extends BaseVisitor {
    private static final Logger log = Logger.getLogger(SatisfactionVisitor.class);

    protected Set<String> nonEventFields;
    private Collection<String> unindexedFields = Lists.newArrayList();
    protected boolean isQueryFullySatisfied;
    protected Collection<String> includeReferences = UniversalSet.instance();
    protected Collection<String> excludeReferences = Collections.emptyList();

    public boolean isQueryFullySatisfied() {
        return isQueryFullySatisfied;
    }

    public SatisfactionVisitor(Set<String> nonEventFields, Collection<String> includes, Collection<String> excludes, boolean isQueryFullySatisfied) {
        this.nonEventFields = nonEventFields;
        this.isQueryFullySatisfied = isQueryFullySatisfied;
        this.includeReferences = includes;
        this.excludeReferences = excludes;
    }

    private JexlNode defensiveGetLiteral(JexlNode node) {
        JexlNode literal = null;
        try {
            literal = JexlASTHelper.getLiteral(node);
        } catch (Exception ex) {
            if (log.isTraceEnabled()) {
                log.trace("It is okay that we got no literal because of ", ex);
            }
            // there are cases where there is no literal, like when a query looks like this:
            // afunction() == anotherFunction()
            // we don't want any trouble here, so just let it go with something safe
        }
        return literal;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        /**
         * If we have an unindexed type enforced, we've been configured to assert whether the field is indexed.
         */
        if (isUnindexed(node)) {
            isQueryFullySatisfied = false;
            return null;
        }

        if (defensiveGetLiteral(node) instanceof ASTNullLiteral) {
            isQueryFullySatisfied = false;
            return null;
        }
        String field = JexlASTHelper.getIdentifier(node);
        final boolean included = includeReferences.contains(field);
        final boolean excluded = excludeReferences.contains(field);

        if (excluded || !included) {
            isQueryFullySatisfied = false;
            return null;
        }
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            // visit the kids in case there is a surprise there (like a Method node)
            node.jjtGetChild(i).jjtAccept(this, data);
        }
        return null;
    }

    @Override
    public Object visit(ASTReference node, Object o) {
        // Recurse only if not delayed
        if (!QueryPropertyMarker.findInstance(node).isType(ASTDelayedPredicate.class)) {
            super.visit(node, o);
        } else {
            isQueryFullySatisfied = false;
        }

        return null;
    }

    @Override
    public Object visit(ASTAndNode and, Object data) {

        if (QueryPropertyMarker.findInstance(and).isNotAnyTypeOf(ExceededOrThresholdMarkerJexlNode.class, ExceededValueThresholdMarkerJexlNode.class)) {
            and.childrenAccept(this, data);
        }
        return null;
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object o) {
        // Recurse
        node.jjtGetChild(0).jjtAccept(this, o);

        return null;
    }

    /**
     * This method only do anything when the function is a content function. In that case, we take the set of term frequency fields and supply it to the
     * {@link ContentFunctionsDescriptor.ContentJexlArgumentDescriptor}, which produces a tree of JexlNodes. This will allow us to create an iterator tree
     * across the expression that tree represents and consider the TF fields when doing an index look up. This also enables us to perform an index lookup when
     * the query is only a content function.
     */
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        // only functions in the QueryFunctions package can be fully satistfied by the field index
        if (!node.jjtGetChild(0).image.equals(QueryFunctions.QUERY_FUNCTION_NAMESPACE)) {
            isQueryFullySatisfied = false;
        }

        return null;
    }

    @Override
    public Object visit(ASTMethodNode node, Object data) {
        // if a method on a field, then not fully satisfied
        isQueryFullySatisfied = false;

        return null;
    }

    @Override
    public Object visit(ASTSizeMethod node, Object data) {
        // if a method on a field, then not fully satisfied
        isQueryFullySatisfied = false;

        return null;
    }

    @Override
    public Object visit(ASTIdentifier node, Object o) {
        String field = JexlASTHelper.getIdentifier(node);
        final boolean included = includeReferences.contains(field);
        final boolean excluded = excludeReferences.contains(field);

        if (excluded || !included) {
            isQueryFullySatisfied = false;
        }
        if (isUnindexed(node)) {
            isQueryFullySatisfied = false;
        }

        return null;
    }

    /**
     * Negated regular expression nodes can be ignored when creating the iterator tree and used a post filter.
     */
    @Override
    public Object visit(ASTNRNode node, Object data) {
        isQueryFullySatisfied = false;

        // we have an ER node that we are not going to process because we know already that it will
        // not match anything.true
        return null;
    }

    /**
     * Regular expression nodes that are part of a conjunction can be ignored, as they may be implemented via a post filter. We will still need to handle the OR
     * case.
     */
    @Override
    public Object visit(ASTERNode node, Object data) {
        isQueryFullySatisfied = false;

        // we have an ER node that we are not going to process because we know already that it will
        // not match anything.
        return null;
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
        isQueryFullySatisfied = false;

        return null;
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        isQueryFullySatisfied = false;

        return null;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        isQueryFullySatisfied = false;

        return null;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        isQueryFullySatisfied = false;

        return null;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        isQueryFullySatisfied = false;
        if (log.isDebugEnabled()) {
            log.debug("isQueryFullySatisfied set to false because " + PrintingVisitor.formattedQueryString(node) + " is an assignment node");
        }

        return null;
    }

    @Override
    public Object visit(ASTAssignment node, Object data) {
        isQueryFullySatisfied = false;
        if (log.isDebugEnabled()) {
            log.debug("isQueryFullySatisfied set to false because " + PrintingVisitor.formattedQueryString(node) + " is an assignment node");
        }
        return null;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        /**
         * If we have an unindexed type enforced, we've been configured to assert whether the field is indexed.
         */
        if (isUnindexed(node)) {
            isQueryFullySatisfied = false;
            if (log.isDebugEnabled()) {
                log.debug("isQueryFullySatisfied set to false because " + PrintingVisitor.formattedQueryString(node) + " is UnIndexed");
            }
            return null;
        }

        if (defensiveGetLiteral(node) instanceof ASTNullLiteral) {
            isQueryFullySatisfied = false;
            if (log.isDebugEnabled()) {
                log.debug("isQueryFullySatisfied set to false because " + PrintingVisitor.formattedQueryString(node) + " has a null literal");
            }
            return null;
        }
        String field = JexlASTHelper.getIdentifier(node);
        final boolean included = includeReferences.contains(field);
        final boolean excluded = excludeReferences.contains(field);

        if (excluded || !included) {
            isQueryFullySatisfied = false;
            if (log.isDebugEnabled()) {
                log.debug("isQueryFullySatisfied set to false because field for " + PrintingVisitor.formattedQueryString(node) + " is in " + excludeReferences
                                + " or not in " + includeReferences);
            }
        }

        return null;
    }

    protected boolean isUnindexed(JexlNode node) {
        final String fieldName = JexlASTHelper.getIdentifier(node);
        return this.unindexedFields.contains(fieldName);
    }

    protected boolean isUnindexed(ASTIdentifier node) {
        final String fieldName = JexlASTHelper.deconstructIdentifier(node.image);
        return this.unindexedFields.contains(fieldName);
    }

    public SatisfactionVisitor setUnindexedFields(Collection<String> unindexedField) {
        this.unindexedFields.addAll(unindexedField);
        return this;
    }
}
