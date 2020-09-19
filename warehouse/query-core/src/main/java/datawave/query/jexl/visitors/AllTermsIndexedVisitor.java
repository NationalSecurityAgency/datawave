package datawave.query.jexl.visitors;

import com.google.common.base.Preconditions;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.EmptyUnfieldedTermExpansionException;
import datawave.query.exceptions.InvalidFieldIndexQueryFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.PreConditionFailedQueryException;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.log4j.Logger;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.BiFunction;

import static org.apache.commons.jexl2.parser.JexlNodes.promote;

/**
 *
 */
public class AllTermsIndexedVisitor extends RebuildingVisitor {
    
    private static final Logger log = Logger.getLogger(AllTermsIndexedVisitor.class);
    
    protected final ShardQueryConfiguration config;
    protected final MetadataHelper helper;
    
    public AllTermsIndexedVisitor(ShardQueryConfiguration config, MetadataHelper helper) {
        Preconditions.checkNotNull(config, "ShardQueryConfiguration must not be null");
        Preconditions.checkNotNull(helper, "MetadataHelper must not be null");
        
        this.config = config;
        this.helper = helper;
    }
    
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T isIndexed(T script, ShardQueryConfiguration config, MetadataHelper helper) {
        Preconditions.checkNotNull(script, "JEXL script must not be null");
        
        AllTermsIndexedVisitor visitor = new AllTermsIndexedVisitor(config, helper);
        
        return (T) script.jjtAccept(visitor, null);
    }
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        JexlNode copy = getCopyWithVisitedChildren(node, data);
        
        if (copy.jjtGetNumChildren() == 0) {
            NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.NO_ANYFIELD_EXPANSION_MATCH);
            log.warn(qe);
            throw new EmptyUnfieldedTermExpansionException(qe);
        }
        
        return copy;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        return visitJunctionNode(node, data);
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        return visitJunctionNode(node, data);
    }
    
    private Object visitJunctionNode(JexlNode node, Object data) {
        JexlNode copy = getCopyWithVisitedChildren(node, data);
        
        switch (copy.jjtGetNumChildren()) {
            case 0:
                return null;
            case 1:
                JexlNode child = copy.jjtGetChild(0);
                promote(copy, child);
                return child;
            default:
                return copy;
        }
    }
    
    private JexlNode getCopyWithVisitedChildren(JexlNode node, Object data) {
        // @formatter:off
        JexlNode[] children = Arrays.stream(JexlNodes.children(node))
                        .map(n -> (JexlNode) n.jjtAccept(this, data)) // Visit the node
                        .filter(Objects::nonNull) // Filter out null nodes
                        .filter(n -> (!JexlNodes.isAnd(n) && !JexlNodes.isOr(n)) || JexlNodes.isNotChildless(n)) // Filter out empty junction nodes
                        .toArray(JexlNode[]::new);
        // @formatter:on
        JexlNode copy = JexlNodes.newInstanceOfType(node);
        copy.image = node.image;
        System.out.println("Copy: " + copy);
        System.out.println("Children: " + Arrays.toString(children));
        JexlNodes.children(copy, children);
        return copy;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        return visitEqualityNode(node, data);
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        return visitEqualityNode(node, data);
    }
    
    /**
     * Determine, for a binary equality node, if the field name is indexed.
     *
     * @param node
     *            the node to verify the indexed status of
     * @param data
     *            the node data
     * @return a copy of the
     */
    protected JexlNode visitEqualityNode(JexlNode node, Object data) {
        String fieldName;
        try {
            fieldName = JexlASTHelper.getIdentifier(node);
        } catch (NoSuchElementException e) {
            // We only have literals.
            throw new InvalidFieldIndexQueryFatalQueryException(getExceptionWithPrintedNode(node, DatawaveErrorCode.EQUALS_NODE_TWO_LITERALS,
                            PreConditionFailedQueryException::new));
        }
        try {
            // In the case of an ANY_FIELD or NO_FIELD, the query could not be expanded against the index and hence this
            // term has no results. Leave it in the query as such.
            if (Constants.ANY_FIELD.equals(fieldName) || Constants.NO_FIELD.equals(fieldName)) {
                return node;
            }
            
            if (!this.helper.isIndexed(fieldName, config.getDatatypeFilter())) {
                PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.FIELD_NOT_INDEXED, MessageFormat.format(
                                "Fieldname: {0}", fieldName));
                throw new InvalidFieldIndexQueryFatalQueryException(qe);
            }
        } catch (TableNotFoundException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.INDETERMINATE_INDEX_STATUS, e);
            throw new DatawaveFatalQueryException(qe);
        }
        
        return node;
    }
    
    /**
     * Throw an {@link datawave.query.exceptions.InvalidFieldIndexQueryFatalQueryException}.
     *
     * @param node
     *            the node
     * @param data
     *            the node data
     * @return nothing
     */
    @Override
    public Object visit(ASTERNode node, Object data) {
        throw new InvalidFieldIndexQueryFatalQueryException(getExceptionWithPrintedNode(node, DatawaveErrorCode.EQUALS_REGEX_NODE_PROCESSING_ERROR,
                        QueryException::new));
    }
    
    /**
     * Throw an {@link datawave.query.exceptions.InvalidFieldIndexQueryFatalQueryException}.
     *
     * @param node
     *            the node
     * @param data
     *            the node data
     * @return nothing
     */
    @Override
    public Object visit(ASTNRNode node, Object data) {
        throw new InvalidFieldIndexQueryFatalQueryException(getExceptionWithPrintedNode(node, DatawaveErrorCode.NOT_EQUALS_REGEX_NODE_PROCESSING_ERROR,
                        QueryException::new));
    }
    
    /**
     * Throw an {@link datawave.query.exceptions.InvalidFieldIndexQueryFatalQueryException}.
     *
     * @param node
     *            the node
     * @param data
     *            the node data
     * @return nothing
     */
    @Override
    public Object visit(ASTLTNode node, Object data) {
        throw new InvalidFieldIndexQueryFatalQueryException(getExceptionWithPrintedNode(node, DatawaveErrorCode.LT_NODE_PROCESSING_ERROR, QueryException::new));
    }
    
    /**
     * Throw an {@link datawave.query.exceptions.InvalidFieldIndexQueryFatalQueryException}.
     *
     * @param node
     *            the node
     * @param data
     *            the node data
     * @return nothing
     */
    @Override
    public Object visit(ASTGTNode node, Object data) {
        throw new InvalidFieldIndexQueryFatalQueryException(getExceptionWithPrintedNode(node, DatawaveErrorCode.GT_NODE_PROCESSING_ERROR, QueryException::new));
    }
    
    /**
     * Throw an {@link datawave.query.exceptions.InvalidFieldIndexQueryFatalQueryException}.
     *
     * @param node
     *            the node
     * @param data
     *            the node data
     * @return nothing
     */
    @Override
    public Object visit(ASTLENode node, Object data) {
        throw new InvalidFieldIndexQueryFatalQueryException(getExceptionWithPrintedNode(node, DatawaveErrorCode.LTE_NODE_PROCESSING_ERROR, QueryException::new));
    }
    
    /**
     * Throw an {@link datawave.query.exceptions.InvalidFieldIndexQueryFatalQueryException}.
     *
     * @param node
     *            the node
     * @param data
     *            the node data
     * @return nothing
     */
    @Override
    public Object visit(ASTGENode node, Object data) {
        throw new InvalidFieldIndexQueryFatalQueryException(getExceptionWithPrintedNode(node, DatawaveErrorCode.GTE_NODE_PROCESSING_ERROR, QueryException::new));
    }
    
    /**
     * Throw an {@link datawave.query.exceptions.InvalidFieldIndexQueryFatalQueryException}.
     *
     * @param node
     *            the node
     * @param data
     *            the node data
     * @return nothing
     */
    @Override
    public Object visit(ASTAssignment node, Object data) {
        throw new InvalidFieldIndexQueryFatalQueryException(getExceptionWithPrintedNode(node, DatawaveErrorCode.ASSIGNMENT_NODE_PROCESSING_ERROR,
                        QueryException::new));
    }
    
    /**
     * Throw an {@link datawave.query.exceptions.InvalidFieldIndexQueryFatalQueryException}.
     *
     * @param node
     *            the node
     * @param data
     *            the node data
     * @return nothing
     */
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        throw new InvalidFieldIndexQueryFatalQueryException(getExceptionWithPrintedNode(node, DatawaveErrorCode.FUNCTION_NODE_PROCESSING_ERROR,
                        QueryException::new));
    }
    
    private <T extends Throwable> T getExceptionWithPrintedNode(JexlNode node, DatawaveErrorCode errorCode,
                    BiFunction<DatawaveErrorCode,String,T> throwableConstructor) {
        String debugMessage = MessageFormat.format("Node: {0}", PrintingVisitor.formattedQueryString(node).replace('\n', ' '));
        return throwableConstructor.apply(errorCode, debugMessage);
    }
}
