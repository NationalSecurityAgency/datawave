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
import org.apache.commons.jexl2.parser.Node;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import java.text.MessageFormat;
import java.util.NoSuchElementException;

/**
 * 
 */
public class AllTermsIndexedVisitor extends RebuildingVisitor {
    private static final Logger log = Logger.getLogger(AllTermsIndexedVisitor.class);
    
    protected final ShardQueryConfiguration config;
    protected final MetadataHelper helper;
    
    public AllTermsIndexedVisitor(ShardQueryConfiguration config, MetadataHelper helper) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(helper);
        
        this.config = config;
        this.helper = helper;
    }
    
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T isIndexed(T script, ShardQueryConfiguration config, MetadataHelper helper) {
        Preconditions.checkNotNull(script);
        
        AllTermsIndexedVisitor visitor = new AllTermsIndexedVisitor(config, helper);
        
        return (T) script.jjtAccept(visitor, null);
    }
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        ASTJexlScript newNode = new ASTJexlScript(ParserTreeConstants.JJTJEXLSCRIPT);
        newNode.image = node.image;
        
        int newIndex = 0;
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            Node newChild = (Node) node.jjtGetChild(i).jjtAccept(this, data);
            
            if (newChild != null) {
                // When we have an AND or OR
                if ((newChild instanceof ASTOrNode || newChild instanceof ASTAndNode)) {
                    // Only add that node if it actually has children
                    if (0 < newChild.jjtGetNumChildren()) {
                        newNode.jjtAddChild(newChild, newIndex);
                        newChild.jjtSetParent(newNode);
                        newIndex++;
                    }
                } else {
                    // Otherwise, we want to add the child regardless
                    newNode.jjtAddChild(newChild, newIndex);
                    newChild.jjtSetParent(newNode);
                    newIndex++;
                }
            }
        }
        
        if (newNode.jjtGetNumChildren() == 0) {
            NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.NO_ANYFIELD_EXPANSION_MATCH);
            log.warn(qe);
            throw new EmptyUnfieldedTermExpansionException(qe);
        }
        
        return newNode;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        ASTOrNode newNode = new ASTOrNode(ParserTreeConstants.JJTORNODE);
        newNode.image = node.image;
        
        int newIndex = 0;
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            Node newChild = (Node) node.jjtGetChild(i).jjtAccept(this, data);
            
            if (newChild != null) {
                // When we have an AND or OR
                if ((newChild instanceof ASTOrNode || newChild instanceof ASTAndNode)) {
                    // Only add that node if it actually has children
                    if (0 < newChild.jjtGetNumChildren()) {
                        newNode.jjtAddChild(newChild, newIndex);
                        newChild.jjtSetParent(newNode);
                        newIndex++;
                    }
                } else {
                    // Otherwise, we want to add the child regardless
                    newNode.jjtAddChild(newChild, newIndex);
                    newChild.jjtSetParent(newNode);
                    newIndex++;
                }
            }
        }
        
        switch (newNode.jjtGetNumChildren()) {
            case 0:
                return null;
            case 1:
                JexlNode child = newNode.jjtGetChild(0);
                JexlNodes.promote(newNode, child);
                return child;
            default:
                return newNode;
        }
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        ASTAndNode newNode = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
        newNode.image = node.image;
        
        int newIndex = 0;
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            Node newChild = (Node) node.jjtGetChild(i).jjtAccept(this, data);
            
            if (newChild != null) {
                // When we have an AND or OR
                if ((newChild instanceof ASTOrNode || newChild instanceof ASTAndNode)) {
                    // Only add that node if it actually has children
                    if (0 < newChild.jjtGetNumChildren()) {
                        newNode.jjtAddChild(newChild, newIndex);
                        newChild.jjtSetParent(newNode);
                        newIndex++;
                    }
                } else {
                    // Otherwise, we want to add the child regardless
                    newNode.jjtAddChild(newChild, newIndex);
                    newChild.jjtSetParent(newNode);
                    newIndex++;
                }
            }
        }
        switch (newNode.jjtGetNumChildren()) {
            case 0:
                return null;
            case 1:
                JexlNode child = newNode.jjtGetChild(0);
                JexlNodes.promote(newNode, child);
                return child;
            default:
                return newNode;
        }
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        return equalityVisitor(node, data);
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        return equalityVisitor(node, data);
    }
    
    /**
     * Determine, for a binary equality node, if the field name is indexed
     * 
     * @param node
     * @param data
     * @return
     */
    protected JexlNode equalityVisitor(JexlNode node, Object data) {
        String fieldName = null;
        try {
            fieldName = JexlASTHelper.getIdentifier(node);
        } catch (NoSuchElementException e) {
            // We only have literals
            PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.EQUALS_NODE_TWO_LITERALS, e, MessageFormat.format(
                            "Node: {0}", PrintingVisitor.formattedQueryString(node).replace('\n', ' ')));
            throw new InvalidFieldIndexQueryFatalQueryException(qe);
        }
        try {
            // in the case of an ANY_FIELD or NO_FIELD, the query could not be exapnded against the index and hence this
            // term has no results. Leave it in the query as such.
            if (Constants.ANY_FIELD.equals(fieldName) || Constants.NO_FIELD.equals(fieldName)) {
                return copy(node);
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
        return copy(node);
        
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        QueryException qe = new QueryException(DatawaveErrorCode.EQUALS_REGEX_NODE_PROCESSING_ERROR, MessageFormat.format("Node: {0}", PrintingVisitor
                        .formattedQueryString(node).replace('\n', ' ')));
        throw new InvalidFieldIndexQueryFatalQueryException(qe);
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        QueryException qe = new QueryException(DatawaveErrorCode.NOT_EQUALS_REGEX_NODE_PROCESSING_ERROR, MessageFormat.format("Node: {0}", PrintingVisitor
                        .formattedQueryString(node).replace('\n', ' ')));
        throw new InvalidFieldIndexQueryFatalQueryException(qe);
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        QueryException qe = new QueryException(DatawaveErrorCode.LT_NODE_PROCESSING_ERROR, MessageFormat.format("Node: {0}", PrintingVisitor
                        .formattedQueryString(node).replace('\n', ' ')));
        throw new InvalidFieldIndexQueryFatalQueryException(qe);
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        QueryException qe = new QueryException(DatawaveErrorCode.GT_NODE_PROCESSING_ERROR, MessageFormat.format("Node: {0}", PrintingVisitor
                        .formattedQueryString(node).replace('\n', ' ')));
        throw new InvalidFieldIndexQueryFatalQueryException(qe);
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        QueryException qe = new QueryException(DatawaveErrorCode.LTE_NODE_PROCESSING_ERROR, MessageFormat.format("Node: {0}", PrintingVisitor
                        .formattedQueryString(node).replace('\n', ' ')));
        throw new InvalidFieldIndexQueryFatalQueryException(qe);
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        QueryException qe = new QueryException(DatawaveErrorCode.GTE_NODE_PROCESSING_ERROR, MessageFormat.format("Node: {0}", PrintingVisitor
                        .formattedQueryString(node).replace('\n', ' ')));
        throw new InvalidFieldIndexQueryFatalQueryException(qe);
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        QueryException qe = new QueryException(DatawaveErrorCode.ASSIGNMENT_NODE_PROCESSING_ERROR, MessageFormat.format("Node: {0}", PrintingVisitor
                        .formattedQueryString(node).replace('\n', ' ')));
        throw new InvalidFieldIndexQueryFatalQueryException(qe);
    }
    
    public Object visit(ASTFunctionNode node, Object data) {
        QueryException qe = new QueryException(DatawaveErrorCode.FUNCTION_NODE_PROCESSING_ERROR, MessageFormat.format("Node: {0}", PrintingVisitor
                        .formattedQueryString(node).replace('\n', ' ')));
        throw new InvalidFieldIndexQueryFatalQueryException(qe);
    }
}
