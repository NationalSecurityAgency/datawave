package datawave.query.jexl.visitors;

import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;

/**
 * This will remove the SHARDS_AND_DAYS assignment node. This is used after the range stream has been executed, but before the query is actually built to be
 * sent to the tservers.
 */
public class DateIndexCleanupVisitor extends RebuildingVisitor {
    
    /**
     * This will cleanup/remove the SHARDS_AND_DAYS assignment node.
     * 
     * @param node
     *            the node
     * @param <T>
     *            type of node
     * @return a reference to the node
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T cleanup(T node) {
        DateIndexCleanupVisitor visitor = new DateIndexCleanupVisitor();
        
        return (T) node.jjtAccept(visitor, null);
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        JexlNode retnode = (JexlNode) (super.visit(node, data));
        // if only one member remaining, then return only that
        if (node.jjtGetNumChildren() == 0) {
            retnode = null;
        } else if (retnode.jjtGetNumChildren() == 1) {
            retnode = retnode.jjtGetChild(0);
        }
        return retnode;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        JexlNode retnode = (JexlNode) (super.visit(node, data));
        // if only one member remaining, then return only that
        if (node.jjtGetNumChildren() == 0) {
            retnode = null;
        } else if (retnode.jjtGetNumChildren() == 1) {
            retnode = retnode.jjtGetChild(0);
        }
        return retnode;
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        JexlNode retnode = (JexlNode) (super.visit(node, data));
        if (retnode.jjtGetNumChildren() == 0) {
            retnode = null;
        }
        return retnode;
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        JexlNode retnode = (JexlNode) (super.visit(node, data));
        if (retnode.jjtGetNumChildren() == 0) {
            retnode = null;
        }
        return retnode;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        JexlNode retnode = (JexlNode) (super.visit(node, data));
        if (retnode.jjtGetNumChildren() == 0) {
            retnode = null;
        } else if (retnode.jjtGetNumChildren() == 1) {
            if (retnode.jjtGetChild(0) instanceof ASTReferenceExpression) {
                retnode = retnode.jjtGetChild(0);
            } else if (retnode.jjtGetChild(0) instanceof ASTOrNode) {
                retnode = (JexlNode) retnode.jjtGetChild(0).jjtAccept(this, null);
            }
        }
        
        return retnode;
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        String identifier = JexlASTHelper.getIdentifier(node);
        if (Constants.SHARD_DAY_HINT.equals(identifier)) {
            node = null;
        }
        return node;
    }
}
