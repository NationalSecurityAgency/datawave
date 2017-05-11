package datawave.query.rewrite.jexl.visitors;

import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import datawave.query.rewrite.Constants;
import datawave.query.rewrite.jexl.JexlASTHelper;

/**
 * This will remove the SHARDS_AND_DAYS assignment node. This is used after the range stream has been executed, but before the query is actually built to be
 * sent to the tservers.
 */
public class DateIndexCleanupVisitor extends RebuildingVisitor {
    private static final Logger log = Logger.getLogger(DateIndexCleanupVisitor.class);
    
    public DateIndexCleanupVisitor() {
        
    }
    
    /**
     * This will cleanup/remove the SHARDS_AND_DAYS assignment node.
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
    public Object visit(ASTReferenceExpression node, Object data) {
        JexlNode retnode = (JexlNode) (super.visit(node, data));
        if (retnode.jjtGetNumChildren() == 0) {
            retnode = null;
        } else if (retnode.jjtGetNumChildren() == 1) {
            if (retnode.jjtGetChild(0) instanceof ASTReferenceExpression) {
                retnode = retnode.jjtGetChild(0);
            } else if ((retnode.jjtGetChild(0) instanceof ASTReference) && (retnode.jjtGetChild(0).jjtGetChild(0) instanceof ASTReferenceExpression)) {
                retnode = retnode.jjtGetChild(0).jjtGetChild(0);
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
