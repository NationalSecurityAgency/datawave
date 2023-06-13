package datawave.query.planner.pushdown.rules;

import datawave.query.jexl.visitors.RebuildingVisitor;
import datawave.query.planner.pushdown.Cost;
import datawave.query.planner.pushdown.PushDownVisitor;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;

/**
 * Purpose: Base class which aids in pushing nodes down when necessary.
 * 
 * Assumptions: The PushDownRule always assumes that the visitor will be run by PushDownVisitor, which will ultimately ensure that not all nodes are pushed
 * down.
 */
public abstract class PushDownRule extends RebuildingVisitor {
    
    protected PushDownVisitor parentVisitor = null;
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        setPushDown((PushDownVisitor) data);
        return super.visit(node, null);
    }
    
    protected void setPushDown(PushDownVisitor visitor) {
        this.parentVisitor = visitor;
    }
    
    public abstract Cost getCost(JexlNode node);
    
    /**
     * Determines if parent is a type
     * 
     * @param currentNode
     *            the current node
     * @param clazz
     *            a class
     * @return if parent is a type
     */
    public static boolean isParent(final JexlNode currentNode, final Class<? extends JexlNode> clazz) {
        JexlNode parentNode = currentNode.jjtGetParent();
        
        return parentNode.getClass().isAssignableFrom(clazz);
    }
    
}
