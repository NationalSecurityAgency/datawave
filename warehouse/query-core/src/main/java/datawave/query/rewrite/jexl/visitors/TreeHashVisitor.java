package datawave.query.rewrite.jexl.visitors;

import datawave.query.rewrite.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.rewrite.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.rewrite.jexl.nodes.IndexHoleMarkerJexlNode;
import datawave.query.rewrite.jexl.nodes.TreeHashNode;

import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
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
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;

/**
 * Builds hash for necessary node. Not that this depends on JexlStringBuildingVisitor which will descend children of the provided node. Provides the string
 * representation of the expression nodes. Note that as a result, reference nodes are dropped. The result of this class can be used to determine if we have the
 * same nodes within a sub tree. Passing the result of this to a map will use the hashcode of the string followed by the equality of it to ensure that we do not
 * duplicate nodes.
 */
public class TreeHashVisitor extends BaseVisitor {
    
    private static final String ANDNODE = "&&";
    private static final String ORNODE = "||";
    
    public static TreeHashNode getNodeHash(JexlNode root) {
        TreeHashVisitor vis = new TreeHashVisitor();
        TreeHashNode inte = new TreeHashNode();
        root.jjtAccept(vis, inte);
        return inte;
    }
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        
        return super.visit(node, data);
        
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        
        return ((TreeHashNode) data).append(JexlStringBuildingVisitor.buildQueryWithoutParse(node));
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        
        return ((TreeHashNode) data).append(JexlStringBuildingVisitor.buildQueryWithoutParse(node));
    }
    
    public Object visit(ASTLENode node, Object data) {
        return ((TreeHashNode) data).append(JexlStringBuildingVisitor.buildQueryWithoutParse(node));
    }
    
    public Object visit(ASTGENode node, Object data) {
        return ((TreeHashNode) data).append(JexlStringBuildingVisitor.buildQueryWithoutParse(node));
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        
        return ((TreeHashNode) data).append(JexlStringBuildingVisitor.buildQueryWithoutParse(node));
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        
        return ((TreeHashNode) data).append(JexlStringBuildingVisitor.buildQueryWithoutParse(node));
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        
        return ((TreeHashNode) data).append(JexlStringBuildingVisitor.buildQueryWithoutParse(node));
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        return ((TreeHashNode) data).append(JexlStringBuildingVisitor.buildQueryWithoutParse(node));
    }
    
    public Object visit(ASTNotNode node, Object data) {
        return ((TreeHashNode) data).append(JexlStringBuildingVisitor.buildQueryWithoutParse(node));
    }
    
    public Object visit(ASTAssignment node, Object data) {
        return ((TreeHashNode) data).append(JexlStringBuildingVisitor.buildQueryWithoutParse(node));
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        if (isDelayed(node)) {
            return ((TreeHashNode) data).append(JexlStringBuildingVisitor.buildQueryWithoutParse(node));
        } else
            return super.visit(node, data);
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        if (isDelayed(node)) {
            return ((TreeHashNode) data).append(JexlStringBuildingVisitor.buildQueryWithoutParse(node));
        } else
            return super.visit(node, data);
    }
    
    public Object visit(ASTOrNode node, Object data) {
        
        int numChildren = node.jjtGetNumChildren();
        
        TreeHashNode inte = (TreeHashNode) (data);
        int lastsize = inte.length();
        
        for (int i = 0; i < numChildren; i++) {
            if (inte.length() != lastsize) {
                inte.append(ORNODE);
            }
            lastsize = inte.length();
            node.jjtGetChild(i).jjtAccept(this, inte);
        }
        
        return inte;
    }
    
    public Object visit(ASTAndNode node, Object data) {
        
        int numChildren = node.jjtGetNumChildren();
        
        TreeHashNode inte = (TreeHashNode) (data);
        int lastsize = inte.length();
        
        for (int i = 0; i < numChildren; i++) {
            if (inte.length() != lastsize) {
                inte.append(ANDNODE);
            }
            lastsize = inte.length();
            node.jjtGetChild(i).jjtAccept(this, inte);
        }
        
        return inte;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return ((TreeHashNode) data).append(JexlStringBuildingVisitor.buildQueryWithoutParse(node));
    }
    
    protected boolean isDelayed(JexlNode testNode) {
        if (ASTDelayedPredicate.instanceOf(testNode)) {
            return true;
        } else if (IndexHoleMarkerJexlNode.instanceOf(testNode)) {
            return true;
        } else if (ExceededValueThresholdMarkerJexlNode.instanceOf(testNode)) {
            return true;
        } else if (ExceededTermThresholdMarkerJexlNode.instanceOf(testNode)) {
            return true;
        } else {
            return false;
        }
    }
    
}
