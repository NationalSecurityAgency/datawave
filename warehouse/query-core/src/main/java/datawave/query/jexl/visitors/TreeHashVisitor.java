package datawave.query.jexl.visitors;

import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
import datawave.query.jexl.nodes.TreeHashNode;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

/**
 * Builds a hash for the given node.
 *
 * Depends on the JexlStringBuildingVisitor to descend through the children of the given node. This builds the string representation for expression nodes. A
 * side effect of this is that reference nodes are dropped.
 *
 * The result of {@link TreeHashVisitor#getNodeHash(JexlNode)} can be used to determine if we have the same nodes within a subtree. A map can be built using the
 * hashcode of the string to ensure node uniqueness.
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
        return visitAndOrChildren(node, data, ORNODE);
    }
    
    public Object visit(ASTAndNode node, Object data) {
        return visitAndOrChildren(node, data, ANDNODE);
    }
    
    private Object visitAndOrChildren(JexlNode node, Object data, String joinTerm) {
        int numChildren = node.jjtGetNumChildren();
        
        TreeHashNode inte = (TreeHashNode) (data);
        int lastsize = inte.length();
        
        // Build keys for child node map
        List<TreeHashNode> keys = new ArrayList<>();
        TreeMap<TreeHashNode,JexlNode> nodeMap = new TreeMap<>();
        for (int ii = 0; ii < numChildren; ii++) {
            JexlNode child = node.jjtGetChild(ii);
            TreeHashNode key = TreeHashVisitor.getNodeHash(child);
            nodeMap.put(key, child);
            keys.add(key);
        }
        
        // Sort the list of keys, append child node in order
        Collections.sort(keys);
        for (TreeHashNode key : keys) {
            JexlNode child = nodeMap.get(key);
            if (inte.length() != lastsize) {
                inte.append(joinTerm);
            }
            lastsize = inte.length();
            child.jjtAccept(this, inte);
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
