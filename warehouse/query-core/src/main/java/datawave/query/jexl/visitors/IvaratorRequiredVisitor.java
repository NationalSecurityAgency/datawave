package datawave.query.jexl.visitors;

import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
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
 * A visitor that checks the query tree to determine if the query requires an ivarator (ExceededValue or ExceededOr)
 */
public class IvaratorRequiredVisitor extends ShortCircuitBaseVisitor {
    
    private boolean ivaratorRequired = false;
    
    public boolean isIvaratorRequired() {
        return ivaratorRequired;
    }
    
    public static boolean isIvaratorRequired(JexlNode node) {
        IvaratorRequiredVisitor visitor = new IvaratorRequiredVisitor();
        node.jjtAccept(visitor, null);
        return visitor.isIvaratorRequired();
    }
    
    // We always want to descend into these nodes
    @Override
    public Object visit(ASTAndNode and, Object data) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(and);
        if (instance.isAnyTypeOf(ExceededOrThresholdMarkerJexlNode.class, ExceededValueThresholdMarkerJexlNode.class)) {
            ivaratorRequired = true;
        } else if (!instance.isAnyTypeOf()) {
            super.visit(and, data);
        }
        return data;
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        if (!ivaratorRequired) {
            node.childrenAccept(this, data);
        }
        return data;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        if (!ivaratorRequired) {
            node.childrenAccept(this, data);
        }
        return data;
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        if (!ivaratorRequired) {
            node.childrenAccept(this, data);
        }
        return data;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        if (!ivaratorRequired) {
            node.childrenAccept(this, data);
        }
        return data;
    }
    
    // Ensure we short circuit leaf nodes
    public Object visit(ASTEQNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return data;
    }
}
