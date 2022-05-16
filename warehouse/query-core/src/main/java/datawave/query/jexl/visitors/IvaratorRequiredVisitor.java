package datawave.query.jexl.visitors;

import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
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
    
    // ensure we short circuit these nodes
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
