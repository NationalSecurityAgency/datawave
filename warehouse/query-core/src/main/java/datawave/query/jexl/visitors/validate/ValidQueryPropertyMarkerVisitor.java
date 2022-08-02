package datawave.query.jexl.visitors.validate;

import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.QueryPropertyMarkerVisitor;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;

/**
 * This visitor verifies that all query property marker nodes present in a query tree are wrapped and have a singular root source node.
 */
public class ValidQueryPropertyMarkerVisitor extends BaseVisitor {
    
    /**
     * Verify whether all query property marker nodes present in the given query tree adhere to the structure required for a valid query property marker.
     * 
     * @param node
     *            the node to validate
     * @return the validation result
     */
    public static Validation validate(JexlNode node) {
        ValidQueryPropertyMarkerVisitor visitor = new ValidQueryPropertyMarkerVisitor();
        node.jjtAccept(visitor, null);
        return visitor.validation == null ? Validation.IS_VALID : visitor.validation;
    }
    
    public static class Validation {
        
        private static final Validation IS_VALID = new Validation(true, null);
        
        private static Validation invalid(String reason) {
            return new Validation(false, reason);
        }
        
        private final boolean valid;
        private final String reason;
        
        private Validation(boolean valid, String reason) {
            this.valid = valid;
            this.reason = reason;
        }
        
        public boolean isValid() {
            return valid;
        }
        
        public String getReason() {
            return reason;
        }
    }
    
    private Validation validation;
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        // If a validation result has been recorded, a violation has already been found.
        if (validation != null) {
            return data;
        }
        
        // Check if this is a marker.
        QueryPropertyMarker.Instance instance = QueryPropertyMarkerVisitor.getInstance(node);
        if (instance.isAnyType()) {
            // Verify that the marker does not have multiple source nodes.
            if (instance.hasMutipleSources()) {
                validation = Validation.invalid("Marker found with multiple sources: " + JexlStringBuildingVisitor.buildQuery(node));
                return null;
            }
            // Verify that the marker is wrapped with parens.
            JexlNode parent = node.jjtGetParent();
            if (!(parent instanceof ASTReferenceExpression && parent.jjtGetParent() instanceof ASTReference)) {
                validation = Validation.invalid("Unwrapped marker found: " + JexlStringBuildingVisitor.buildQuery(node));
            }
            return null;
        } else {
            return super.visit(node, data);
        }
    }
    
    // Pass through to children only if not invalid yet.
    
    @Override
    public Object visit(ASTReference node, Object data) {
        if (validation == null) {
            return super.visit(node, data);
        }
        return data;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        if (validation == null) {
            return super.visit(node, data);
        }
        return data;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        if (validation == null) {
            return super.visit(node, data);
        }
        return data;
    }
    
    // Do not pass through to children.
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
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
    public Object visit(ASTERNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        return data;
    }
}
