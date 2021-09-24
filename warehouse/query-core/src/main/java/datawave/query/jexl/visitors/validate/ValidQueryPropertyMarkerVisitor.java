package datawave.query.jexl.visitors.validate;

import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.visitors.QueryPropertyMarkerVisitor;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.JexlNode;

/**
 * This visitor verifies that all query property marker nodes present in a query tree have a singular root source node.
 */
public class ValidQueryPropertyMarkerVisitor extends BaseVisitor {
    
    /**
     * Verify whether all query property marker nodes present in the given query tree adhere to the rule that they must have a singular source node.
     * 
     * @param node
     *            the node to validate
     * @return true if the query tree contains no query property markers with multiple source nodes, or false otherwise
     */
    public static boolean validate(JexlNode node) {
        ValidQueryPropertyMarkerVisitor visitor = new ValidQueryPropertyMarkerVisitor();
        node.jjtAccept(visitor, null);
        return visitor.valid;
    }
    
    private boolean valid = true;
    
    /**
     * Return whether the query tree was determined to be valid.
     * 
     * @return true if the query tree is valid, or false otherwise
     */
    public boolean isValid() {
        return valid;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarkerVisitor.getInstance(node);
        // Only modify the node if it is a query property marker with multiple source nodes.
        if (instance.isAnyType()) {
            if (instance.hasMutipleSources()) {
                valid = false;
            }
            return data;
        } else {
            return super.visit(node, data);
        }
    }
}
