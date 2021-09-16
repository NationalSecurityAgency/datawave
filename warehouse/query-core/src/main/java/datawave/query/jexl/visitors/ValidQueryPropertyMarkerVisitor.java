package datawave.query.jexl.visitors;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.JexlNode;

/**
 * This visitor validates that all query property marker nodes present have a singular root source node.
 */
public class ValidQueryPropertyMarkerVisitor extends BaseVisitor {
    
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T validate(T node) {
        if (node == null) {
            return null;
        }
        
        ValidQueryPropertyMarkerVisitor visitor = new ValidQueryPropertyMarkerVisitor();
        return (T) node.jjtAccept(visitor, null);
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarkerVisitor.getInstance(node);
        // Only modify the node if it is a query property marker with multiple source nodes.
        if (instance.isAnyType()) {
            if (instance.hasMutipleSources()) {
                throw new DatawaveFatalQueryException("Query contains a query property marker with multiple source nodes: "
                                + JexlStringBuildingVisitor.buildQuery(node));
            } else {
                return data;
            }
        } else {
            return super.visit(node, data);
        }
    }
}
