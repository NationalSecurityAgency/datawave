package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.JexlNode;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This visitor will ensure that query marker nodes always have a singular, root source node. If multiple source nodes are found, they will be bundled within a
 * new {@link ASTAndNode} which will in turn be set as the new source node.
 */
public class QueryPropertyMarkerSourceConsolidator extends RebuildingVisitor {
    
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T consolidate(T node) {
        if (node == null) {
            return null;
        }
        
        QueryPropertyMarkerSourceConsolidator visitor = new QueryPropertyMarkerSourceConsolidator();
        return (T) node.jjtAccept(visitor, null);
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarkerVisitor.getInstance(node);
        // Only modify the node if it is a query property marker with multiple source nodes.
        if (instance.isAnyType() && instance.hasMutipleSources()) {
            // Make safe copies of the sources and wrap them in an AND node.
            List<JexlNode> copiedSources = instance.getSources().stream().map(RebuildingVisitor::copy).collect(Collectors.toList());
            JexlNode source = JexlNodeFactory.createAndNode(copiedSources);
            // Create a new query property marker of the same instance.
            JexlNode marker = QueryPropertyMarker.create(source, instance.getType());
            return JexlASTHelper.dereference(marker);
        } else {
            return super.visit(node, data);
        }
    }
}
