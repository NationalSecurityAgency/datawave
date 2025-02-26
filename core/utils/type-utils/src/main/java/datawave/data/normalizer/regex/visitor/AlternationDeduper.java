package datawave.data.normalizer.regex.visitor;

import java.util.LinkedHashMap;

import datawave.data.normalizer.regex.AlternationNode;
import datawave.data.normalizer.regex.ExpressionNode;
import datawave.data.normalizer.regex.Node;

public class AlternationDeduper extends CopyVisitor {
    
    public static Node dedupe(Node node) {
        if (node == null) {
            return null;
        }
        AlternationDeduper visitor = new AlternationDeduper();
        return (Node) node.accept(visitor, null);
    }
    
    @Override
    public Object visitExpression(ExpressionNode node, Object data) {
        // If the node holds an alternation, dedupe the alternation's children.
        if (node.getFirstChild() instanceof AlternationNode) {
            Node visited = (Node) node.getFirstChild().accept(this, data);
            // If an alternation was returned, multiple patterns were retained. Wrap it in an expression node before returning.
            if (visited instanceof AlternationNode) {
                return new ExpressionNode(visited);
            } else {
                // Otherwise we only have a single pattern remaining. Return the node as is.
                return visited;
            }
        } else {
            // Otherwise this tree does not hold any alternations. Return a copy.
            return copy(node);
        }
    }
    
    @Override
    public Object visitAlternation(AlternationNode node, Object data) {
        // Use LinkedHashMap to preserve insertion order.
        LinkedHashMap<String,Node> uniquePatterns = new LinkedHashMap<>();
        // Check each child for uniqueness.
        for (Node child : node.getChildren()) {
            String childPattern = StringVisitor.toString(child);
            // If the child has a pattern we have not seen before, retain a copy of it.
            if (!uniquePatterns.containsKey(childPattern)) {
                uniquePatterns.put(childPattern, copy(child));
            }
        }
        
        // If only one
        if (uniquePatterns.size() == 1) {
            return uniquePatterns.values().iterator().next();
        } else {
            return new AlternationNode(uniquePatterns.values());
        }
    }
}
