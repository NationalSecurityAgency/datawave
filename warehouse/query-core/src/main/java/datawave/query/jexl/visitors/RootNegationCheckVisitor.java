package datawave.query.jexl.visitors;

import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.JexlNode;

/**
 * Determine if all query terms are negated
 */
public class RootNegationCheckVisitor {
    
    /**
     * Since negations may be pushed down instead of simply looking for a top level negation, look for a leaf node without a negation.
     * 
     * @param script
     * @return true if there is no path to a leaf node without passing through a negation, false otherwise
     */
    public static Boolean hasTopLevelNegation(JexlNode script) {
        RootNegationCheckVisitor visitor = new RootNegationCheckVisitor();
        
        // ensure all negations are pushed down
        JexlNode pushedDownTree = PushdownNegationVisitor.pushdownNegations(script);
        return !visitor.hasLeafWithoutNegation(pushedDownTree);
    }
    
    /**
     * For each child skip over any property markers and look for negations and leafs. If neither is found examine that child until either a negation or leaf is
     * found
     * 
     * @param node
     * @return true if a leaf node was found, false otherwise
     */
    private boolean hasLeafWithoutNegation(JexlNode node) {
        boolean foundLeaf = false;
        for (int i = 0; i < node.jjtGetNumChildren() && !foundLeaf; i++) {
            JexlNode child = node.jjtGetChild(i);
            Class<?> childClass = child.getClass();
            
            // skip over any query property markers/assignments
            QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(child);
            if (instance.isAnyType()) {
                child = instance.getSource();
            }
            
            if (!(ASTNENode.class.equals(childClass) || ASTNotNode.class.equals(childClass) || ASTNRNode.class.equals(childClass))) {
                // either reached a leaf node without encountering a negation, or we need to continue looking
                foundLeaf = child.jjtGetNumChildren() == 0 || hasLeafWithoutNegation(child);
            }
        }
        
        return foundLeaf;
    }
}
