package datawave.query.jexl.visitors;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Visitor that enforces node uniqueness within AND or OR expressions. Nodes can be single nodes or subtrees.
 * 
 * <pre>
 * For example:
 * {@code (A || A) => (A)}
 * {@code (A && A) => (A)}
 * </pre>
 * 
 * This visitor returns a copy of the original query tree, and flattens the copy via the {@link TreeFlatteningRebuildingVisitor}
 * <p>
 * Node traversal is post order.
 */
public class UniqueExpressionTermsVisitor extends RebuildingVisitor {
    
    private int duplicates = 0;
    
    private static final Logger log = Logger.getLogger(UniqueExpressionTermsVisitor.class);
    
    /**
     * Apply this visitor to the query tree.
     *
     * @param node
     *            - the root node for a query tree
     * @param <T>
     *            type of the node
     * @return a reference to the tree
     */
    public static <T extends JexlNode> T enforce(T node) {
        if (node == null)
            return null;
        
        // Operate on copy of query tree.
        T copy = (T) copy(node);
        
        // Flatten query tree prior to visit.
        copy = TreeFlatteningRebuildingVisitor.flatten(copy);
        
        // Visit and enforce unique nodes within expressions.
        UniqueExpressionTermsVisitor visitor = new UniqueExpressionTermsVisitor();
        copy = (T) copy.jjtAccept(visitor, null);
        
        if (log.isDebugEnabled()) {
            log.debug(UniqueExpressionTermsVisitor.class.getSimpleName() + " removed " + visitor.duplicates + " duplicate terms");
        }
        return copy;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        return removeDuplicateNodes(node, data);
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        return removeDuplicateNodes(node, data);
    }
    
    private JexlNode removeDuplicateNodes(JexlNode node, Object data) {
        // Traverse each child to de-dupe their children.
        List<JexlNode> visitedChildren = new ArrayList<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode childCopy = (JexlNode) node.jjtGetChild(i).jjtAccept(this, data);
            if (childCopy != null) {
                visitedChildren.add(childCopy);
            }
        }
        
        // Dedupe the visited children.
        List<JexlNode> uniqueChildren = getUniqueChildren(visitedChildren);
        
        if (uniqueChildren.size() == 1) {
            // If only one child remains, return it.
            return uniqueChildren.get(0);
        } else {
            // If two or more children remain, return a copy with the unique children.
            JexlNode copy = JexlNodes.newInstanceOfType(node);
            JexlNodes.copyImage(node, copy);
            copy.jjtSetParent(node.jjtGetParent());
            JexlNodes.setChildren(copy, uniqueChildren.toArray(new JexlNode[0]));
            return copy;
        }
    }
    
    private List<JexlNode> getUniqueChildren(List<JexlNode> nodes) {
        Set<String> childKeys = new HashSet<>();
        List<JexlNode> unique = new ArrayList<>();
        for (JexlNode node : nodes) {
            String childKey = JexlStringBuildingVisitor.buildQueryWithoutParse(TreeFlatteningRebuildingVisitor.flatten(node), true);
            if (childKeys.add(childKey)) {
                unique.add(node);
            } else {
                this.duplicates++;
            }
        }
        return unique;
    }
}
