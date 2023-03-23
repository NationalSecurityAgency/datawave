package datawave.query.jexl.visitors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
import org.apache.commons.jexl2.parser.ASTArrayLiteral;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTBlock;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFloatLiteral;
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTIntegerLiteral;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMapEntry;
import org.apache.commons.jexl2.parser.ASTMapLiteral;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTModNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTReturnStatement;
import org.apache.commons.jexl2.parser.ASTSizeFunction;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTernaryNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.ASTVar;
import org.apache.commons.jexl2.parser.ASTWhileStatement;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserVisitor;
import org.apache.commons.jexl2.parser.SimpleNode;

/**
 * Determine whether two trees are equivalent, accounting for arbitrary order within subtrees.
 */
public class TreeEqualityVisitor implements ParserVisitor {
    
    public final static class Comparison {
        
        private static final Comparison IS_EQUAL = new Comparison(true, null);
        
        private static Comparison notEqual(String reason) {
            return new Comparison(false, reason);
        }
        
        private final boolean equal;
        private final String reason;
        
        private Comparison(boolean equal, String reason) {
            this.equal = equal;
            this.reason = reason;
        }
        
        public boolean isEqual() {
            return equal;
        }
        
        public String getReason() {
            return reason;
        }
    }
    
    /**
     * Return whether the provided query trees are considered equivalent.
     * 
     * @param first
     *            the first query tree
     * @param second
     *            the second query tree
     * @return true if the query trees are considered equivalent, or false otherwise
     */
    public static boolean isEqual(JexlNode first, JexlNode second) {
        return checkEquality(first, second).isEqual();
    }
    
    /**
     * Compare the provided query trees for equivalency and return the resulting comparison.
     * 
     * @param first
     *            the first query tree
     * @param second
     *            the second query tree
     * @return the comparison result
     */
    public static Comparison checkEquality(JexlNode first, JexlNode second) {
        if (first == second) {
            return Comparison.IS_EQUAL;
        } else if (first == null || second == null) {
            return Comparison.notEqual("One tree is null: " + first + " vs " + second);
        } else {
            TreeEqualityVisitor visitor = new TreeEqualityVisitor();
            return (Comparison) first.jjtAccept(visitor, second);
        }
    }

    /**
     * Compare the given node and evaluate their equivalence. This visitor will be applied to each of the first node's children. The visitor will be accepted on
     * all the first node's children.
     *
     * @param node1
     *            first node
     * @param node2
     *            second node
     * @return result of visit
     **/
    private Comparison checkEquality(SimpleNode node1, SimpleNode node2) {
        // Compare the classes.
        Comparison comparison = compareClasses(node1, node2);
        if (!comparison.isEqual()) {
            return comparison;
        }

        // Compare the values.
        comparison = compareValues(node1, node2);
        if (!comparison.isEqual()) {
            return comparison;
        }

        // Compare the images.
        comparison = compareImages(node1, node2);
        if (!comparison.isEqual()) {
            return comparison;
        }

        // Compare the children.
        return compareChildren(node1, node2);
    }

    /**
     * Compare the classes of both nodes. Note: any nodes that are a subclass of {@link QueryPropertyMarker} will be considered equivalent to a
     * {@link ASTReference} since the underlying child trees may later be found to be equivalent.
     *
     * @param first
     *            the first node
     * @param second
     *            the second node
     * @return the comparison result
     */
    private Comparison compareClasses(SimpleNode first, SimpleNode second) {
        Class<?> firstClass = first.getClass();
        Class<?> secondClass = second.getClass();
        if (firstClass.equals(secondClass) || possiblyEqualQueryPropertyMarkers(firstClass, secondClass)) {
            return Comparison.IS_EQUAL;
        } else {
            return Comparison.notEqual("Classes differ: " + firstClass.getSimpleName() + " vs " + secondClass.getSimpleName());
        }
    }

    /**
     * Returns whether either the first or second node is an {@link ASTReference}, and the other node is any subclass of {@link QueryPropertyMarker}.
     *
     * @param first
     *            the first node
     * @param second
     *            the second node
     * @return true if the classes indicate the possible presence of equivalent query property markers
     */
    private boolean possiblyEqualQueryPropertyMarkers(Class<?> first, Class<?> second) {
        return (first.equals(ASTReference.class) && ASTReference.class.isAssignableFrom(second))
                        || (second.equals(ASTReference.class) && ASTReference.class.isAssignableFrom(first));
    }

    /**
     * Compare the values of both node.
     *
     * @param first
     *            the first node
     * @param second
     *            the second node
     * @return the comparison result
     */
    private Comparison compareValues(SimpleNode first, SimpleNode second) {
        return Objects.equals(first.jjtGetValue(), second.jjtGetValue()) ? Comparison.IS_EQUAL : Comparison.notEqual("Node values differ: "
                        + first.jjtGetValue() + " vs " + second.jjtGetValue());
    }

    /**
     * Compare the images of both node (if applicable).
     *
     * @param first
     *            the first node
     * @param second
     *            the second node
     * @return the comparison result
     */
    private Comparison compareImages(SimpleNode first, SimpleNode second) {
        if (first instanceof JexlNode) {
            String firstImage = ((JexlNode) first).image;
            String secondImage = ((JexlNode) second).image;
            if (!Objects.equals(firstImage, secondImage)) {
                return Comparison.notEqual("Node images differ: " + firstImage + " vs " + secondImage);
            }
        }
        return Comparison.IS_EQUAL;
    }

    /**
     * Compare the children of both nodes.
     *
     * @param first
     *            the first node
     * @param second
     *            the second node
     * @return the comparison result
     */
    private Comparison compareChildren(SimpleNode first, SimpleNode second) {
        List<SimpleNode> firstChildren = getChildren(first);
        List<SimpleNode> secondChildren = getChildren(second);

        // Compare the sizes.
        if (firstChildren.size() != secondChildren.size()) {
            return Comparison.notEqual("Num children differ: " + firstChildren + " vs " + secondChildren);
        }

        // Look for an equivalent of each child, visiting each child recursively when needed.
        Comparison currentComparison = null;
        for (SimpleNode firstChild : firstChildren) {
            for (int i = 0; i < secondChildren.size(); i++) {
                SimpleNode secondChild = secondChildren.get(i);
                currentComparison = (Comparison) firstChild.jjtAccept(this, secondChild);
                if (currentComparison.isEqual()) {
                    secondChildren.remove(i);
                    break;
                }
            }

            if (!currentComparison.isEqual()) {
                return Comparison.notEqual("Did not find a matching child for " + firstChild + " in " + secondChildren + ": " + currentComparison.getReason());
            }
        }

        return Comparison.IS_EQUAL;
    }
    
    /**
     * Return the flattened children of the given node.
     *
     * @param node
     *            the node to get the children of
     * @return the flattened children
     */
    private List<SimpleNode> getChildren(SimpleNode node) {
        Class<?> nodeType = node.getClass();
        List<SimpleNode> children = new ArrayList<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            children.addAll(flatten(node.jjtGetChild(i), nodeType));
        }
        return children;
    }

    /**
     * Return a flattened version of the given node if possible, expanding the node to its base comparable children.
     *
     * @param node
     *            the node to flatten
     * @param rootType
     *            the type of the original root node
     * @return the flattened node components.
     */
    private List<SimpleNode> flatten(SimpleNode node, Class<?> rootType) {
        if (isFlattenable(node, rootType)) {
            List<SimpleNode> children = new ArrayList<>();
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                children.addAll(flatten(node.jjtGetChild(i), rootType));
            }
            return children;
        } else {
            return Collections.singletonList(node);
        }
    }

    /**
     * Return whether the node is considered flattenable, e.g. can we further unwrap it. This specifically does NOT apply to query property marker nodes, they
     * must be compared as a whole to verify that wrapped query property markers are only considered equal to similarly wrapped equivalents. For instance, the
     * query {@code (_Bounded_ = true) && (NUM > '1' && NUM < '5')} is not considered equivalent to {@code ((_Bounded_ = true) && (NUM > '1' && NUM < '5'))}.
     *
     * @param node
     *            the node
     * @param rootType
     *            the type of the original root node
     * @return true if the node can be flattened to its children, or false otherwise
     */
    private boolean isFlattenable(SimpleNode node, Class<?> rootType) {
        Class<?> nodeType = node.getClass();
        boolean hasSingleChild = node.jjtGetNumChildren() == 1;
        // @formatter:off
        return (nodeType.equals(ASTReference.class) && hasSingleChild && !isFlattenedQueryPropertyMarker(node))
                        || (nodeType.equals(ASTReferenceExpression.class) && hasSingleChild)
                        || (nodeType.equals(ASTOrNode.class) && (hasSingleChild || rootType.equals(ASTOrNode.class)))
                        || (nodeType.equals(ASTAndNode.class) && (hasSingleChild || rootType.equals(ASTAndNode.class)));
        // @formatter:on
    }
    
    /**
     * Return whether the node is a query property marker node that has been flattened down to its last wrapping parentheses, e.g. flattened from something like
     * {@code ((((_Bounded_ = true) && (NUM > '1' && NUM < '5'))))} to {@code ((_Bounded_ = true) && (NUM > '1' && NUM < '5'))}
     *
     * @param node
     *            the node
     * @return true if the node is a query property marker or false otherwise
     */
    private boolean isFlattenedQueryPropertyMarker(SimpleNode node) {
        JexlNode child = node.jjtGetChild(0);
        // @formatter:off
        // Perform a fast check of whether the structure looks like:
        // Reference
        //   ReferenceExpression
        //     AndNode
        // before checking if this is a query property marker. If it does not look like the above, it's either not a query property marker, or it's a query
        // property marker with extra wrapping that needs to be flattened.
        // @formatter:on
        return child instanceof ASTReferenceExpression && child.jjtGetNumChildren() == 1 && child.jjtGetChild(0) instanceof ASTAndNode
                        && QueryPropertyMarker.findInstance((JexlNode) node).isAnyType();
    }
    
    @Override
    public Object visit(SimpleNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTBlock node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTAmbiguous node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTIfStatement node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTAdditiveNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTMulNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTDivNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTModNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    public Object visit(ASTIntegerLiteral node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    public Object visit(ASTFloatLiteral node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTMapEntry node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTSizeMethod node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTVar node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
}
