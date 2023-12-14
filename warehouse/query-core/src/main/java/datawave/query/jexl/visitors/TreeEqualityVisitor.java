package datawave.query.jexl.visitors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.commons.jexl3.parser.ASTAddNode;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTAnnotatedStatement;
import org.apache.commons.jexl3.parser.ASTAnnotation;
import org.apache.commons.jexl3.parser.ASTArguments;
import org.apache.commons.jexl3.parser.ASTArrayAccess;
import org.apache.commons.jexl3.parser.ASTArrayLiteral;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl3.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl3.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl3.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl3.parser.ASTBlock;
import org.apache.commons.jexl3.parser.ASTBreak;
import org.apache.commons.jexl3.parser.ASTConstructorNode;
import org.apache.commons.jexl3.parser.ASTContinue;
import org.apache.commons.jexl3.parser.ASTDecrementGetNode;
import org.apache.commons.jexl3.parser.ASTDefineVars;
import org.apache.commons.jexl3.parser.ASTDivNode;
import org.apache.commons.jexl3.parser.ASTDoWhileStatement;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTEWNode;
import org.apache.commons.jexl3.parser.ASTEmptyFunction;
import org.apache.commons.jexl3.parser.ASTExtendedLiteral;
import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.ASTForeachStatement;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTGetDecrementNode;
import org.apache.commons.jexl3.parser.ASTGetIncrementNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTIdentifierAccess;
import org.apache.commons.jexl3.parser.ASTIfStatement;
import org.apache.commons.jexl3.parser.ASTIncrementGetNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTJxltLiteral;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTMapEntry;
import org.apache.commons.jexl3.parser.ASTMapLiteral;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTModNode;
import org.apache.commons.jexl3.parser.ASTMulNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNEWNode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNSWNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTNullLiteral;
import org.apache.commons.jexl3.parser.ASTNullpNode;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTQualifiedIdentifier;
import org.apache.commons.jexl3.parser.ASTRangeNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTRegexLiteral;
import org.apache.commons.jexl3.parser.ASTReturnStatement;
import org.apache.commons.jexl3.parser.ASTSWNode;
import org.apache.commons.jexl3.parser.ASTSetAddNode;
import org.apache.commons.jexl3.parser.ASTSetAndNode;
import org.apache.commons.jexl3.parser.ASTSetDivNode;
import org.apache.commons.jexl3.parser.ASTSetLiteral;
import org.apache.commons.jexl3.parser.ASTSetModNode;
import org.apache.commons.jexl3.parser.ASTSetMultNode;
import org.apache.commons.jexl3.parser.ASTSetOrNode;
import org.apache.commons.jexl3.parser.ASTSetShiftLeftNode;
import org.apache.commons.jexl3.parser.ASTSetShiftRightNode;
import org.apache.commons.jexl3.parser.ASTSetShiftRightUnsignedNode;
import org.apache.commons.jexl3.parser.ASTSetSubNode;
import org.apache.commons.jexl3.parser.ASTSetXorNode;
import org.apache.commons.jexl3.parser.ASTShiftLeftNode;
import org.apache.commons.jexl3.parser.ASTShiftRightNode;
import org.apache.commons.jexl3.parser.ASTShiftRightUnsignedNode;
import org.apache.commons.jexl3.parser.ASTSizeFunction;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.ASTSubNode;
import org.apache.commons.jexl3.parser.ASTTernaryNode;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl3.parser.ASTUnaryPlusNode;
import org.apache.commons.jexl3.parser.ASTVar;
import org.apache.commons.jexl3.parser.ASTWhileStatement;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParserVisitor;
import org.apache.commons.jexl3.parser.SimpleNode;

import datawave.query.jexl.nodes.QueryPropertyMarker;

/**
 * Determine whether two trees are equivalent, accounting for arbitrary order within subtrees.
 */
public class TreeEqualityVisitor extends ParserVisitor {

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
            String firstImage = String.valueOf(JexlNodes.getImage((JexlNode) first));
            String secondImage = String.valueOf(JexlNodes.getImage((JexlNode) second));
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
    public Object visit(ASTJexlScript node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    public Object visit(ASTBlock node, Object data) {
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

    @Override
    protected Object visit(ASTDoWhileStatement node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTContinue node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTBreak node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTDefineVars node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTNullpNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTShiftLeftNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTShiftRightNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTShiftRightUnsignedNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSWNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTNSWNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTEWNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTNEWNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTAddNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSubNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTUnaryPlusNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTRegexLiteral node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetLiteral node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTExtendedLiteral node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTRangeNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTIdentifierAccess node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTArguments node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetAddNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetSubNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetMultNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetDivNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetModNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetAndNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetOrNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetXorNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetShiftLeftNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetShiftRightNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTSetShiftRightUnsignedNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTGetDecrementNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTGetIncrementNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTDecrementGetNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTIncrementGetNode node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTJxltLiteral node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTAnnotation node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTAnnotatedStatement node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }

    @Override
    protected Object visit(ASTQualifiedIdentifier node, Object data) {
        return checkEquality(node, (SimpleNode) data);
    }
}
