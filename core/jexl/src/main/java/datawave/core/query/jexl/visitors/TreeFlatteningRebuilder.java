package datawave.core.query.jexl.visitors;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import datawave.core.query.jexl.JexlASTHelper;

/**
 * This will flatten ands and ors. If requested this will also remove reference expressions where possible. A reference expression represents a set of
 * parentheses in the printed query - that node will be removed except when it is used for a marked node, or when it is being used within a NOT node. NOTE: If
 * you remove reference expressions, this may adversely affect the jexl evaluation of the query.
 */
public class TreeFlatteningRebuilder {
    private static final Logger log = Logger.getLogger(TreeFlatteningRebuilder.class);
    private final boolean simplifyReferenceExpressions;

    public TreeFlatteningRebuilder(boolean simplifyReferenceExpressions) {
        this.simplifyReferenceExpressions = simplifyReferenceExpressions;
    }

    /**
     * This will flatten ands and ors.
     *
     * @param node
     *            a node
     * @param <T>
     *            node type
     * @return a jexl node
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T flatten(T node) {
        return flatten(node, false);
    }

    /**
     * This will flatten ands, ors, and references expressions NOTE: If you remove reference expressions, this may adversely affect the evaluation of the query
     * (true in the index query logic case: bug?).
     *
     * @param node
     *            a node
     * @param <T>
     *            node type
     * @return a jexl node
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T flattenAll(T node) {
        return flatten(node, true);
    }

    /**
     * This will flatten ands and ors. If requested this will also remove reference expressions where possible. NOTE: If you remove reference expressions, this
     * may adversely affect the evaluation of the query (true in the index query logic case: bug?).
     *
     * @param <T>
     *            type of the node
     * @param simplifyReferenceExpressions
     *            flag to simplify reference expressions
     * @param rootNode
     *            the root node
     * @return the flattened copy
     */
    @SuppressWarnings("unchecked")
    private static <T extends JexlNode> T flatten(T rootNode, boolean simplifyReferenceExpressions) {
        TreeFlatteningRebuilder visitor = new TreeFlatteningRebuilder(simplifyReferenceExpressions);
        return visitor.flattenTree(rootNode);
    }

    /**
     * Given a JexlNode, creates a copy of that node which has had it's AND and OR nodes flattened.
     *
     * @param rootNode
     *            the node to flatten
     * @param <T>
     *            type of the node
     * @return the flattened copy
     */
    public <T extends JexlNode> T flattenTree(T rootNode) {

        Deque<JexlNode> postOrderStack = new LinkedList<>();

        // iteratively copy the root node, and create the post order traversal stack
        copyTree(rootNode, postOrderStack);

        // use the copied post order traversal stack to iteratively flatten the tree
        JexlNode newRoot = flattenTree(postOrderStack);

        return (T) newRoot;
    }

    /**
     * Given a stack of nodes, representing the post order traversal of a JexlNode, iteratively flattens the AND and OR nodes of the tree.
     *
     * @param postOrderStack
     *            the post order traversal representation of the tree
     * @return the flattened tree
     */
    private JexlNode flattenTree(Deque<JexlNode> postOrderStack) {
        Deque<JexlNode> parentStack = new LinkedList<>();
        Deque<List<JexlNode>> childrenStack = new LinkedList<>();

        JexlNode newNode = null;

        // now that we have the post order traversal, we can operate on the nodes...
        while (!postOrderStack.isEmpty()) {
            JexlNode node = postOrderStack.pop();

            boolean hasChildren = node.jjtGetNumChildren() > 0;

            // if this is a reference node, flatten it
            if (hasChildren && node instanceof ASTReferenceExpression) {
                newNode = flattenReferenceExpression(
                                (ASTReferenceExpression) JexlNodes.setChildren(parentStack.pop(), childrenStack.pop().toArray(new JexlNode[0])));
            }
            // if this is an AND or OR node, flatten it
            else if (hasChildren && (node instanceof ASTOrNode || node instanceof ASTAndNode)) {
                newNode = flattenAndOrNode(JexlNodes.setChildren(parentStack.pop(), childrenStack.pop().toArray(new JexlNode[0])));
            }
            // if this is a node with children, assign the children
            else if (hasChildren && node == parentStack.peek()) {
                newNode = JexlNodes.setChildren(parentStack.pop(), childrenStack.pop().toArray(new JexlNode[0]));
            }
            // if this is a leaf node, just keep it
            else {
                newNode = node;
            }

            // if we still have nodes to evaluate
            if (!postOrderStack.isEmpty()) {

                // if the original node's parent is NOT the next one on the parent stack,
                // then this is a new parent node. add it to the parent stack, and add a new list of children.
                // otherwise, add this node to the existing parent's list of children
                if (node.jjtGetParent() != parentStack.peek()) {
                    parentStack.push(node.jjtGetParent());
                    childrenStack.push(Lists.newArrayList(newNode));
                } else {
                    childrenStack.peek().add(newNode);
                }
            }
        }

        return newNode;
    }

    /**
     * Iteratively creates a copy of the passed in JexlNode
     *
     * @param node
     *            the node to be copied
     * @param postOrderDeque
     *            the post order traversal of the copied tree
     * @return the copied tree
     */
    private JexlNode copyTree(JexlNode node, Deque<JexlNode> postOrderDeque) {
        // add all the nodes to the stack and iterate...
        Deque<JexlNode> workingStack = new LinkedList<>();

        // create a copy of this node which shares the same children as the original node
        JexlNode copiedNode = rebuildNode(node);
        workingStack.push(copiedNode);

        // compute the post order traversal of all of the nodes, and copy them
        while (!workingStack.isEmpty()) {
            JexlNode poppedNode = workingStack.pop();
            postOrderDeque.push(poppedNode);

            // if this node has children, create copies of them
            if (poppedNode.jjtGetNumChildren() > 0) {
                List<JexlNode> copiedChildren = new ArrayList<>();
                List<JexlNode> children;
                if (poppedNode instanceof ASTAndNode || poppedNode instanceof ASTOrNode) {
                    children = getAndOrLeaves(poppedNode);
                } else {
                    children = new ArrayList<>();
                    for (int i = 0; i < poppedNode.jjtGetNumChildren(); i++) {
                        children.add(poppedNode.jjtGetChild(i));
                    }
                }
                for (JexlNode child : children) {
                    if (child != null) {

                        // create a copy of this node which shares the same children as the original node
                        JexlNode copiedChild = rebuildNode(child);

                        copiedChildren.add(copiedChild);
                        workingStack.push(copiedChild);
                    }
                }

                // Reassign the children for this copied node
                JexlNodes.setChildren(poppedNode, copiedChildren.toArray(new JexlNode[0]));
            }
        }

        return copiedNode;
    }

    private List<JexlNode> getAndOrLeaves(JexlNode node) {
        LinkedList<JexlNode> children = new LinkedList<>();
        LinkedList<JexlNode> stack = new LinkedList<>();
        stack.push(node);

        while (!stack.isEmpty()) {
            JexlNode currNode = stack.pop();

            // only add children if
            // 1) this is the original node, or
            // 2) this node is the same type as the root node and
            // -- a) this is an OR node, or
            // -- b) this is an AND node (but not a bounded range)
            // @formatter:off
            if (currNode == node ||
                    (node.getClass().isInstance(currNode) &&
                            (currNode instanceof ASTOrNode ||
                                    (currNode instanceof ASTAndNode && !isBoundedRange((ASTAndNode) currNode))))) {
                // @formatter:on
                for (int i = 0; i < currNode.jjtGetNumChildren(); i++) {
                    stack.push(currNode.jjtGetChild(i));
                }
            } else {
                children.push(currNode);
            }
        }

        return children;
    }

    /**
     * Returns a copy of the passed in node.
     *
     * If the original node has children, those exact children (not copies) will be added to the copied node. However, the parentage of those child nodes will
     * be left as-is.
     *
     * If the original node has no children, we will simply use the RebuildingVisitor to copy the node.
     *
     * @param node
     *            the node to copy
     * @return the copied node
     */
    private JexlNode rebuildNode(JexlNode node) {
        JexlNode newNode;
        if (node.jjtGetNumChildren() == 0) {
            newNode = RebuildingVisitor.copy(node);
        } else {
            newNode = JexlNodes.newInstanceOfType(node);
            JexlNodes.copyIdentifierOrLiteral(node, newNode);
            JexlNodes.ensureCapacity(newNode, node.jjtGetNumChildren());

            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                newNode.jjtAddChild(node.jjtGetChild(i), i);
            }
        }
        return newNode;
    }

    /**
     * Determine whether the and node represents a bounded range.
     *
     * @param node
     *            the and node to check
     * @return true if the and node contains a bounded range, false otherwise
     */
    private boolean isBoundedRange(ASTAndNode node) {
        if (node.jjtGetNumChildren() == 2) {
            JexlNode firstChild = node.jjtGetChild(0);
            JexlNode secondChild = node.jjtGetChild(1);
            if (isLowerBound(firstChild) && isUpperBound(secondChild)) {
                return isIdentifierEqual(firstChild, secondChild);
            } else if (isUpperBound(firstChild) && isLowerBound(secondChild)) {
                return isIdentifierEqual(secondChild, firstChild);
            }
        }
        return false;
    }

    private boolean isIdentifierEqual(JexlNode lower, JexlNode upper) {
        try {
            String leftField = JexlASTHelper.getIdentifier(lower);
            String rightField = JexlASTHelper.getIdentifier(upper);
            return leftField.equals(rightField);
        } catch (Exception e) {
            log.info("Unable to compare identifiers.");
        }
        return false;
    }

    private boolean isLowerBound(JexlNode node) {
        return node instanceof ASTGTNode || node instanceof ASTGENode;
    }

    private boolean isUpperBound(JexlNode node) {
        return node instanceof ASTLTNode || node instanceof ASTLENode;
    }

    /**
     * Given a reference expression node, if we are configured to simplify reference expressions, we will attempt to remove reference expressions, where
     * possible. Otherwise, we will preserve them.
     *
     * @param node
     *            the reference expression node to flatten
     * @return the flattened reference expression node
     */
    private JexlNode flattenReferenceExpression(ASTReferenceExpression node) {
        JexlNode parent = node.jjtGetParent();

        // if we are told not to remove references OR
        // if this is a marked node OR
        // if this is an assignment node OR
        // if the parent is a NOT node, keep the reference
        if (!simplifyReferenceExpressions || parent instanceof ASTNotNode || JexlASTHelper.dereference(node) instanceof ASTAssignment
                        || QueryPropertyMarkerVisitor.getInstance(node).isAnyType()) {
            return node;
        }
        // if this is a reference expression with a single child, remove the reference expression
        else if (node.jjtGetNumChildren() == 1) {
            return node.jjtGetChild(0);
        }

        // otherwise, keep the reference expression
        return node;
    }

    /**
     * Given an AND or OR node, this method will determine which child nodes can be merged into the parent node.
     *
     * @param node
     *            the node we are flattening into
     * @return the flattened version of node
     */
    private JexlNode flattenAndOrNode(JexlNode node) {

        if (!(node instanceof ASTAndNode || node instanceof ASTOrNode)) {
            log.error("Only ASTAndNodes and ASTOrNodes can be flattened!");
            throw new RuntimeException("Only ASTAndNodes and ASTOrNodes can be flattened!");
        }

        // if the AND/OR node only has a single child, just return the child
        if (node.jjtGetNumChildren() == 1) {
            return node.jjtGetChild(0);
        }
        // if there are multiple children, determine which ones can be flattened into the parent
        else {

            Deque<JexlNode> children = new LinkedList<>();
            Deque<JexlNode> stack = new LinkedList<>();
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                stack.push(node.jjtGetChild(i));
            }

            while (!stack.isEmpty()) {
                JexlNode poppedNode = stack.pop();
                JexlNode dereferenced = JexlASTHelper.dereference(poppedNode);

                if (acceptableNodesToCombine(node, dereferenced, poppedNode != dereferenced)) {
                    for (int i = 0; i < dereferenced.jjtGetNumChildren(); i++) {
                        stack.push(dereferenced.jjtGetChild(i));
                    }
                } else {
                    children.push(poppedNode);
                }
            }

            return JexlNodes.setChildren(node, children.toArray(new JexlNode[0]));
        }
    }

    /**
     * Determines whether the candidate node can be flattened into the parent node.
     *
     * @param parentNode
     *            the parent node to flatten into
     * @param candidateNode
     *            the potential node to flatten into the parent
     * @param isWrapped
     *            whether or not the candidate node is wrapped
     * @return true if candidateNode can be flattened into parentNode, false otherwise
     */
    private boolean acceptableNodesToCombine(JexlNode parentNode, JexlNode candidateNode, boolean isWrapped) {
        // do not combine nodes unless they are the same type
        if (parentNode.getClass().equals(candidateNode.getClass())) {

            // if this is an AND node, do not combine if either of these conditions are true
            // 1) candidateNode is a bounded range or marker node
            // 2) candidateNode is wrapped, and the candidateNode or parentNode is a marked node
            // @formatter:off
            return  !(candidateNode instanceof ASTAndNode &&
                    (isBoundedRange((ASTAndNode) candidateNode) ||
                            (isWrapped && (QueryPropertyMarkerVisitor.getInstance(candidateNode).isAnyType() ||
                                    QueryPropertyMarkerVisitor.getInstance(parentNode).isAnyType()))));
            // @formatter:on
        }

        return false;
    }
}
