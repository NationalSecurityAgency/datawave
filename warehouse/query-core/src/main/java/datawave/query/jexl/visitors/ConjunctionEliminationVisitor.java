package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.commons.jexl2.parser.ParserTreeConstants.JJTORNODE;

/**
 * Visitor that removes conjunction children of OR nodes that are made redundant by virtue of distributed equivalency. For example:
 *
 * <pre>
 * {@code (A && B) || A --> A}
 * {@code ((A && C) && B) || (A && C) --> (A && C)}
 * {@code ((A || B) && C) || A || B --> A || B}
 * {@code (A && B && C) || A --> A}
 * </pre>
 *
 * The following cases will not be affected:
 *
 * <pre>
 * {@code (A && B) || C}
 * {@code ((A || B) && C) || A}
 * </pre>
 *
 * This visitor returns a copy of the original query tree, and flattens the copy via {@link TreeFlatteningRebuildingVisitor}.
 * <p>
 * Node traversal is post-order.
 */
public class ConjunctionEliminationVisitor extends RebuildingVisitor {

    private static final Logger log = Logger.getLogger(ConjunctionEliminationVisitor.class);

    /**
     * Given a JexlNode, determine if any redundant conjunctions in the node can be removed. The query will be flattened before applying this visitor.
     *
     * @param node
     *            a query node
     * @param <T>
     *            type of node
     * @return a re-written query tree for the node
     */
    public static <T extends JexlNode> T optimize(T node) {
        if (node == null) {
            return null;
        }

        // Operate on copy of query tree.
        T copy = (T) copy(node);

        // Flatten the tree.
        copy = TreeFlatteningRebuildingVisitor.flatten(copy);

        // Visit and enforce collapsing redundant nodes within expression.
        ConjunctionEliminationVisitor visitor = new ConjunctionEliminationVisitor();
        return (T) copy.jjtAccept(visitor, null);
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        return removeRedundantConjunctions(node);
    }

    // Return a node with redundant conjunctions removed.
    private JexlNode removeRedundantConjunctions(JexlNode node) {
        try {
            List<JexlNode> children = getNonRedundantChildren(node);
            if (children.size() == 1) {
                // If only one child remains, return it.
                return children.get(0);
            } else if (children.size() < node.jjtGetNumChildren()) {
                // If there were some redundant children, but more than one relevant child, return a new OR node with the relevant children.
                JexlNode copy = new ASTOrNode(JJTORNODE);
                copy.image = node.image;
                JexlNodes.children(copy, children.toArray(new JexlNode[0]));
                return copy;
            } else {
                // If no children are redundant, return a copy of the original node.
                return copy(node);
            }
        } catch (ParseException e) {
            if (log.isDebugEnabled()) {
                log.debug("Failed to parse child node to check for equivalency", e);
            }
        }
        return node;
    }

    // Return a list of all non-redundant children.
    private List<JexlNode> getNonRedundantChildren(JexlNode node) throws ParseException {
        List<JexlNode> children = new ArrayList<>();
        int totalChildren = node.jjtGetNumChildren();
        for (int i = 0; i < totalChildren; i++) {
            JexlNode child = node.jjtGetChild(i);
            // If the current child is a conjunction, check if any other child makes it redundant.
            // If not, keep it as a child.
            if (isConjunction(getFirstNonASTReference(child))) {
                if (!isRedundant(node, i)) {
                    children.add(child);
                }
            } else {
                // Automatically keep all non-conjunction children.
                children.add(child);
            }
        }
        return children;
    }

    // Return whether or not if the specified conjunction node is redundant and can be removed.
    private boolean isRedundant(JexlNode node, int conjunctionIndex) throws ParseException {
        List<JexlNode> children = getNonASTReferenceChildren(node);
        JexlNode conjunction = children.remove(conjunctionIndex);
        List<JexlNode> conjunctionChildren = getNonASTReferenceChildren(conjunction);
        // Check for an equivalent match to any of the conjunction's children.
        for (JexlNode child : conjunctionChildren) {
            if (isDisjunction(child) && containsEquivalentDisjunction(child, children)) {
                return true;
            } else if (containsEquivalent(child, children)) {
                return true;
            }
        }

        // Handle the case where there may be a match between a conjunction and a subset of that conjunction. This cannot be handled in the previous loop since
        // it requires an iteration over the original list of children.
        for (JexlNode child : children) {
            if (isConjunction(child) && isSubsetOf(child, conjunctionChildren)) {
                return true;
            }
        }
        return false;
    }

    // Return whether or not if an equivalent disjunction is found in the list of nodes for the provided disjunction.
    private boolean containsEquivalentDisjunction(JexlNode disjunction, List<JexlNode> nodes) throws ParseException {
        ASTJexlScript script = getScript(disjunction);
        List<JexlNode> uniqueChildren = getNonASTReferenceChildren(disjunction);
        for (JexlNode node : nodes) {
            // Check if the node is an equivalent disjunction.
            if (isDisjunction(node) && isEquivalent(node, script)) {
                return true;
            } else {
                // Otherwise, remove equivalent matches from the disjunction's children.
                removeEquivalentNodes(node, uniqueChildren);
            }
        }

        // No equivalent match was found in the list, but the list can still be considered to contain an equivalent disjunction overall if the list contains
        // an equivalent match for each term of the provided disjunction. If there are no unique children left after removing matches, then the list contains
        // an equivalent disjunction. This handles cases similar to ((A || B) && C) || A || B, which should reduce to A || B.
        return uniqueChildren.isEmpty();
    }

    // Remove all nodes from the provided list of nodes that are equivalent to the provided node.
    private void removeEquivalentNodes(JexlNode node, List<JexlNode> nodes) throws ParseException {
        ASTJexlScript script = getScript(node);
        Iterator<JexlNode> iterator = nodes.iterator();
        while (iterator.hasNext()) {
            if (isEquivalent(iterator.next(), script)) {
                iterator.remove();
            }
        }
    }

    // Return whether or not the provided conjunction has a match for each of it's children in the provided list of children. This will also return true if the
    // provided conjunction is a subset of the list. This handles the case ((A && B && C) && D) || (A && B), which should reduce to (A && B).
    private boolean isSubsetOf(JexlNode conjunction, List<JexlNode> nodes) throws ParseException {
        List<JexlNode> children = getNonASTReferenceChildren(conjunction);
        for (JexlNode child : children) {
            if (!containsEquivalent(child, nodes)) {
                return false;
            }
        }
        return true;
    }

    // Return whether or not the provided list of nodes contains any node that is equivalent to the other node.
    private boolean containsEquivalent(JexlNode node, List<JexlNode> nodes) throws ParseException {
        ASTJexlScript script = getScript(node);
        for (JexlNode other : nodes) {
            if (isEquivalent(other, script)) {
                return true;
            }
        }
        return false;
    }

    // Return a list of the node's unwrapped children.
    private List<JexlNode> getNonASTReferenceChildren(JexlNode node) {
        int totalChildren = node.jjtGetNumChildren();
        List<JexlNode> children = new ArrayList<>(totalChildren);
        for (int i = 0; i < totalChildren; i++) {
            children.add(getFirstNonASTReference(node.jjtGetChild(i)));
        }
        return children;
    }

    // Return the first non-wrapped node.
    private JexlNode getFirstNonASTReference(JexlNode node) {
        if (node instanceof ASTReference || node instanceof ASTReferenceExpression) {
            return getFirstNonASTReference(node.jjtGetChild(0));
        } else {
            return node;
        }
    }

    // Return whether or not the given node is an AND.
    private boolean isConjunction(JexlNode node) {
        return node instanceof ASTAndNode;
    }

    // Return whether or not the given node is an OR.
    private boolean isDisjunction(JexlNode node) {
        return node instanceof ASTOrNode;
    }

    // Return whether or not the two JEXL queries are equivalent.
    private boolean isEquivalent(JexlNode node, ASTJexlScript script) throws ParseException {
        ASTJexlScript nodeScript = getScript(node);
        return TreeEqualityVisitor.isEqual(nodeScript, script);
    }

    // Return the Jexl node as a script.
    private ASTJexlScript getScript(JexlNode node) throws ParseException {
        return JexlASTHelper.parseJexlQuery(JexlStringBuildingVisitor.buildQuery(node));
    }
}
