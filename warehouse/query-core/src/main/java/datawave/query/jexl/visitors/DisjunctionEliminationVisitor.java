package datawave.query.jexl.visitors;

import static org.apache.commons.jexl2.parser.ParserTreeConstants.JJTANDNODE;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;

import datawave.query.jexl.JexlASTHelper;

/**
 * Visitor that removes disjunction children of AND nodes that are made redundant by virtue of distributed equivalency. For example:
 *
 * <pre>
 * {@code (A || B) && A --> A}
 * {@code ((A || B) || C) && (A || B) --> (A || B)}
 * {@code ((A && B) || D) && (A && B && C) --> (A && B && C)}
 * {@code ((A || B) && A) || (C || D) --> (A) || (C || D)}
 * </pre>
 *
 * The following cases will not be affected:
 *
 * <pre>
 * {@code (A || B) && C}
 * {@code ((A && C) || B) && A}
 * </pre>
 *
 * This visitor returns a copy of the original query tree, and flattens the copy via {@link TreeFlatteningRebuildingVisitor}.
 * <p>
 * Node traversal is post-order.
 */
public class DisjunctionEliminationVisitor extends RebuildingVisitor {

    private static final Logger log = Logger.getLogger(DisjunctionEliminationVisitor.class);

    /**
     * Given a JexlNode, determine if any redundant disjunctions in the node can be removed. The query will be flattened before applying this visitor.
     *
     * @param node
     *            a query node
     * @param <T>
     *            type of the node
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
        DisjunctionEliminationVisitor visitor = new DisjunctionEliminationVisitor();
        return (T) copy.jjtAccept(visitor, null);

    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        return removeRedundantDisjunctions(node);
    }

    // Return a node with redundant disjunctions removed.
    private JexlNode removeRedundantDisjunctions(JexlNode node) {
        try {
            List<JexlNode> children = getNonRedundantChildren(node);
            if (children.size() == 1) {
                // If only one child remains, return it.
                return children.get(0);
            } else if (children.size() < node.jjtGetNumChildren()) {
                // If there were some redundant children, but more than one relevant child, return a new AND node with the relevant children.
                JexlNode copy = new ASTAndNode(JJTANDNODE);
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
            // If the current child is a disjunction, check if any other child makes it redundant.
            // If not, keep it as a child.
            if (isDisjunction(getFirstNonASTReference(child))) {
                if (!isRedundant(node, i)) {
                    children.add(child);
                }
            } else {
                // Automatically keep all non-disjunction children.
                children.add(child);
            }
        }
        return children;
    }

    // Return whether or not if the specified disjunction node is redundant and can be removed.
    private boolean isRedundant(JexlNode node, int disjunctionIndex) throws ParseException {
        List<JexlNode> children = getNonASTReferenceChildren(node);
        JexlNode disjunction = children.remove(disjunctionIndex);
        List<JexlNode> disjunctionChildren = getNonASTReferenceChildren(disjunction);
        // Check for an equivalent match to any of the disjunction's children.
        for (JexlNode child : disjunctionChildren) {
            if (isDisjunction(child) && containsEquivalentDisjunction(child, children)) {
                return true;
            } else if (isConjunction(child) && isSubsetOf(child, children)) {
                return true;
            } else if (containsEquivalent(child, children)) {
                return true;
            }
        }

        // Handle the case where there may be a match between a disjunction and a subset of that disjunction. This cannot be handled in the previous loop since
        // it requires an iteration over the original list of children.
        for (JexlNode child : children) {
            if (isDisjunction(child) && containsEquivalentDisjunction(child, disjunctionChildren)) {
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
        // an equivalent disjunction. This handles cases similar to (A || B || C) && (A || B), which should reduce to (A || B).
        return uniqueChildren.isEmpty();
    }

    // Return whether or not the provided conjunction has a match for each of it's children in the provided list of children. This will also return true if the
    // provided conjunction is a subset of the list. This handles the case ((A && B) || D) && (A && B && C), which should reduce to (A && B && C).
    private boolean isSubsetOf(JexlNode conjunction, List<JexlNode> nodes) throws ParseException {
        List<JexlNode> children = getNonASTReferenceChildren(conjunction);
        for (JexlNode child : children) {
            if (!containsEquivalent(child, nodes)) {
                return false;
            }
        }
        return true;
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

    // Return the JEXL node as a script.
    private ASTJexlScript getScript(JexlNode node) throws ParseException {
        return JexlASTHelper.parseJexlQuery(JexlStringBuildingVisitor.buildQuery(node));
    }
}
