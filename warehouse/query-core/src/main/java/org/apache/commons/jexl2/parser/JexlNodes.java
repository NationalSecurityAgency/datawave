package org.apache.commons.jexl2.parser;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import datawave.query.jexl.nodes.QueryPropertyMarker;

/**
 * A utility class that can introspect JexlNodes for useful things like raw access to the children array and type ID. This makes cloning and mutation easier.
 */
public class JexlNodes {

    private JexlNodes() {
        // this is a static utility
    }

    /**
     * Ensures that the child array as at least {i} capacity.
     *
     * @param <T>
     *            type of node
     * @param node
     *            a node
     * @param capacity
     *            the capacity
     * @return a node
     */
    public static <T extends JexlNode> T ensureCapacity(T node, final int capacity) {
        JexlNode[] children = node.children;
        if (children == null) {
            node.children = new JexlNode[capacity];
        } else if (children.length < capacity) {
            node.children = new JexlNode[capacity];
            System.arraycopy(children, 0, node.children, 0, children.length);
        }
        return node;
    }

    /**
     * Returns the internal {id} of the supplied node.
     *
     * Refer to ParserTreeConstants for a mapping of id number to a label.
     *
     * @param n
     *            the jexl node
     * @return the internal {id} of the supplied node.
     */
    public static int id(JexlNode n) {
        return n.id;
    }

    /**
     * Returns a new instance of type of node supplied to this method.
     *
     * @param node
     *            the jexl node
     * @param <T>
     *            type of node
     * @return new instance of type of node supplied to this method.
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T newInstanceOfType(T node) {
        try {
            @SuppressWarnings("rawtypes")
            Constructor constructor = node.getClass().getConstructor(Integer.TYPE);
            return (T) constructor.newInstance(node.id);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Returns an array representation of a nodes children. If a node has no children, an empty array is returned.
     *
     * @param node
     *            the jexl node
     * @return array representation of a nodes children
     */
    public static JexlNode[] children(JexlNode node) {
        return node.children == null ? new JexlNode[0] : node.children;
    }

    /**
     * Sets the supplied child array as the children member of {node} and sets the parent reference of each element in {children} to {node}.
     *
     * @param node
     *            the jexl node
     * @param <T>
     *            type of node
     * @param children
     *            the children nodes
     * @return the provided node
     */
    public static <T extends JexlNode> T children(T node, JexlNode... children) {
        node.children = children;
        for (JexlNode child : node.children)
            newParent(child, node);
        return node;
    }

    /**
     * Wraps any node in a reference node. This is useful for getting rid of the boilerplate associated with wrapping an {ASTStringLiteral}.
     *
     * @param node
     *            the jexl node
     * @return the node with children nodes assigned
     */
    public static ASTReference makeRef(JexlNode node) {
        ASTReference ref = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        return children(ref, node);
    }

    /**
     * Wraps some node in a ReferenceExpression, so when rebuilding, the subtree will be surrounded by parens
     *
     * @param node
     *            the jexl node
     * @return the node wrapped in a reference expression
     */
    public static ASTReferenceExpression wrap(JexlNode node) {
        ASTReferenceExpression ref = new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);
        return children(ref, node);
    }

    public static boolean isWrapped(JexlNode node) {
        return id(node.jjtGetParent()) == ParserTreeConstants.JJTREFERENCEEXPRESSION;
    }

    /**
     * Fluid wrapper for calling {child.jjtSetParent(parent)}.
     *
     * @param child
     *            the child
     * @param parent
     *            the parent node
     * @param <T>
     *            type of node
     * @return the child data
     */
    public static <T extends JexlNode> T newParent(T child, JexlNode parent) {
        child.jjtSetParent(parent);
        return child;
    }

    /**
     * Swaps {childA} with {childB} in {parent}'s list of children, but does not reset {childA}'s parent.
     *
     * @param <T>
     *            type of the parent
     * @param a
     *            a node
     * @param b
     *            b node
     * @param parent
     *            the parent
     * @return the parent
     */
    public static <T extends JexlNode> T replaceChild(T parent, JexlNode a, JexlNode b) {
        for (int i = 0; i < parent.children.length; ++i) {
            if (parent.children[i] == a) {
                parent.children[i] = b;
                b.parent = parent;
            }
        }
        return parent;
    }

    /**
     * Swaps {childA} with {childB} in {parent}'s list of children and resets {childA}'s parent to null.
     *
     * @param <T>
     *            type of the parent
     * @param a
     *            a node
     * @param b
     *            b node
     * @param parent
     *            the parent
     * @return the parent
     */
    public static <T extends JexlNode> T swap(T parent, JexlNode a, JexlNode b) {
        for (int i = 0; i < parent.children.length; ++i) {
            if (parent.children[i] == a) {
                parent.children[i] = b;
                b.parent = parent;
                a.parent = null;
            }
        }
        return parent;
    }

    public static JexlNode promote(JexlNode parent, JexlNode child) {
        JexlNode grandpa = parent.jjtGetParent();
        if (grandpa == null) {
            child.parent = null;
            return child;
        } else {
            return swap(parent.jjtGetParent(), parent, child);
        }
    }

    public static void setLiteral(ASTNumberLiteral literal, Number value) {
        Preconditions.checkNotNull(literal);
        Preconditions.checkNotNull(value);

        literal.literal = value;
    }

    public static void setLiteral(ASTStringLiteral literal, String value) {
        Preconditions.checkNotNull(literal);
        Preconditions.checkNotNull(value);

        literal.image = value;
    }

    /**
     * Negate the provided JexlNode
     *
     * @param node
     *            an arbitrary JexlNode
     * @return a negated version of the provided JexlNode
     */
    public static ASTNotNode negate(JexlNode node) {
        if (QueryPropertyMarker.findInstance(node).isAnyType()) {
            // marked node trees begin with ref-refExpr, no need to wrap again
            return children(new ASTNotNode(ParserTreeConstants.JJTNOTNODE), node);
        }
        return children(new ASTNotNode(ParserTreeConstants.JJTNOTNODE), makeRef(wrap(node)));
    }

    public static ASTIdentifier makeIdentifierWithImage(String image) {
        ASTIdentifier id = new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
        id.image = image;
        return id;
    }

    public static JexlNode otherChild(JexlNode parent, JexlNode child) {
        Preconditions.checkArgument(parent.jjtGetNumChildren() == 2, "Jexl tree must be binary, but received node with %s children.",
                        parent.jjtGetNumChildren());
        JexlNode otherChild = null;
        for (JexlNode n : children(parent))
            if (child != n)
                otherChild = n;
        return Preconditions.checkNotNull(otherChild);
    }

    public static ASTReference literal(String s) {
        ASTStringLiteral l = new ASTStringLiteral(ParserTreeConstants.JJTSTRINGLITERAL);
        l.image = s;
        return makeRef(l);
    }

    public static ASTNumberLiteral literal(Number n) {
        ASTNumberLiteral l = new ASTNumberLiteral(ParserTreeConstants.JJTNUMBERLITERAL);
        l.literal = n;
        return l;
    }

    /**
     * Remove childToRemove from parent, updating all references on both the parent and childToRemove to be consistent
     *
     * @param parent
     *            the parent to remove from
     * @param childToRemove
     *            the child to remove from parent
     * @return true if childToRemove was successfully found and removed from parent, false otherwise
     */
    public static boolean removeFromParent(JexlNode parent, JexlNode childToRemove) {
        // sanity check
        if (childToRemove == null || parent == null) {
            return false;
        }

        boolean found = false;
        // at most as many children as currently exist
        List<JexlNode> children = new ArrayList<>(parent.jjtGetNumChildren());
        JexlNode[] nodeArray = new JexlNode[0];

        for (int i = 0; i < parent.jjtGetNumChildren(); i++) {
            JexlNode child = parent.jjtGetChild(i);
            if (child != childToRemove) {
                children.add(child);
            } else {
                // clear the parent of the node that is removed
                childToRemove.jjtSetParent(null);
                found = true;
            }
        }

        // update the children references if they changed
        if (found) {
            // reset the children on the parent node to remove this one
            JexlNodes.children(parent, children.toArray(nodeArray));
        }

        return found;
    }

    /**
     * Return whether or not the node has at least one child.
     *
     * @param node
     *            the node
     * @return true if the node is not null and has at least one child, or false otherwise.
     */
    public static boolean isNotChildless(JexlNode node) {
        return node != null && node.jjtGetNumChildren() > 0;
    }

    /**
     * Ascends the entire Jexl tree searching for a negation. In the case of an unflattened tree this may be an expensive operation
     *
     * @param node
     *            an arbitrary JexlNode
     * @return true if an ASTNotNode exists in this node's ancestry
     */
    public static boolean findNegatedParent(JexlNode node) {
        while (node.jjtGetParent() != null) {
            node = node.jjtGetParent();
            if (node instanceof ASTNotNode)
                return true;
        }
        return false;
    }
}
