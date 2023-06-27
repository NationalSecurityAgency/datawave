package org.apache.commons.jexl3.parser;

import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.math.BigInteger;
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
        int numChildren = node.jjtGetNumChildren();
        if (numChildren < capacity) {
            JexlNode[] newChildren = new JexlNode[capacity];
            for (int i = 0; i < numChildren; i++) {
                newChildren[i] = node.jjtGetChild(i);
            }
            node.jjtSetChildren(newChildren);
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
            Constructor constructor = node.getClass().getDeclaredConstructor(Integer.TYPE);
            constructor.setAccessible(true);
            return (T) constructor.newInstance(node.id);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
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
    public static <T extends JexlNode> T setChildren(T node, JexlNode... children) {
        node.jjtSetChildren(children);
        for (JexlNode child : children)
            newParent(child, node);
        return node;
    }

    public static ASTReferenceExpression makeRefExp() {
        return new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);
    }

    public static ASTFunctionNode makeFunction(String namespace, String name, JexlNode... arguments) {
        ASTNamespaceIdentifier namespaceNode = new ASTNamespaceIdentifier(ParserTreeConstants.JJTNAMESPACEIDENTIFIER);
        namespaceNode.setNamespace(namespace, name);

        ASTArguments argumentNode = new ASTArguments(ParserTreeConstants.JJTARGUMENTS);
        argumentNode.jjtSetChildren(arguments);

        ASTFunctionNode functionNode = new ASTFunctionNode(ParserTreeConstants.JJTFUNCTIONNODE);
        functionNode.jjtSetChildren(new JexlNode[] {namespaceNode, argumentNode});

        return functionNode;
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
        return setChildren(ref, node);
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
        for (int i = 0; i < parent.jjtGetNumChildren(); ++i) {
            if (parent.jjtGetChild(i) == a) {
                parent.jjtAddChild(b, i);
                b.jjtSetParent(parent);
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
        for (int i = 0; i < parent.jjtGetNumChildren(); ++i) {
            if (parent.jjtGetChild(i) == a) {
                parent.jjtAddChild(b, i);
                b.jjtSetParent(parent);
                a.jjtSetParent(null);
            }
        }
        return parent;
    }

    public static JexlNode promote(JexlNode parent, JexlNode child) {
        JexlNode grandpa = parent.jjtGetParent();
        if (grandpa == null) {
            child.jjtSetParent(null);
            return child;
        } else {
            return swap(parent.jjtGetParent(), parent, child);
        }
    }

    public static boolean setLiteral(ASTNumberLiteral literal, Number value) {
        boolean success = false;

        Preconditions.checkNotNull(literal);
        Preconditions.checkNotNull(value);

        if (isNaturalNumber(value.getClass())) {
            literal.setNatural(value.toString());
            success = true;
        } else if (isRealNumber(value.getClass())) {
            literal.setReal(value.toString());
            success = true;
        }

        return success;
    }

    protected static boolean isNaturalNumber(Class<? extends Number> clazz) {
        return clazz == Integer.class || clazz == Long.class || clazz == BigInteger.class;
    }

    protected static boolean isRealNumber(Class<? extends Number> clazz) {
        return clazz == Float.class || clazz == Double.class || clazz == BigDecimal.class;
    }

    public static void setLiteral(ASTStringLiteral literal, String value) {
        Preconditions.checkNotNull(literal);
        Preconditions.checkNotNull(value);

        literal.setLiteral(value);
    }

    public static void setLiteral(ASTRegexLiteral literal, String value) {
        Preconditions.checkNotNull(literal);
        Preconditions.checkNotNull(value);

        literal.setLiteral(value);
    }

    public static void setLiteral(ASTJxltLiteral literal, String value) {
        Preconditions.checkNotNull(literal);
        Preconditions.checkNotNull(value);

        literal.setLiteral(value);
    }

    public static void setAnnotation(ASTAnnotation annotation, String name) {
        Preconditions.checkNotNull(annotation);
        Preconditions.checkNotNull(name);

        annotation.setName(name);
    }

    public static void setQualifiedIdentifier(ASTQualifiedIdentifier identifier, String value) {
        Preconditions.checkNotNull(identifier);
        Preconditions.checkNotNull(value);

        identifier.setName(value);
    }

    public static void setIdentifier(ASTIdentifier identifier, String name) {
        Preconditions.checkNotNull(identifier);
        Preconditions.checkNotNull(name);

        identifier.setSymbol(name);
    }

    public static void setIdentifierAccess(ASTIdentifierAccess identifierAccess, String value) {
        Preconditions.checkNotNull(identifierAccess);
        Preconditions.checkNotNull(value);

        identifierAccess.setIdentifier(value);
    }

    public static Object getImage(JexlNode node) {
        Object value = null;
        if (node instanceof ASTIdentifier) {
            value = ((ASTIdentifier) node).getName();
        } else if (node instanceof ASTStringLiteral) {
            value = ((ASTStringLiteral) node).getLiteral();
        } else if (node instanceof ASTNumberLiteral) {
            value = ((ASTNumberLiteral) node).getLiteral();
        }
        return value;
    }

    public static boolean setImage(JexlNode node, Object value) {
        boolean success = false;
        if (node instanceof ASTIdentifier && value instanceof String) {
            JexlNodes.setIdentifier((ASTIdentifier) node, (String) value);
        } else if (node instanceof ASTStringLiteral && value instanceof String) {
            JexlNodes.setLiteral((ASTStringLiteral) node, (String) value);
        } else if (node instanceof ASTNumberLiteral && value instanceof Number) {
            JexlNodes.setLiteral((ASTNumberLiteral) node, (Number) value);
        }
        return success;
    }

    public static boolean copyImage(JexlNode original, JexlNode copy) {
        return setImage(copy, getImage(original));
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
            return setChildren(new ASTNotNode(ParserTreeConstants.JJTNOTNODE), node);
        }
        return setChildren(new ASTNotNode(ParserTreeConstants.JJTNOTNODE), wrap(node));
    }

    public static ASTIdentifier makeIdentifier() {
        return new ASTIdentifier(ParserTreeConstants.JJTIDENTIFIER);
    }

    public static ASTNumberLiteral makeNumberLiteral() {
        return new ASTNumberLiteral(ParserTreeConstants.JJTNUMBERLITERAL);
    }

    public static ASTStringLiteral makeStringLiteral() {
        return new ASTStringLiteral(ParserTreeConstants.JJTSTRINGLITERAL);
    }

    public static ASTIdentifier makeIdentifierWithImage(String identifier) {
        ASTIdentifier id = makeIdentifier();
        id.setSymbol(identifier);
        return id;
    }

    public static JexlNode otherChild(JexlNode parent, JexlNode child) {
        Preconditions.checkArgument(parent.jjtGetNumChildren() == 2, "Jexl tree must be binary, but received node with %s children.",
                        parent.jjtGetNumChildren());
        JexlNode otherChild = null;
        for (int i = 0; i < parent.jjtGetNumChildren(); i++) {
            JexlNode n = parent.jjtGetChild(i);
            if (child != n) {
                otherChild = n;
            }
        }
        return Preconditions.checkNotNull(otherChild);
    }

    public static ASTStringLiteral literal(String s) {
        ASTStringLiteral l = new ASTStringLiteral(ParserTreeConstants.JJTSTRINGLITERAL);
        setLiteral(l, s);
        return l;
    }

    public static ASTNumberLiteral literal(Number n) {
        ASTNumberLiteral l = new ASTNumberLiteral(ParserTreeConstants.JJTNUMBERLITERAL);
        setLiteral(l, n);
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
            JexlNodes.setChildren(parent, children.toArray(nodeArray));
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
