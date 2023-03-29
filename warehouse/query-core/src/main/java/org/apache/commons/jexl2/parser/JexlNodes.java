package org.apache.commons.jexl2.parser;

import com.google.common.base.Preconditions;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A utility class that can introspect JexlNodes for useful things like raw access to the children array and type ID. This makes cloning and mutation easier.
 */
public class JexlNodes {
    
    /**
     * Ensures that the child array as at least {i} capacity.
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
     * @return
     */
    public static int id(JexlNode n) {
        return n.id;
    }
    
    /**
     * Returns a new instance of type of node supplied to this method.
     *
     * @param node
     * @return
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
     * @return
     */
    public static JexlNode[] children(JexlNode node) {
        return node.children == null ? new JexlNode[0] : node.children;
    }
    
    /**
     * Sets the supplied child array as the children member of {node} and sets the parent reference of each element in {children} to {node}.
     *
     * @param node
     * @param children
     * @return
     */
    public static <T extends JexlNode> T children(T node, JexlNode... children) {
        node.children = children;
        for (JexlNode child : node.children)
            newParent(child, node);
        return node;
    }
    
    /**
     * Creates a child array from {children} which is then set as the children of the {node}.
     *
     * @param node
     * @param children
     * @return
     */
    public static <T extends JexlNode> T children(T node, Collection<JexlNode> children) {
        return children(node, children.toArray(new JexlNode[0]));
    }
    
    /**
     * Wraps any node in a reference node. This is useful for getting rid of the boilerplate associated with wrapping an {ASTStringLiteral}.
     *
     * @param node
     * @return
     */
    public static ASTReference makeRef(JexlNode node) {
        ASTReference ref = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        return children(ref, node);
    }
    
    /**
     * Wraps some node in a ReferenceExpression, so when rebuilding, the subtree will be surrounded by parens
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
     * @param parent
     * @return
     */
    public static <T extends JexlNode> T newParent(T child, JexlNode parent) {
        child.jjtSetParent(parent);
        return child;
    }
    
    /**
     * Swaps {childA} with {childB} in {parent}'s list of children, but does not reset {childA}'s parent.
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
    
    /**
     * Sets the -
     *
     * @param literal
     * @param value
     */
    public static <T> void setLiteral(ASTNumberLiteral literal, Number value) {
        Preconditions.checkNotNull(literal);
        Preconditions.checkNotNull(value);
        
        literal.literal = value;
    }
    
    public static <T> void setLiteral(ASTStringLiteral literal, String value) {
        Preconditions.checkNotNull(literal);
        Preconditions.checkNotNull(value);
        
        literal.image = value;
    }
    
    public static ASTNotNode negate(JexlNode node) {
        return children(new ASTNotNode(ParserTreeConstants.JJTNOTNODE), wrap(node));
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
