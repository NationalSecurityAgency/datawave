package datawave.data.normalizer.regex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import datawave.data.normalizer.regex.visitor.Visitor;

public abstract class Node {
    
    protected Node parent;
    protected Map<String,String> properties;
    protected ArrayList<Node> children = new ArrayList<>();
    
    protected Node() {}
    
    @SuppressWarnings("CopyConstructorMissesField")
    protected Node(Node child) {
        addChild(child);
    }
    
    protected Node(Map<String,String> properties) {
        if (properties != null) {
            this.properties = new HashMap<>();
            this.properties.putAll(properties);
        }
    }
    
    protected Node(Collection<? extends Node> children) {
        addChildren(children);
    }
    
    /**
     * Return the node type.
     * 
     * @return the type
     */
    public abstract NodeType getType();
    
    /**
     * Return the parent of this {@link Node}. Possibly null if a parent was never set.
     * 
     * @return the parent
     */
    public Node getParent() {
        return parent;
    }
    
    /**
     * Set the parent for this node.
     * 
     * @param parent
     *            the parent
     */
    public void setParent(Node parent) {
        this.parent = parent;
    }
    
    public boolean hasProperties() {
        return properties != null;
    }
    
    public boolean hasProperty(String key) {
        return hasProperties() && properties.containsKey(key);
    }
    
    public String getProperty(String key) {
        return properties.get(key);
    }
    
    public void setProperty(String key, String value) {
        if (properties == null) {
            properties = new HashMap<>();
        }
        properties.put(key, value);
    }
    
    public void setProperties(Map<String,String> properties) {
        if (properties != null) {
            if (this.properties == null) {
                this.properties = new HashMap<>();
            }
            this.properties.putAll(properties);
        }
    }
    
    /**
     * Return the children of this {@link Node}. Possibly empty, but never null.
     * 
     * @return the children
     */
    public List<Node> getChildren() {
        return children;
    }
    
    /**
     * Set the children for this {@link Node}. If the given list is null, the list of children for this node will be cleared.
     * 
     * @param children
     *            the children
     */
    public void setChildren(Collection<Node> children) {
        this.children.clear();
        if (children != null) {
            children.forEach(this::addChild);
        }
    }
    
    /**
     * Add a child to the end of the list of children for this node.
     * 
     * @param child
     *            the child to add
     */
    public void addChild(Node child) {
        this.children.add(child);
        child.parent = this;
    }
    
    /**
     * Add a child to this node at the specified index. Shifts the child at the specified index and any subsequent children to the right by one index.
     * 
     * @param child
     *            the child to insert
     * @param index
     *            the index at which the child is to be inserted
     */
    public void addChild(Node child, int index) {
        this.children.add(index, child);
        child.parent = this;
    }
    
    /**
     * Add each node in the given list to the end of the list of children for this node.
     * 
     * @param children
     *            the children to add
     */
    public void addChildren(Collection<? extends Node> children) {
        children.forEach(this::addChild);
    }
    
    /**
     * Return the child at the specified index in this node's list of children.
     * 
     * @param index
     *            the index
     * @return the child
     */
    public Node getChildAt(int index) {
        return children.get(index);
    }
    
    /**
     * Return the number of children this node has.
     * 
     * @return the total number of children
     */
    public int getChildCount() {
        return children.size();
    }
    
    /**
     * Return whether this node has any children.
     * 
     * @return true if this node has at least one child, or false otherwise
     */
    public boolean hasChildren() {
        return !children.isEmpty();
    }
    
    /**
     * Returns whether this node is a leaf, that is, whether it has no children.
     * 
     * @return true if this node has no children, or false otherwise
     */
    public boolean isLeaf() {
        return children.size() == 0;
    }
    
    /**
     * Accepts the given visitor and passes itself to the appropriate method in the {@link Visitor} with the given data.
     * 
     * @param visitor
     *            the visitor
     * @param data
     *            the data
     * @return the result from the visitor
     */
    public abstract Object accept(Visitor visitor, Object data);
    
    /**
     * Passes the visitor to each child in this node for the child to accept.
     * 
     * @param visitor
     *            the visitor
     * @param data
     *            the data
     * @return the data
     */
    public Object childrenAccept(Visitor visitor, Object data) {
        children.forEach((child) -> child.accept(visitor, data));
        return data;
    }
    
    /**
     * Return a shallow copy of the node of the same type with all relevant attributes except for the parent and children.
     * 
     * @return the shallow copy
     */
    public abstract Node shallowCopy();
    
    /**
     * Return whether any child of this node an instance of a type not found in the given types.
     * 
     * @param types
     *            the types
     * @return true if any child of this node is a type not found in the given types, or false otherwise
     */
    public boolean isAnyChildNotOf(Set<Class<? extends Node>> types) {
        return children.stream().map(Node::getClass).anyMatch((t) -> !types.contains(t));
    }
    
    /**
     * Return whether any child of this node is an instance of the given type.
     * 
     * @param type
     *            the type
     * @return true if any child of this node is an instance of the given type, or false otherwise
     */
    public boolean isAnyChildOf(Class<? extends Node> type) {
        return children.stream().anyMatch(type::isInstance);
    }
    
    /**
     * Returns the index within this node of the first child of the specified type. If no child of the specified type exists in this node, -1 is returned.
     * 
     * @param type
     *            the type
     * @return the index of the first child of the specified type, or -1 if no child of the type is found
     */
    public int indexOf(Class<? extends Node> type) {
        return indexOf(type, 0);
    }
    
    /**
     * Returns the index within this node of the first child of the specified type, starting the search at the specified index. If no child of the specified
     * type exists at or after position {@code fromIndex}, -1 is returned.
     * 
     * @param type
     *            the type
     * @param fromIndex
     *            the index to start the search from
     * @return the index of the first child of the specified type that is greater than or equal to {@code fromIndex}, or -1 if no child of the type is found
     */
    public int indexOf(Class<? extends Node> type, int fromIndex) {
        for (int i = fromIndex; i < children.size(); i++) {
            if (type.isInstance(children.get(i))) {
                return i;
            }
        }
        return -1;
    }
    
    /**
     * Return the first child of this node, or null if this node has no children.
     * 
     * @return the first node, possibly null
     */
    public Node getFirstChild() {
        return children.isEmpty() ? null : children.get(0);
    }
    
    /**
     * Return the last child of this node, or null if this node has no children.
     * 
     * @return the last node, possibly null
     */
    public Node getLastChild() {
        return children.isEmpty() ? null : children.get((children.size() - 1));
    }
    
    /**
     * Removes the first child from this node.
     * 
     * @throws IndexOutOfBoundsException
     *             if there are no children
     */
    public void removeFirstChild() {
        children.remove(0);
    }
    
    /**
     * Return a new {@link NodeListIterator} instance that will traverse over this node's children.
     * 
     * @return a new iterator
     */
    public NodeListIterator getChildrenIterator() {
        return new NodeListIterator(this.children);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Node node = (Node) o;
        return Objects.equals(properties, node.properties);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(properties);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        if (properties != null) {
            sb.append("(").append(properties).append(")");
        }
        return sb.toString();
    }
    
}
