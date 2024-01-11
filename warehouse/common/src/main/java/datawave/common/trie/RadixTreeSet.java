package datawave.common.trie;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.collections4.keyvalue.UnmodifiableMapEntry;

/**
 * A Radix Tree is a compressed TRIE optimized for space.
 */
public class RadixTreeSet extends AbstractSet<String> implements Set<String>, Serializable {

    protected int size;
    protected RadixSetNode root;

    public RadixTreeSet() {
        this.root = new RadixSetNode("");
        this.size = 0;
    }

    public class NodeIterator implements Iterator<Map.Entry<String,RadixSetNode>> {
        protected Stack<Map.Entry<String,RadixSetNode>> position = new Stack<>();
        protected Map.Entry<String,RadixSetNode> next;

        public NodeIterator() {
            position.push(new UnmodifiableMapEntry(root.getKey(), root));
            findNextLeaf(position);
        }

        @Override
        public boolean hasNext() {
            return !position.isEmpty();
        }

        @Override
        public Map.Entry<String,RadixSetNode> next() {
            next = position.peek();
            getNextLeaf(position);
            return next;
        }

        @Override
        public void remove() {
            if (next == null) {
                throw new IllegalStateException("Cannot call remove until after next is called, and then only once");
            }
            next.getValue().setIsLeaf(false);
            next.getValue().setPayload(null);
            size--;
            next = null;
        }
    }

    Iterator<Map.Entry<String,RadixSetNode>> nodeIterator() {
        return new NodeIterator();
    }

    @Override
    public Iterator<String> iterator() {
        final Iterator<Map.Entry<String,RadixSetNode>> iterator = nodeIterator();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public String next() {
                return iterator.next().getKey();
            }

            @Override
            public void remove() {
                iterator.remove();
            }
        };
    }

    private void getNextLeaf(Stack<Map.Entry<String,RadixSetNode>> position) {
        replaceTopWithChildren(position);
        findNextLeaf(position);
    }

    private void findNextLeaf(Stack<Map.Entry<String,RadixSetNode>> position) {
        while (!position.isEmpty() && !position.peek().getValue().isLeaf()) {
            replaceTopWithChildren(position);
        }
    }

    private void replaceTopWithChildren(Stack<Map.Entry<String,RadixSetNode>> position) {
        Map.Entry<String,RadixSetNode> top = position.pop();
        RadixSetNode[] children = top.getValue().getChildren();
        if (children != null) {
            // add the children in reverse order onto the stack
            for (int i = children.length - 1; i >= 0; i--) {
                position.push(new UnmodifiableMapEntry<>(top.getKey() + children[i].getKey(), children[i]));
            }
        }
    }

    @Override
    public void clear() {
        root.clearChildren();
        size = 0;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean contains(Object key) {
        final String sKey = (String) key;
        if (sKey.equals(root.getKey())) {
            if (root.isLeaf()) {
                return true;
            }
        } else {
            return contains(sKey, root);
        }
        return false;
    }

    private boolean contains(String key, RadixSetNode node) {
        boolean contains = false;
        for (RadixSetNode child : node.children()) {
            final int matchingLength = matchingLength(key, child.getKey());
            if (matchingLength == child.getKey().length() && matchingLength == key.length()) {
                if (child.isLeaf()) {
                    contains = true;
                }
            } else if (matchingLength > 0 && matchingLength < key.length()) {
                final String leftoverKey = key.substring(matchingLength);
                contains = contains(leftoverKey, child);
                break;
            }
        }

        return contains;
    }

    public Object getPayload(String key) {
        final String sKey = (String) key;
        if (sKey.equals(root.getKey())) {
            if (root.isLeaf()) {
                return root.getPayload();
            }
        } else {
            return getPayload(sKey, root);
        }
        return false;
    }

    private Object getPayload(String key, RadixSetNode node) {
        Object payload = null;
        for (RadixSetNode child : node.children()) {
            final int matchingLength = matchingLength(key, child.getKey());
            if (matchingLength == child.getKey().length() && matchingLength == key.length()) {
                if (child.isLeaf()) {
                    payload = child.getPayload();
                }
            } else if (matchingLength > 0 && matchingLength < key.length()) {
                final String leftoverKey = key.substring(matchingLength);
                payload = getPayload(leftoverKey, child);
                break;
            }
        }

        return payload;
    }

    @Override
    public boolean add(String value) {
        return put(value, null);
    }

    public static int matchingLength(CharSequence a, CharSequence b) {
        int len = 0;
        for (int i = 0; i < Math.min(a.length(), b.length()); ++i) {
            if (a.charAt(i) != b.charAt(i))
                break;
            ++len;
        }
        return len;
    }

    public boolean put(String value, Object payload) {
        if (put(value, payload, root)) {
            size++;
            return true;
        }
        return false;
    }

    /**
     * Put a new value in the tree, adding or splitting nodes as needed
     *
     * @param value
     *            The new value to add
     * @param node
     *            The root of the tree to search from
     * @return
     */
    private boolean put(String value, Object payload, RadixSetNode node) {
        boolean added = false;

        // if this value equals to our nodes value, then set it as a leaf if not already part of the set
        if (value.equals(node.getKey())) {
            if (!node.isLeaf()) {
                added = true;
                node.setIsLeaf();
                node.setPayload(payload);
            }
        } else {
            // Get the matching prefix length
            final int matchingLength = matchingLength(value, node.getKey());

            // if we are not completely matching our value but completely matched this node
            if (matchingLength < value.length() && matchingLength == node.getKey().length()) {
                // get the remaining value
                final String valueSuffix = value.substring(matchingLength);

                // find the child to string the suffix off of
                boolean found = false;
                for (RadixSetNode child : node.children()) {
                    if (child.getKey().charAt(0) == valueSuffix.charAt(0)) {
                        found = true;
                        added = put(valueSuffix, payload, child);
                        break;
                    }
                }

                // if no child found, then add a new child
                if (!found) {
                    final RadixSetNode n = new RadixSetNode(valueSuffix);
                    n.setIsLeaf();
                    n.setPayload(payload);
                    added = true;
                    node.addChild(n);
                }
            } else { // partially matched this node (may or may not have matched value completely)
                // create a new node that contains the suffix of this node
                final String nodeSuffix = node.getKey().substring(matchingLength);
                final RadixSetNode n = new RadixSetNode(nodeSuffix);
                n.setChildren(node.getChildren());
                n.setIsLeaf(node.isLeaf());
                n.setPayload(node.getPayload());

                // replace this node with the matching portion and add the suffix as a child
                node.setKey(node.getKey().substring(0, matchingLength));
                node.setIsLeaf(false);
                node.setPayload(null);
                node.clearChildren();
                node.addChild(n);

                // if this node completely matches our value then set as a leaf
                if (matchingLength == value.length()) {
                    node.setIsLeaf();
                    node.setPayload(payload);
                    added = true;
                } else {
                    // otherwise create a new child with the remainder of the value
                    final String valueSuffix = value.substring(matchingLength);
                    final RadixSetNode keyNode = new RadixSetNode(valueSuffix);
                    keyNode.setIsLeaf();
                    keyNode.setPayload(payload);
                    added = true;
                    node.addChild(keyNode);
                }
            }
        }

        return added;
    }

    @Override
    public boolean remove(Object key) {
        final String sKey = (String) key;
        if (sKey.equals(root.getKey())) {
            if (root.isLeaf()) {
                root.setIsLeaf(false);
                root.setPayload(null);
                size--;
                return true;
            }
        } else if (remove(sKey, root)) {
            size--;
            return true;
        }
        return false;
    }

    private boolean remove(String key, RadixSetNode node) {
        boolean removed = false;
        for (RadixSetNode child : node.children()) {
            final int matchingLength = matchingLength(key, child.getKey());
            if (matchingLength == child.getKey().length() && matchingLength == key.length()) {
                if (child.isLeaf()) {
                    removed = true;
                    child.setIsLeaf(false);
                    child.setPayload(null);
                }
            } else if (matchingLength > 0 && matchingLength < key.length()) {
                final String leftoverKey = key.substring(matchingLength);
                removed = remove(leftoverKey, child);
                break;
            }
        }

        return removed;
    }
}
