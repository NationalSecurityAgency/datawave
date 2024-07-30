package datawave.ingest.util;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.keyvalue.UnmodifiableMapEntry;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Iterators;

/**
 * A constructed trie which maps Text (byte arrays) to string values. This does NOT support null values or null keys. This implementation was geared towards
 * minimizing memory without sacrificing read performance. Write performance is dependent on the variability of the characters at any one point in the tree as
 * the children array will be reallocated as the variability is realized. This implementatoion was written with the splits cache in mind and how it is used. For
 * example it is expected that the splits cache reads in the cache and then never actually removes entries back out of the map. Hence remove operations will
 * work but will not prune the tree afterwards. Also the key, value, and entry collections are based on a single entry set iterator and do not optimize any of
 * the other operations;
 */
public final class ShardLocationTrieMap<V> implements Map<Text,V> {
    protected TrieNode<V> root;
    protected int size;

    private static final TrieNode[] EMPTY_CHILDREN = new TrieNode[0];

    public ShardLocationTrieMap() {
        this.root = new TrieNode<>();
    }

    /**
     * The number of entries in the trie
     *
     * @return the size
     */
    @Override
    public int size() {
        return size;
    }

    /**
     * Get the value for a given text object
     *
     * @param text
     *            the key whose associated value is to be returned
     * @return the value or null if not found
     */
    @Override
    public V get(Object text) {
        TrieNode<V> ptr = getNode((Text) text, false);
        return (ptr == null ? null : ptr.value);
    }

    /**
     * Test for an empty map
     *
     * @return true if empty
     */
    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Test for key membership
     *
     * @param key
     *            key whose presence in this map is to be tested
     * @return true if the key is in the map
     */
    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    /**
     * Test for value membership
     *
     * @param value
     *            value whose presence in this map is to be tested
     * @return true if the value if found
     */
    @Override
    public boolean containsValue(Object value) {
        if (value == null) {
            return false;
        }
        return entrySet().stream().anyMatch(e -> value.equals(e.getValue()));
    }

    /**
     * Put a new Text,String entry into the map
     *
     * @param key
     *            key with which the specified value is to be associated
     * @param value
     *            value to be associated with the specified key
     * @return The previous value if any
     */
    @Override
    public V put(Text key, V value) {
        TrieNode<V> ptr = getNode(key, true);
        V previous = ptr.value;
        ptr.value = value;
        size++;
        return previous;
    }

    /**
     * Remove an entry from the map
     *
     * @param key
     *            key whose mapping is to be removed from the map
     * @return the value if found and removed
     */
    @Override
    public V remove(Object key) {
        TrieNode<V> ptr = getNode((Text) key, false);
        if (ptr == null) {
            return null;
        } else if (ptr.value != null) {
            V previous = ptr.value;
            // simply setting the value to null effectively removes this entry
            // much simpler than trying to reallocate children and remove nodes
            ptr.value = null;
            size--;
            return previous;
        } else {
            return null;
        }
    }

    /**
     * Put all of the entries into the map
     *
     * @param m
     *            mappings to be stored in this map
     */
    @Override
    public void putAll(Map<? extends Text,? extends V> m) {
        m.entrySet().stream().forEach(e -> put(e.getKey(), e.getValue()));
    }

    /**
     * clear the map
     */
    @Override
    public void clear() {
        // simply reset the root
        root = new TrieNode<>();
    }

    /**
     * Return a key set
     *
     * @return the key set
     */
    @Override
    public Set<Text> keySet() {
        return new AbstractSet<>() {
            @Override
            public Iterator<Text> iterator() {
                return Iterators.transform(new EntryIterator(), e -> e.getKey());
            }

            @Override
            public int size() {
                return size;
            }
        };
    }

    /**
     * Return a value collection
     *
     * @return the value collection
     */
    @Override
    public Collection<V> values() {
        return new AbstractCollection<>() {
            @Override
            public Iterator<V> iterator() {
                return Iterators.transform(new EntryIterator(), e -> e.getValue());
            }

            @Override
            public int size() {
                return size;
            }
        };
    }

    /**
     * Return an entry set
     *
     * @return the entry set
     */
    @Override
    public Set<Entry<Text,V>> entrySet() {
        return new AbstractSet<Entry<Text,V>>() {
            @Override
            public Iterator<Entry<Text,V>> iterator() {
                return new EntryIterator();
            }

            @Override
            public int size() {
                return size;
            }
        };
    }

    /**
     * A helper method to find the node for a given key, and create it if required.
     *
     * @param key
     *            The Text key
     * @param createIfNeeded
     *            should we create the node if it does not exist?
     * @return The node, or null if not found and not creating.
     */
    private TrieNode<V> getNode(Text key, boolean createIfNeeded) {
        TrieNode<V> ptr = root;
        byte[] data = key.getBytes();
        for (int i = 0; i < key.getLength(); i++) {
            ptr = ptr.getChild(data[i], createIfNeeded);
            if (ptr == null) {
                return null;
            }
        }
        return ptr;
    }

    /**
     * This is the trie node that links to a set of children and potentially contains a value.
     *
     * @param <W>
     *     value type
     */
    public static class TrieNode<W> {
        // the first character represented by children[0]
        int firstChar = 0;
        // the children
        TrieNode<W>[] children = EMPTY_CHILDREN;
        // the value
        W value = null;

        /**
         * convert a child index into its key (a character from the Text key)
         *
         * @param index
         *     the child index in the underlying children array
         * @return the character byte key
         */
        public byte getChildKey(int index) {
            return (byte) (index + firstChar);
        }

        /**
         * Return the value
         *
         * @return the value or null if an intermediate node
         */
        public W getValue() {
            return value;
        }

        /**
         * Get the child for the given key byte, and create if needed
         *
         * @param b
         *            The character byte key (from a Text key)
         * @param createIfNeeded
         *            Should we create the child node if empty?
         * @return the child node, or null if does not exist and node creating
         */
        public TrieNode<W> getChild(byte b, boolean createIfNeeded) {
            // the integer value for the key byte
            int c = (0xff & (int) b);
            // the theoretical index into the children array
            int index = c - firstChar;
            // if not creating
            if (!createIfNeeded) {
                // no children so does not exist
                if (children == null) {
                    return null;
                } else {
                    // if the index is within the children bounds, then return it
                    if (index >= 0 && index < children.length) {
                        return children[index];
                    } else {
                        return null;
                    }
                }
            } else { // else we need to create the node if it does not exist
                // if no children, then we have a simple 1 child array
                if (children == null) {
                    children = new TrieNode[1];
                    children[0] = new TrieNode();
                    firstChar = c;
                    return children[0];
                }
                // if the index is in the bounds of the children array
                else if (index >= 0 && index < children.length) {
                    // create the child node if null
                    if (children[index] == null) {
                        children[index] = new TrieNode();
                    }
                    return children[index];
                }
                // if the new entry is before the existing children range
                else if (index < 0) {
                    // need to reallocate the array and reset the firstChar
                    int difference = 0 - index;
                    TrieNode[] newArray = new TrieNode[children.length + difference];
                    System.arraycopy(children, 0, newArray, difference, children.length);
                    children = newArray;
                    children[0] = new TrieNode();
                    firstChar = c;
                    return children[0];
                }
                // else the new entry is after the existing children range
                else {
                    // need to reallocate the array
                    int difference = index + 1 - children.length;
                    TrieNode[] newArray = new TrieNode[children.length + difference];
                    System.arraycopy(children, 0, newArray, 0, children.length);
                    children = newArray;
                    children[index] = new TrieNode();
                    return children[index];
                }
            }
        }

    }

    /**
     * To facilitate navigation through the tree, we need an object representing a specific child of a node and can be used to find subsequent children.
     *
     * @param <U>
     *     value type
     */
    public static class TrieNodeChildRef<U> {
        // the parent node
        final TrieNode<U> parent;
        // the index of the child (not the character value)
        int childIndex;

        // when creating a new child reference, find the first non-null child
        public TrieNodeChildRef(TrieNode<U> parent) {
            this.parent = parent;
            this.childIndex = -1;
            findNext();
        }

        // true if we are not past the end of the children
        public boolean hasChild() {
            return childIndex < parent.children.length;
        }

        // get the current child node
        public TrieNode<U> getChild() {
            return parent.children[childIndex];
        }

        // get the current child key (as in the character key for the Text key)
        public byte getChildKey() {
            return parent.getChildKey(childIndex);
        }

        // find the next child
        public boolean findNext() {
            while (++childIndex < parent.children.length) {
                if (parent.children[childIndex] != null) {
                    return true;
                }
            }
            return false;
        }

    }

    /**
     * An iterator if map entry objects representing the entries in this trie. This will do a depth first navigation of the tree.
     */
    public class EntryIterator implements Iterator<Map.Entry<Text,V>> {
        // the stack of trie node children references
        Deque<TrieNodeChildRef<V>> queue = new LinkedList<>();
        // The key of the next value
        byte[] key = new byte[16];
        // the length of the key of the next value
        int keyLength = 0;
        // the last node returned by next
        TrieNode last = null;

        // Create the iterator an find the first entry
        public EntryIterator() {
            init();
        }

        // Is the queue empty
        public boolean isEmpty() {
            return queue.isEmpty();
        }

        // Get the key for the current entry
        public Text getKey() {
            return new Text(Arrays.copyOf(key, keyLength));
        }

        // Get the value for the current entry
        public V getValue() {
            return queue.peekLast().parent.value;
        }

        // initialize the path
        private void init() {
            // add the root (Note that creating a child ref will find the first child)
            queue.add(new TrieNodeChildRef<>(root));
            // add the nodes all the way to the first leaf
            gotoLeaf();
            // if the current entry does not have a value
            if (!hasNext()) {
                // then find the next entry with a value
                findNext();
            }
        }

        // add the nodes all the way to the first leaf
        private void gotoLeaf() {
            // while we have a child
            while (queue.peekLast().hasChild()) {
                // push the child onto the path
                pushChild();
            }
        }

        // Find the next entry with a value
        private void findNext() {
            // until we are empty or we found a value
            do {
                // if no more children at the current path end
                if (!queue.isEmpty() && !queue.peekLast().findNext()) {
                    // then remove the end of the path
                    pop();
                    // and find the next child for the new path end
                    if (!queue.isEmpty()) {
                        queue.peekLast().findNext();
                    }
                }
                // if we have a path, then goto the end.
                if (!queue.isEmpty()) {
                    gotoLeaf();
                }
            } while (!isEmpty() && !hasNext());
        }

        // push the child onto the path
        private void pushChild() {
            TrieNodeChildRef<V> parent = queue.peekLast();
            TrieNode<V> childNode = parent.getChild();
            queue.add(new TrieNodeChildRef(childNode));
            // update the key with the new child key value
            if (key.length == keyLength) {
                key = Arrays.copyOf(key, key.length * 2);
            }
            key[keyLength++] = parent.getChildKey();
        }

        // pop off the end of the path
        private void pop() {
            keyLength--;
            queue.removeLast();
        }

        // we have a next if we have a value
        @Override
        public boolean hasNext() {
            return !isEmpty() && (getValue() != null);
        }

        // return the next value, and update the path to find the next one
        @Override
        public Entry<Text,V> next() {
            if (hasNext()) {
                last = queue.peekLast().parent;
                Entry<Text,V> next = new UnmodifiableMapEntry<>(getKey(), getValue());
                findNext();
                return next;
            }
            return null;
        }

        // Remove the last entry returned by next
        @Override
        public void remove() {
            if (last != null) {
                last.value = null;
                last = null;
                size--;
            }
        }
    }

}
