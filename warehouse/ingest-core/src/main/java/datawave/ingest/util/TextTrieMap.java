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
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

/**
 * A constructed trie which maps Text (byte arrays) to string values. This does NOT support null values or null keys.
 */
public final class TextTrieMap<V> implements Map<Text,V> {
    private static final Logger log = Logger.getLogger(TextTrieMap.class);

    protected TrieNode<V> root;
    protected int size;

    public TextTrieMap() {
        this.root = createNode();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public V get(Object text) {
        TrieNode<V> ptr = getNode((Text) text, false);
        return (ptr == null ? null : ptr.value);
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        if (value == null) {
            return false;
        }
        return entrySet().stream().anyMatch(e -> value.equals(e.getValue()));
    }

    @Override
    public V put(Text key, V value) {
        TrieNode<V> ptr = getNode(key, true);
        V previous = ptr.value;
        ptr.value = value;
        size++;
        return previous;
    }

    @Override
    public V remove(Object key) {
        TrieNode<V> ptr = getNode((Text) key, false);
        if (ptr == null) {
            return null;
        } else if (ptr.value != null) {
            V previous = ptr.value;
            ptr.value = null;
            size--;
            return previous;
        } else {
            return null;
        }
    }

    @Override
    public void putAll(Map<? extends Text,? extends V> m) {
        m.entrySet().stream().forEach(e -> put(e.getKey(), e.getValue()));
    }

    @Override
    public void clear() {
        root = createNode();
    }

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

    public static class TrieNode<W> {
        final TrieNode<W>[] children;
        W value;

        public TrieNode() {
            // TODO: can we reduce this array size?
            this.children = new TrieNode[256];
        }
    }

    public static class TrieNodeChildRef<U> {
        final TrieNode<U> parent;
        int child;

        public TrieNodeChildRef(TrieNode<U> parent) {
            this.parent = parent;
            this.child = -1;
            findNext();
        }

        public boolean hasChild() {
            return child < parent.children.length;
        }

        public TrieNode<U> getChild() {
            return parent.children[child];
        }

        public int getChildIndex() {
            return child;
        }

        public boolean findNext() {
            while (++child < parent.children.length) {
                if (parent.children[child] != null) {
                    return true;
                }
            }
            return false;
        }

    }

    public class EntryIterator implements Iterator<Map.Entry<Text,V>> {
        Deque<TrieNodeChildRef<V>> queue = new LinkedList<>();
        byte[] key = new byte[16];
        int keyLength = 0;

        public EntryIterator() {
            init();
        }

        public boolean isEmpty() {
            return queue.isEmpty();
        }

        public Text getKey() {
            return new Text(Arrays.copyOf(key, keyLength));
        }

        public V getValue() {
            return queue.peekLast().parent.value;
        }

        private void init() {
            queue.add(new TrieNodeChildRef<>(root));
            gotoLeaf();
            if (!hasNext()) {
                findNext();
            }
        }

        private void gotoLeaf() {
            while (queue.peekLast().hasChild()) {
                pushChild();
            }
        }

        private void findNext() {
            do {
                if (!queue.isEmpty() && !queue.peekLast().findNext()) {
                    pop();
                    if (!queue.isEmpty()) {
                        queue.peekLast().findNext();
                    }
                }
                if (!queue.isEmpty()) {
                    gotoLeaf();
                }
            } while (!isEmpty() && !hasNext());
        }

        private void pushChild() {
            TrieNodeChildRef<V> parent = queue.peekLast();
            int character = parent.getChildIndex();
            TrieNode<V> childNode = parent.getChild();
            queue.add(new TrieNodeChildRef(childNode));
            if (key.length == keyLength) {
                key = Arrays.copyOf(key, key.length * 2);
            }
            key[keyLength++] = (byte) (character);
        }

        private void pop() {
            keyLength--;
            queue.removeLast();
        }

        @Override
        public boolean hasNext() {
            return !isEmpty() && (getValue() != null);
        }

        @Override
        public Entry<Text,V> next() {
            if (hasNext()) {
                Entry<Text,V> next = new UnmodifiableMapEntry<>(getKey(), getValue());
                findNext();
                return next;
            }
            return null;
        }

        @Override
        public void remove() {
            if (hasNext()) {
                queue.peekLast().parent.value = null;
                size--;
            }
        }
    }

    private TrieNode<V> getNode(Text key, boolean createIfNeeded) {
        TrieNode<V> ptr = root;
        for (byte b : key.getBytes()) {
            TrieNode<V> next = ptr.children[(0xff & (int) b)];
            if (next == null) {
                if (createIfNeeded) {
                    ptr.children[(0xff & (int) b)] = next = createNode();
                } else {
                    return null;
                }
            }
            ptr = next;
        }
        return ptr;
    }

    public TrieNode<V> createNode() {
        return new TrieNode();
    }

}
