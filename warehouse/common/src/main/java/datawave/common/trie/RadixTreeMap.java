package datawave.common.trie;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A Radix Tree is a compressed TRIE optimized for space.
 */
public class RadixTreeMap<R> extends AbstractMap<String,R> implements Map<String,R>, Serializable {

    protected RadixTreeSet set;

    public RadixTreeMap() {
        set = new RadixTreeSet();
    }

    @Override
    public int size() {
        return set.size();
    }

    @Override
    public boolean containsKey(Object key) {
        return set.contains(key);
    }

    @Override
    public R get(Object key) {
        return (R) (set.getPayload((String) key));
    }

    @Override
    public R put(String key, R value) {
        R previous = get(key);
        set.put(key, value);
        return previous;
    }

    @Override
    public R remove(Object key) {
        R previous = get(key);
        set.remove(key);
        return previous;
    }

    @Override
    public void clear() {
        set.clear();
    }

    @Override
    public Set<String> keySet() {
        return set;
    }

    @Override
    public Set<Entry<String,R>> entrySet() {
        return new AbstractSet<Entry<String,R>>() {
            @Override
            public int size() {
                return set.size();
            }

            @Override
            public void clear() {
                set.clear();
            }

            @Override
            public boolean contains(Object entry) {
                if (!(entry instanceof Map.Entry)) {
                    return false;
                } else {
                    Map.Entry<String,R> e = (Map.Entry) entry;
                    Object payload = set.getPayload(e.getKey());
                    return payload != null && payload.equals(e.getValue());
                }
            }

            @Override
            public boolean remove(Object obj) {
                if (!(obj instanceof Map.Entry)) {
                    return false;
                } else {
                    Map.Entry<String,R> e = (Map.Entry) obj;
                    return RadixTreeMap.this.remove(e.getKey(), e.getValue());
                }
            }

            @Override
            public Iterator<Entry<String,R>> iterator() {
                final Iterator<Map.Entry<String,RadixSetNode>> iterator = set.nodeIterator();
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Entry<String,R> next() {
                        final Entry<String,RadixSetNode> next = iterator.next();
                        return new Entry<String,R>() {
                            @Override
                            public String getKey() {
                                return next.getKey();
                            }

                            @Override
                            public R getValue() {
                                return (R) (next.getValue().getPayload());
                            }

                            @Override
                            public R setValue(R value) {
                                set.put(getKey(), value);
                                return getValue();
                            }
                        };
                    }

                    @Override
                    public void remove() {
                        iterator.remove();
                    }
                };
            }
        };
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        RadixTreeMap<R> map = new RadixTreeMap<>();
        for (Map.Entry<String,R> entry : entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

}
