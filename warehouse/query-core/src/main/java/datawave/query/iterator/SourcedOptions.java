package datawave.query.iterator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * Container for iterator options, plus a source iterator and its environment
 *
 * @param <K>
 *            type for key
 * @param <V>
 *            type for value
 */
public class SourcedOptions<K,V> extends HashMap<K,V> {
    
    private static final long serialVersionUID = 1771565394501515800L;
    
    private final IteratorEnvironment env;
    private final SortedKeyValueIterator<Key,Value> source;
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    public SourcedOptions(final SortedKeyValueIterator<Key,Value> source) {
        this(source, null, (Map) null);
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    public SourcedOptions(final SortedKeyValueIterator<Key,Value> source, final IteratorEnvironment env) {
        this(source, env, (Map) null);
    }
    
    public SourcedOptions(final SortedKeyValueIterator<Key,Value> source, final Map<? extends K,? extends V> map) {
        this(source, null, map);
    }
    
    public SourcedOptions(final SortedKeyValueIterator<Key,Value> source, final IteratorEnvironment env, int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
        this.source = source;
        this.env = env;
    }
    
    public SourcedOptions(final SortedKeyValueIterator<Key,Value> source, final IteratorEnvironment env, int initialCapacity) {
        super(initialCapacity);
        this.source = source;
        this.env = env;
    }
    
    @SuppressWarnings("unchecked")
    public SourcedOptions(final SortedKeyValueIterator<Key,Value> source, final IteratorEnvironment env, final Map<? extends K,? extends V> map) {
        super((null != map) ? map : (Map<? extends K,? extends V>) Collections.emptyMap());
        this.source = source;
        this.env = env;
    }
    
    public IteratorEnvironment getEnvironment() {
        return this.env;
    }
    
    public SortedKeyValueIterator<Key,Value> getSource() {
        return this.source;
    }
}
