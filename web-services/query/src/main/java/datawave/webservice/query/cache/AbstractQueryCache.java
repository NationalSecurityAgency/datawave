package datawave.webservice.query.cache;

import com.google.common.cache.Cache;

import javax.annotation.PostConstruct;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A Guava-based cache for storing objects in the {@link AbstractRunningQuery} hierarchy.
 */
public abstract class AbstractQueryCache<T extends AbstractRunningQuery> implements Iterable<T> {
    private Cache<String,T> cache;
    
    @PostConstruct
    public void init() {
        cache = buildCache();
    }
    
    protected abstract Cache<String,T> buildCache();
    
    /**
     * Indicates whether or not this cache contains a {@link AbstractRunningQuery} associated with the query {@code id}.
     * 
     * @param id
     *            the id
     * @return boolean
     */
    public boolean containsKey(String id) {
        return cache.asMap().containsKey(id);
    }
    
    /**
     * Gets the {@link AbstractRunningQuery} whose query id is {@code id}, or {@code null} if there is no query cached under {@code id}.
     * 
     * @param id
     *            the id
     * @return the running query
     */
    public T get(String id) {
        return cache.getIfPresent(id);
    }
    
    /**
     * Caches {@code query} under the identifier {@code id}.
     * 
     * @param id
     *            the id
     * @param query
     *            a query
     */
    public void put(String id, T query) {
        cache.put(id, query);
    }
    
    /**
     * Removes the query identified by {@code id} from the cache.
     * 
     * @param id
     *            the id
     */
    public void remove(String id) {
        cache.invalidate(id);
    }
    
    /**
     * Removes all entries from the cache.
     */
    public void clear() {
        cache.asMap().clear();
    }
    
    /**
     * Retrieve an {@link java.util.Iterator} to iterate over all stored {@link AbstractRunningQuery}s stored in the cache.
     */
    @Override
    public Iterator<T> iterator() {
        return cache.asMap().values().iterator();
    }
    
    /**
     * Retrieve a {@link java.util.Set} of String vs {@link AbstractRunningQuery} entries stored in the cache.
     */
    public Set<Map.Entry<String,T>> entrySet() {
        return cache.asMap().entrySet();
    }
}
