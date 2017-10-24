package datawave.microservice.cached;

import java.util.List;

/**
 * Provides functionality to query the objects (typically DatawaveUser objects) stored in a cache.
 */
public interface CacheInspector {
    /**
     * Retrieves the T cached in cacheName under key.
     *
     * @param cacheName
     *            the name of the cache to search
     * @param cacheObjectType
     *            the type of object stored in cacheName
     * @param key
     *            the key to lookup in cache
     * @param <T>
     *            the type of object stored in cacheName
     * @return the T stored in the cache named cacheName under key, or null if no such object exists
     */
    <T> T list(String cacheName, Class<T> cacheObjectType, String key);
    
    /**
     * Lists all Ts in the cache named cacheName.
     *
     * @param cacheName
     *            the name of the cache to search
     * @param cacheObjectType
     *            the type of object stored in cacheName
     * @param <T>
     *            the type of object stored in cacheName
     * @return all Ts stored in the cache named cacheName
     */
    <T> List<? extends T> listAll(String cacheName, Class<T> cacheObjectType);
    
    /**
     * Lists all Ts in the cache named cacheName stored under keys containing substring.
     *
     * @param cacheName
     *            the name of the cache to search
     * @param cacheObjectType
     *            the type of object stored in cacheName
     * @param substring
     *            a substring that appears in the key used to store all of the returned entries
     * @param <T>
     *            the type of object stored in cacheName
     * @return all Ts stored in the cache named cacheName under a key containing substring
     */
    <T> List<? extends T> listMatching(String cacheName, Class<T> cacheObjectType, String substring);
    
    /**
     * Evicts from the cache named cacheName all Ts stored under keys containing substring.
     *
     * @param cacheName
     *            the name of the cache to search
     * @param cacheObjectType
     *            the type of object stored in cacheName
     * @param substring
     *            a substring contained in the corresponding key for all evicted entries
     * @param <T>
     *            the type of object stored in cacheName
     * @return the number of entries that were evicted from the cache
     */
    <T> int evictMatching(String cacheName, Class<T> cacheObjectType, String substring);
}
