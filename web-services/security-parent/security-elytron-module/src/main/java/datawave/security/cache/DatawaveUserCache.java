package datawave.security.cache;

import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import datawave.security.authorization.DatawaveUser;

/**
 * Represents a cache of mappings of keys to collections of {@link DatawaveUser} instances.
 */
public class DatawaveUserCache implements ElytronCache {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final Cache<String,Collection<DatawaveUser>> cache;

    /**
     * Return a new {@link DatawaveUserCache} with the given maximum size and time to live for entries in the cache.
     *
     * @param maxSize
     *            the maximum number of entries to keep in the cache. No maximum will be set if given a negative value
     * @param ttl
     *            the time in milliseconds that an entry can stay in the cache, unlimited if given a negative value
     */
    public DatawaveUserCache(long maxSize, long ttl) {
        Caffeine<Object,Object> cacheBuilder = Caffeine.newBuilder();
        if (maxSize > -1) {
            cacheBuilder.maximumSize(maxSize);
        }
        if (ttl > -1) {
            cacheBuilder.expireAfterWrite(Duration.ofMillis(ttl));
        }
        this.cache = cacheBuilder.build();
    }

    /**
     * Associate the collection of users with the given key in the cache.
     *
     * @param key
     *            the key
     * @param users
     *            the users
     */
    public void put(String key, Collection<DatawaveUser> users) {
        cache.put(key, users);
    }

    /**
     * Return the collection of users associated with the key, or null if no such mapping exists.
     *
     * @param key
     *            the key
     * @return the user collection, possibly null
     */
    public Collection<DatawaveUser> get(String key) {
        return cache.getIfPresent(key);
    }

    @Override
    public Set<DatawaveUser> getUsers() {
        // @formatter:off
        return cache.asMap().keySet().stream()
                        .map(this::get)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
        // @formatter:on
    }

    @Override
    public Set<DatawaveUser> getUsersWhereNameContains(String substring) {
        // @formatter:off
        return cache.asMap().keySet().stream()
                        .map(this::get)
                        .flatMap(Collection::stream)
                        .filter(user -> user.getName().contains(substring))
                        .collect(Collectors.toSet());
        // @formatter:on
    }

    @Override
    public DatawaveUser getUserWithName(String name) {
        // @formatter:off
        return cache.asMap().keySet().stream()
                        .map(this::get)
                        .flatMap(Collection::stream)
                        .filter(user -> user.getName().equals(name))
                        .findFirst()
                        .orElse(null);
        // @formatter:on
    }

    @Override
    public void evictUsersWithName(String name) {
        if(log.isTraceEnabled()) {
            log.trace("Evicting users with name {}", name);
        }

        int totalEvictions = 0;
        ConcurrentMap<String,Collection<DatawaveUser>> map = cache.asMap();
        for(String key : map.keySet()) {
            if(map.get(key).stream().anyMatch(user -> user.getName().equals(name))) {
                totalEvictions++;
                map.remove(key);
            }
        }

        if(log.isTraceEnabled()) {
            log.trace("Removed {} entries with user {}", totalEvictions, name);
        }
    }

    @Override
    public void clear() {
        log.trace("Clearing the cache");
        cache.invalidateAll();
        cache.cleanUp();
    }
}
