package datawave.security.cache;

import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;

import datawave.security.authorization.DatawaveUser;

/**
 * Represents a cache of mappings of keys to collections of {@link DatawaveUser} instances.
 */
public class DatawaveUserCache implements ElytronCache {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final Cache<String,Collection<DatawaveUser>> cache;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

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
        Preconditions.checkNotNull(key, "key cannot be null");
        Preconditions.checkNotNull(users, "user collection cannot be null");

        lock.writeLock().lock();
        try {
            cache.put(key, users);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Return the collection of users associated with the key, or null if no such mapping exists.
     *
     * @param key
     *            the key
     * @return the user collection, possibly null
     */
    public Collection<DatawaveUser> get(String key) {
        lock.readLock().lock();
        try {
            return cache.getIfPresent(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Set<DatawaveUser> getUsers() {
        lock.readLock().lock();
        try {
            // @formatter:off
            return cache.asMap().values().stream()
                            .flatMap(Collection::stream)
                            .collect(Collectors.toSet());
            // @formatter:on
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Set<DatawaveUser> getUsersWhereNameContains(String substring) {
        lock.readLock().lock();
        try {
            // @formatter:off
            return cache.asMap().values().stream()
                            .flatMap(Collection::stream)
                            .filter(user -> user.getName().contains(substring))
                            .collect(Collectors.toSet());
            // @formatter:on
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public DatawaveUser getUserWithName(String name) {
        lock.readLock().lock();
        try {
            // @formatter:off
            return cache.asMap().values().stream()
                            .flatMap(Collection::stream)
                            .filter(user -> user.getName().equals(name))
                            .findFirst()
                            .orElse(null);
            // @formatter:on
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void evictUsersWithName(String name) {
        lock.writeLock().lock();
        try {
            if (log.isTraceEnabled()) {
                log.trace("Evicting users with name {}", name);
            }

            int totalEvictions = 0;
            ConcurrentMap<String,Collection<DatawaveUser>> map = cache.asMap();
            for (String key : map.keySet()) {
                Collection<DatawaveUser> users = map.get(key);
                if (users != null && users.stream().anyMatch(user -> user.getName().equals(name))) {
                    totalEvictions++;
                    map.remove(key);
                }
            }

            if (log.isTraceEnabled()) {
                log.trace("Removed {} entries with user {}", totalEvictions, name);
            }
        } finally {
            lock.writeLock().unlock();
        }

    }

    @Override
    public void clear() {
        lock.writeLock().lock();
        try {
            log.trace("Clearing the cache");
            cache.invalidateAll();
            cache.cleanUp();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
