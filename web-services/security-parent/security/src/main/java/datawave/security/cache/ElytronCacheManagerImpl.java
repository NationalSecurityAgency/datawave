package datawave.security.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.annotation.security.PermitAll;
import javax.ejb.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.security.authorization.DatawaveUser;

/**
 * Implementation of {@link ElytronCacheManager} that is expected to be injected into a {@link CredentialsCacheBean}. In order to allow the methods here to be
 * invoked in the datawave elytron module from an unauthenticated context, we must use {@link PermitAll}.
 */
@PermitAll
@Singleton
public class ElytronCacheManagerImpl implements ElytronCacheManager {

    private static final Logger log = LoggerFactory.getLogger(ElytronCacheManagerImpl.class);

    /**
     * The list of managed Elytron caches.
     */
    private final List<ElytronCache> caches = new ArrayList<>();

    /**
     * Guards against concurrent read/write operations.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    @Override
    public void addCache(ElytronCache cache) {
        Preconditions.checkNotNull(cache, "cache cannot be null");

        lock.writeLock().lock();
        if (log.isTraceEnabled()) {
            log.trace("Adding cache {}", cache.getClass().getName());
        }
        try {
            this.caches.add(cache);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Set<DatawaveUser> getUsers() {
        lock.readLock().lock();
        try {
            // @formatter:off
            return caches.stream()
                            .map(ElytronCache::getUsers)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toSet());
            // @formatter:on
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Set<DatawaveUser> getUsersWhereNameContains(String substring) {
        Preconditions.checkNotNull(substring, "substring cannot be null");
        lock.readLock().lock();
        try {
            // @formatter:off
            return caches.stream()
                            .map(cache -> cache.getUsersWhereNameContains(substring))
                            .flatMap(Collection::stream)
                            .collect(Collectors.toSet());
            // @formatter:on
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public DatawaveUser getUserWithName(String name) {
        Preconditions.checkNotNull(name, "name cannot be null");
        lock.readLock().lock();
        try {
            for (ElytronCache cache : caches) {
                DatawaveUser user = cache.getUserWithName(name);
                if (user != null) {
                    return user;
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void evictUsersWithName(String name) {
        lock.writeLock().lock();
        try {
            if (log.isTraceEnabled()) {
                log.trace("Evicting all users with name {}", name);
            }
            caches.forEach(delegate -> delegate.evictUsersWithName(name));
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void clear() {
        lock.writeLock().lock();
        try {
            log.trace("Clearing all caches");
            caches.forEach(ElytronCache::clear);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
