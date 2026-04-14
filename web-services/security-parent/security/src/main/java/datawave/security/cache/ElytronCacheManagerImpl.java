package datawave.security.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
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

    private final List<ElytronCache> caches = new ArrayList<>();

    @Override
    public void addCache(ElytronCache cache) {
        Preconditions.checkNotNull(cache, "cache cannot be null");
        if (log.isTraceEnabled()) {
            log.trace("Adding cache {}", cache.getClass().getName());
        }
        this.caches.add(cache);
    }

    @Override
    public Set<DatawaveUser> getUsers() {
        // @formatter:off
        return caches.stream()
                        .map(ElytronCache::getUsers)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
        // @formatter:on
    }

    @Override
    public Set<DatawaveUser> getUsersWhereNameContains(String substring) {
        Preconditions.checkNotNull(substring, "substring cannot be null");
        // @formatter:off
        return caches.stream()
                        .map(cache -> cache.getUsersWhereNameContains(substring))
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
        // @formatter:on
    }

    @Override
    public DatawaveUser getUserWithName(String name) {
        Preconditions.checkNotNull(name, "name cannot be null");

        for (ElytronCache cache : caches) {
            DatawaveUser user = cache.getUserWithName(name);
            if (user != null) {
                return user;
            }
        }
        return null;
    }

    @Override
    public void evictUsersWithName(String name) {
        if (log.isTraceEnabled()) {
            log.trace("Evicting all users with name {}", name);
        }
        caches.forEach(delegate -> delegate.evictUsersWithName(name));
    }

    @Override
    public void clear() {
        log.trace("Clearing all caches");
        caches.forEach(ElytronCache::clear);
    }
}
