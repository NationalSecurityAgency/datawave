package datawave.security.cache;

import java.security.Principal;
import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wildfly.security.auth.server.RealmIdentity;
import org.wildfly.security.cache.RealmIdentityCache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;

/**
 * A {@link RealmIdentityCache} implementation that can be treated as an {@link ElytronCache}. This cache supports caching {@link RealmIdentity} instances
 * associated with security realm and security domain principals, and is intended for use with a {@link org.wildfly.security.auth.server.SecurityRealm} to
 * support caching of identities without wrapping the realm in a caching realm. This cache requires the domain principals to be instances of
 * {@link DatawavePrincipal}.
 */
public class DatawaveRealmIdentityCache implements RealmIdentityCache, ElytronCache {

    private static final Logger log = LoggerFactory.getLogger(DatawaveRealmIdentityCache.class);

    // Create the lock with fairness to ensure write operations do not get perpetually blocked by read operations.
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

    /**
     * Holds mappings for domain principals to realm identities.
     */
    private final Cache<DatawavePrincipal,RealmIdentity> domainPrincipalsToRealmIdentities;

    /**
     * Holds mappings of realm identity principals to domain principals.
     */
    private final Multimap<Principal,Principal> realmPrincipalsToDomainPrincipals;

    /**
     * Return a new {@link DatawaveRealmIdentityCache}.
     *
     * @param maxSize
     *            the maximum number of entries to keep in the cache, unlimited if given a negative value
     * @param ttl
     *            the time in milliseconds that an entry can stay in the cache, unlimited if given a negative value
     */
    public DatawaveRealmIdentityCache(long maxSize, long ttl) {
        Caffeine<Object,Object> cacheBuilder = Caffeine.newBuilder();
        if (maxSize > -1) {
            cacheBuilder.maximumSize(maxSize);
        }
        if (ttl > -1) {
            cacheBuilder.expireAfterWrite(Duration.ofMillis(ttl));
        }

        // Add an eviction listener to the cache that will remove corresponding entries from the realm principal cache if an entry in the domain principal
        // cache is removed due to expiration or size.
        cacheBuilder.evictionListener(new RemovalListener<Principal,RealmIdentity>() {
            @Override
            public void onRemoval(@Nullable Principal principal, @Nullable RealmIdentity realmIdentity, RemovalCause removalCause) {
                if (removalCause == RemovalCause.EXPIRED || removalCause == RemovalCause.SIZE) {
                    if (realmIdentity != null) {
                        realmPrincipalsToDomainPrincipals.removeAll(realmIdentity.getRealmIdentityPrincipal());
                    }
                }
            }
        });

        domainPrincipalsToRealmIdentities = cacheBuilder.build();
        realmPrincipalsToDomainPrincipals = HashMultimap.create();
    }

    /**
     * Associate the given realm identity with the given principal. The principal must be an instance of {@link DatawavePrincipal}.
     *
     * @param principal
     *            the {@link Principal} that references the realm identity being cached
     * @param realmIdentity
     *            the {@link RealmIdentity} instance
     */
    @Override
    public void put(Principal principal, RealmIdentity realmIdentity) {
        Preconditions.checkArgument(principal instanceof DatawavePrincipal, "principal must be of type " + DatawavePrincipal.class.getName());

        if (obtainedWriteLock()) {
            try {
                domainPrincipalsToRealmIdentities.put((DatawavePrincipal) principal, realmIdentity);
                realmPrincipalsToDomainPrincipals.put(realmIdentity.getRealmIdentityPrincipal(), principal);
            } finally {
                lock.writeLock().lock();
            }
        }
    }

    /**
     * Return the realm identity for the given principal. The principal may a realm derived from a realm identity, or a security domain.
     *
     * @param principal
     *            the {@link Principal} that references a previously cached realm identity
     * @return the identity if present, or null otherwise
     */
    @Override
    public RealmIdentity get(Principal principal) {
        if (obtainedReadLock()) {
            try {
                // Attempt to fetch an identity directly from the realm identity cache. This will succeed if the principal is a domain principal.
                RealmIdentity identity = domainPrincipalsToRealmIdentities.getIfPresent(principal);
                if (identity != null) {
                    return identity;
                }

                // If no identity was found, the principal may be a realm identity principal. If any domain principals are stored for the realm identity
                // principal, return the realm identity associated with the first available domain principal.
                Collection<Principal> domainPrincipals = realmPrincipalsToDomainPrincipals.get(principal);
                if (!domainPrincipals.isEmpty()) {
                    return domainPrincipalsToRealmIdentities.getIfPresent(domainPrincipals.iterator().next());
                } else {
                    return null;
                }
            } finally {
                lock.readLock().unlock();
            }
        }
        return null;
    }

    /**
     * Remove any realm identities associated with the given principal.
     *
     * @param principal
     *            the {@link Principal} that references a previously cached realm identity
     */
    @Override
    public void remove(Principal principal) {
        if (obtainedWriteLock()) {
            try {
                RealmIdentity identity = domainPrincipalsToRealmIdentities.getIfPresent(principal);
                // If the identity is not null, the principal is a domain principal. Remove it from the cache and remove any mappings for the associated realm
                // identity principal.
                if (identity != null) {
                    domainPrincipalsToRealmIdentities.invalidate(principal);
                    Principal realmIdentityPrincipal = identity.getRealmIdentityPrincipal();
                    realmPrincipalsToDomainPrincipals.get(realmIdentityPrincipal).forEach(domainPrincipalsToRealmIdentities::invalidate);
                    realmPrincipalsToDomainPrincipals.removeAll(realmIdentityPrincipal);
                } else {
                    // Otherwise, the principal may be a realm identity principal. Remove all domain principal mappings for it, and remove any mappings for
                    // those domain principals from the cache.
                    if (realmPrincipalsToDomainPrincipals.containsKey(principal)) {
                        realmPrincipalsToDomainPrincipals.get(principal).forEach(domainPrincipalsToRealmIdentities::invalidate);
                        realmPrincipalsToDomainPrincipals.removeAll(principal);
                    }
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    @Override
    public void clear() {
        log.trace("Clearing the cache");
        if (obtainedWriteLock()) {
            try {
                // Clear the cache.
                domainPrincipalsToRealmIdentities.invalidateAll();
                domainPrincipalsToRealmIdentities.cleanUp();
                // Clear the domain principals.
                realmPrincipalsToDomainPrincipals.clear();
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    @Override
    public Set<DatawaveUser> getUsers() {
        if (obtainedReadLock()) {
            try {
                // @formatter:off
                return domainPrincipalsToRealmIdentities.asMap().keySet().stream()
                                .map(DatawavePrincipal::getProxiedUsers)
                                .flatMap(Collection::stream).collect(Collectors.toSet());
                // @formatter:on
            } finally {
                lock.readLock().unlock();
            }
        }
        return Set.of();
    }

    @Override
    public Set<DatawaveUser> getUsersWhereNameContains(String substring) {
        if (obtainedReadLock()) {
            try {
                // @formatter:off
                return domainPrincipalsToRealmIdentities.asMap().keySet().stream()
                                .map(DatawavePrincipal::getProxiedUsers)
                                .flatMap(Collection::stream)
                                .filter(user -> user.getName().contains(substring))
                                .collect(Collectors.toSet());
                // @formatter:on
            } finally {
                lock.readLock().unlock();
            }
        }
        return Set.of();
    }

    @Override
    public DatawaveUser getUserWithName(String name) {
        if (obtainedReadLock()) {
            try {
                // @formatter:off
                return domainPrincipalsToRealmIdentities.asMap().keySet().stream()
                                .map(DatawavePrincipal::getProxiedUsers)
                                .flatMap(Collection::stream)
                                .filter(user -> user.getName().equals(name))
                                .findFirst()
                                .orElse(null);
                // @formatter:on
            } finally {
                lock.readLock().unlock();
            }
        }
        return null;
    }

    @Override
    public void evictUsersWithName(String name) {
        if (log.isTraceEnabled()) {
            log.trace("Evicting users with name {}", name);
        }

        if (obtainedWriteLock()) {
            try {
                int totalEvictions = 0;
                for (DatawavePrincipal principal : domainPrincipalsToRealmIdentities.asMap().keySet()) {
                    if (principal.getProxiedUsers().stream().anyMatch(user -> name.equals(user.getName()))) {
                        totalEvictions++;
                        remove(principal);
                    }
                }
                if (log.isTraceEnabled()) {
                    log.trace("Removed {} entries with user {}", totalEvictions, name);
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    /**
     * Attempt to obtain a read lock.
     *
     * @return true if the lock was obtained or false otherwise
     */
    private boolean obtainedReadLock() {
        try {
            lock.readLock().lockInterruptibly();
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }

    /**
     * Attempt to obtain a write lock.
     *
     * @return true if the lock was obtained or false otherwise
     */
    private boolean obtainedWriteLock() {
        try {
            lock.writeLock().lockInterruptibly();
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }
}
