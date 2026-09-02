package datawave.security.cache;

import java.security.Principal;
import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wildfly.security.auth.server.RealmIdentity;
import org.wildfly.security.cache.RealmIdentityCache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.base.Preconditions;

import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;

/**
 * A {@link RealmIdentityCache} implementation that can be treated as an {@link ElytronCache}. This cache supports caching {@link RealmIdentity} instances
 * associated with security realm and security domain principals, and is intended for use with a {@link org.wildfly.security.auth.server.SecurityRealm} to
 * support caching of identities without wrapping the realm in a caching realm. This cache requires the domain principals to be instances of
 * {@link DatawavePrincipal}.
 * <p>
 * This cache is lock free. {@link #get(Principal)} sits on the authentication path of every request, so reads must never block behind a write.
 */
public class DatawaveRealmIdentityCache implements RealmIdentityCache, ElytronCache {

    private static final Logger log = LoggerFactory.getLogger(DatawaveRealmIdentityCache.class);

    /**
     * Holds mappings for domain principals to realm identities.
     */
    private final Cache<DatawavePrincipal,RealmIdentity> domainPrincipalsToRealmIdentities;

    /**
     * Holds mappings of realm identity principals to the domain principals that resolved to them.
     */
    private final ConcurrentMap<Principal,Set<Principal>> realmPrincipalsToDomainPrincipals = new ConcurrentHashMap<>();

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

        // Drop the reverse mapping for an entry the cache evicted on its own. Only the evicted domain principal is unmapped. Other domain principals that
        // resolve to the same realm identity principal may still have live entries.
        cacheBuilder.evictionListener((RemovalListener<Principal,RealmIdentity>) (principal, realmIdentity, removalCause) -> {
            if (removalCause == RemovalCause.EXPIRED || removalCause == RemovalCause.SIZE) {
                unmapDomainPrincipal(principal, realmIdentity);
            }
        });

        domainPrincipalsToRealmIdentities = cacheBuilder.build();
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

        domainPrincipalsToRealmIdentities.put((DatawavePrincipal) principal, realmIdentity);
        Principal realmPrincipal = realmIdentity.getRealmIdentityPrincipal();
        if (realmPrincipal != null) {
            realmPrincipalsToDomainPrincipals.computeIfAbsent(realmPrincipal, k -> ConcurrentHashMap.newKeySet()).add(principal);
        }
    }

    /**
     * Return the realm identity for the given principal. The principal may be a realm identity principal or a security domain principal.
     *
     * @param principal
     *            the {@link Principal} that references a previously cached realm identity
     * @return the identity if present, or null otherwise
     */
    @Override
    public RealmIdentity get(Principal principal) {
        // Attempt to fetch an identity directly from the realm identity cache. This will succeed if the principal is a domain principal.
        RealmIdentity identity = domainPrincipalsToRealmIdentities.getIfPresent(principal);
        if (identity != null) {
            return identity;
        }

        // If no identity was found, the principal may be a realm identity principal. If any domain principals are stored for the realm identity principal,
        // return the realm identity associated with the first one that still has a live entry.
        Set<Principal> domainPrincipals = realmPrincipalsToDomainPrincipals.get(principal);
        if (domainPrincipals != null) {
            for (Principal domainPrincipal : domainPrincipals) {
                identity = domainPrincipalsToRealmIdentities.getIfPresent(domainPrincipal);
                if (identity != null) {
                    return identity;
                }
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
        RealmIdentity identity = domainPrincipalsToRealmIdentities.getIfPresent(principal);
        // If the identity is not null, the principal is a domain principal. Remove it from the cache and remove any mappings for the associated realm identity
        // principal.
        if (identity != null) {
            domainPrincipalsToRealmIdentities.invalidate(principal);
            removeRealmPrincipal(identity.getRealmIdentityPrincipal());
        } else {
            // Otherwise, the principal may be a realm identity principal. Remove all domain principal mappings for it, and remove any mappings for those
            // domain principals from the cache.
            removeRealmPrincipal(principal);
        }
    }

    /**
     * Invalidate every domain principal mapped to the given realm identity principal, and drop the reverse mapping.
     *
     * @param realmPrincipal
     *            the realm identity principal, possibly null
     */
    private void removeRealmPrincipal(Principal realmPrincipal) {
        if (realmPrincipal == null) {
            return;
        }
        Set<Principal> domainPrincipals = realmPrincipalsToDomainPrincipals.remove(realmPrincipal);
        if (domainPrincipals != null) {
            domainPrincipals.forEach(domainPrincipalsToRealmIdentities::invalidate);
        }
    }

    /**
     * Drop the reverse mapping for a single domain principal that is no longer cached.
     *
     * @param principal
     *            the domain principal that was evicted, possibly null
     * @param realmIdentity
     *            the realm identity it was mapped to, possibly null
     */
    private void unmapDomainPrincipal(Principal principal, RealmIdentity realmIdentity) {
        if (principal == null || realmIdentity == null) {
            return;
        }
        Principal realmPrincipal = realmIdentity.getRealmIdentityPrincipal();
        if (realmPrincipal == null) {
            return;
        }
        realmPrincipalsToDomainPrincipals.computeIfPresent(realmPrincipal, (key, domainPrincipals) -> {
            domainPrincipals.remove(principal);
            return domainPrincipals.isEmpty() ? null : domainPrincipals;
        });
    }

    @Override
    public void clear() {
        log.trace("Clearing the cache");
        domainPrincipalsToRealmIdentities.invalidateAll();
        domainPrincipalsToRealmIdentities.cleanUp();
        realmPrincipalsToDomainPrincipals.clear();
    }

    @Override
    public Set<DatawaveUser> getUsers() {
        // @formatter:off
        return domainPrincipalsToRealmIdentities.asMap().keySet().stream()
                        .map(DatawavePrincipal::getProxiedUsers)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
        // @formatter:on
    }

    @Override
    public Set<DatawaveUser> getUsersWhereNameContains(String substring) {
        // @formatter:off
        return domainPrincipalsToRealmIdentities.asMap().keySet().stream()
                        .map(DatawavePrincipal::getProxiedUsers)
                        .flatMap(Collection::stream)
                        .filter(user -> user.getName().contains(substring))
                        .collect(Collectors.toSet());
        // @formatter:on
    }

    @Override
    public DatawaveUser getUserWithName(String name) {
        // @formatter:off
        return domainPrincipalsToRealmIdentities.asMap().keySet().stream()
                        .map(DatawavePrincipal::getProxiedUsers)
                        .flatMap(Collection::stream)
                        .filter(user -> user.getName().equals(name))
                        .findFirst()
                        .orElse(null);
        // @formatter:on
    }

    @Override
    public void evictUsersWithName(String name) {
        if (log.isTraceEnabled()) {
            log.trace("Evicting users with name {}", name);
        }

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
    }
}
