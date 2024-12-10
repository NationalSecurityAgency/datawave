package datawave.query.jexl.lookups.cache;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.base.Preconditions;

/**
 * Class that supports caching an IndexLookup failure, i.e. when a term fails to expand for any reason.
 * <p>
 * The reason for this is that a failed expansion is not likely to succeed in the future. There is no reason to repeat work that will ultimately fail,
 * especially if the failure is due to a timeout exception.
 */
public class LookupFailureCache {

    private static final Logger log = LoggerFactory.getLogger(LookupFailureCache.class);

    protected final String name;
    protected final Cache<LookupCacheKey,Boolean> cache;

    /**
     * Default constructor
     *
     * @param name
     *            the cache name
     * @param size
     *            the cache size
     * @param expireAfterWriteMinutes
     *            time to expire entry after write
     * @param expireAfterAccessMinutes
     *            time to expire entry after access
     */
    public LookupFailureCache(String name, int size, int expireAfterWriteMinutes, int expireAfterAccessMinutes) {
        this.name = name;
        //  @formatter:off
        cache = Caffeine.newBuilder()
                //  bound the cache size, remove the least used entries
                .maximumSize(size)
                //  do not persist cache entries forever, place a bound on total lifetime
                .expireAfterWrite(expireAfterWriteMinutes, TimeUnit.MINUTES)
                //  allow cache updates to influence expiration time
                .expireAfterAccess(expireAfterAccessMinutes, TimeUnit.MINUTES)
                //  always record stats
                .recordStats()
                .build();
        //  @formatter:on

        log.debug("created {} lookup cache, size {}, write expire: {}m, access expire: {}m", name, size, expireAfterWriteMinutes, expireAfterAccessMinutes);
    }

    public CacheStats stats() {
        Preconditions.checkNotNull(cache);
        return cache.stats();
    }

    public void cleanup() {
        cache.cleanUp();
    }

    public boolean lookupFailed(LookupCacheKey key) {
        Boolean value = cache.getIfPresent(key);
        // only recording failures, so cache miss returns a true value
        return Objects.requireNonNullElse(value, false);
    }

    public void recordFailure(LookupCacheKey key) {
        Preconditions.checkNotNull(key);
        cache.put(key, true);
    }

    public void logStats() {
        log.info("{} {}", name, cache.stats().toString());
    }

    // interface that ensures each lookup cache implementation provides their own key
    public interface LookupCacheKey {

        boolean equals(Object o);

        int hashCode();
    }
}
