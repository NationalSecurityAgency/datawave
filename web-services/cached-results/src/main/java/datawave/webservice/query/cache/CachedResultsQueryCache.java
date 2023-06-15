package datawave.webservice.query.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import datawave.webservice.results.cached.CachedRunningQuery;

import javax.inject.Singleton;
import java.util.concurrent.TimeUnit;

/**
 * A cache for storing {@link CachedRunningQuery} objects. The size of the cache is limited to 20,000 objects and each one expires 24 hours after its last
 * access time.
 */
@Singleton
// CDI singleton
public class CachedResultsQueryCache extends AbstractQueryCache<CachedRunningQuery> {

    @Override
    protected Cache<String,CachedRunningQuery> buildCache() {
        return CacheBuilder.newBuilder().expireAfterAccess(24, TimeUnit.HOURS).maximumSize(20000).concurrencyLevel(1000).build();
    }
}
