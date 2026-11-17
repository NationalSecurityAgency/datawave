package datawave.query.util;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Empties the metadata helper caches before each test class.
 * <p>
 * The metadata lookups on {@link AllFieldMetadataHelper}, {@link TypeMetadataHelper} and {@link datawave.query.composite.CompositeMetadataHelper} are
 * {@code @Cacheable} keyed by {auths, metadataTableName} - never by Accumulo instance. Spring hands every test class sharing a context configuration the same
 * application context, and so the same cache manager, while each class ingests into an Accumulo instance of its own under the same table names and auths.
 * Without this eviction a class that runs second in a JVM reads the first class's field metadata and rejects its own fields as non-existent.
 * <p>
 * Fork-per-class hid this because the cache died with the JVM. It matters as soon as {@code surefire.reuseForks} is on.
 */
public class MetadataHelperCacheEvictor implements BeforeAllCallback {

    private static final String CACHE_MANAGER_BEAN = "metadataHelperCacheManager";

    @Override
    public void beforeAll(ExtensionContext context) {
        // this is registered for every test class in the module, most of which never stand up a spring context
        if (!AnnotatedElementUtils.hasAnnotation(context.getRequiredTestClass(), ContextConfiguration.class)) {
            return;
        }

        ApplicationContext applicationContext = SpringExtension.getApplicationContext(context);
        if (!applicationContext.containsBean(CACHE_MANAGER_BEAN)) {
            return;
        }

        CacheManager cacheManager = applicationContext.getBean(CACHE_MANAGER_BEAN, CacheManager.class);
        for (String cacheName : cacheManager.getCacheNames()) {
            Cache cache = cacheManager.getCache(cacheName);
            if (cache != null) {
                cache.clear();
            }
        }
    }
}
