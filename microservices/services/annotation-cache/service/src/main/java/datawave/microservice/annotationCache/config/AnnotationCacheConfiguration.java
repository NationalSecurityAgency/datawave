package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.DOC_ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import datawave.microservice.annotationCache.AnnotationMapStore;
import datawave.microservice.annotationCache.AnnotationSyncListener;

/** Configures the Hazelcast maps owned by the annotation-cache service. */
@Configuration
public class AnnotationCacheConfiguration {
    private static final Logger log = LoggerFactory.getLogger(AnnotationCacheConfiguration.class);

    private final AnnotationCacheProperties properties;

    public AnnotationCacheConfiguration(AnnotationCacheProperties properties) {
        this.properties = properties;
    }

    @Bean
    public HazelcastInstance hazelcastInstance(Config config, AnnotationMapStore annotationMapStore, AnnotationSyncListener annotationMapListener) {
        configureMaps(config, annotationMapStore, annotationMapListener);
        log.info("Creating annotation cache Hazelcast instance with annotation TTL {} and fetch TTL {}", properties.getMaxCacheAge(),
                        properties.getMaxFetchAge());
        return Hazelcast.newHazelcastInstance(config);
    }

    void configureMaps(Config config, AnnotationMapStore annotationMapStore, AnnotationSyncListener annotationMapListener) {
        int maxCacheAgeSeconds = ttlSeconds("annotation-cache.max-cache-age", properties.getMaxCacheAge());
        int maxFetchAgeSeconds = ttlSeconds("annotation-cache.max-fetch-age", properties.getMaxFetchAge());
        if (properties.getMaxCacheAge().compareTo(properties.getMaxFetchAge()) < 0) {
            throw new IllegalStateException("annotation-cache.max-cache-age must be greater than or equal to annotation-cache.max-fetch-age");
        }

        MapConfig annotationMapConfig = config.getMapConfig(ANNOTATIONS_MAP + "*");
        annotationMapConfig.setTimeToLiveSeconds(maxCacheAgeSeconds);
        annotationMapConfig.setMaxIdleSeconds(0);

        MapStoreConfig storeConfig = annotationMapConfig.getMapStoreConfig();
        storeConfig.setEnabled(true);
        storeConfig.setImplementation(annotationMapStore);

        EntryListenerConfig listenerConfig = new EntryListenerConfig(annotationMapListener, true, true);
        annotationMapConfig.addEntryListenerConfig(listenerConfig);

        MapConfig documentIndexConfig = config.getMapConfig(DOC_ANNOTATIONS_MAP);
        documentIndexConfig.setTimeToLiveSeconds(0);
        documentIndexConfig.setMaxIdleSeconds(0);
        documentIndexConfig.getEvictionConfig().setEvictionPolicy(EvictionPolicy.NONE);

        MapConfig fetchMapConfig = config.getMapConfig(FETCH_MAP + "*");
        fetchMapConfig.setTimeToLiveSeconds(maxFetchAgeSeconds);
        fetchMapConfig.setMaxIdleSeconds(0);
    }

    private int ttlSeconds(String propertyName, Duration duration) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalStateException(propertyName + " must be positive");
        }
        if (duration.getNano() != 0) {
            throw new IllegalStateException(propertyName + " must be specified in whole seconds");
        }

        long seconds = duration.getSeconds();
        if (seconds > Integer.MAX_VALUE) {
            throw new IllegalStateException(propertyName + " exceeds Hazelcast's maximum supported TTL");
        }
        return (int) seconds;
    }
}
