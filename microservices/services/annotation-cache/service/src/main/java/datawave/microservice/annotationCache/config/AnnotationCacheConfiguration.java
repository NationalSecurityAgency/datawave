package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.DOCUMENT_ID_KEY_ATTRIBUTE;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static datawave.microservice.annotationCache.api.Constants.ID_TYPE_KEY_ATTRIBUTE;

import java.time.Duration;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import datawave.microservice.annotationCache.AnnotationMapStore;
import datawave.microservice.annotationCache.AnnotationSyncListener;

/**
 * Creates the annotation-cache Hazelcast instance and configures annotation retention, RabbitMQ write-through, and fetch-freshness invalidation.
 */
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
        // Convert configured retention periods to the whole-second TTL values Hazelcast accepts.
        int maxCacheAgeSeconds = ttlSeconds("annotation-cache.max-cache-age", properties.getMaxCacheAge());
        int maxFetchAgeSeconds = ttlSeconds("annotation-cache.max-fetch-age", properties.getMaxFetchAge());

        // Keep freshness-record retention no longer than annotation retention, so new records cannot outlive their annotation by TTL alone.
        if (properties.getMaxCacheAge().compareTo(properties.getMaxFetchAge()) < 0) {
            throw new IllegalStateException("annotation-cache.max-cache-age must be greater than or equal to annotation-cache.max-fetch-age");
        }

        // Configure only the two named maps. In particular, do not apply MapStore/listeners/indexes to Hazelcast's default map config.
        MapConfig annotationMapConfig = exactMapConfig(config, ANNOTATIONS_MAP);
        annotationMapConfig.setTimeToLiveSeconds(maxCacheAgeSeconds);
        annotationMapConfig.setMaxIdleSeconds(0);
        addHashIndex(annotationMapConfig, ID_TYPE_KEY_ATTRIBUTE, DOCUMENT_ID_KEY_ATTRIBUTE);
        addHashIndex(annotationMapConfig, DOCUMENT_ID_KEY_ATTRIBUTE);

        // Use the MapStore to publish eligible local write-through annotations to RabbitMQ before accepting the write.
        MapStoreConfig storeConfig = annotationMapConfig.getMapStoreConfig();
        storeConfig.setEnabled(true);
        storeConfig.setImplementation(annotationMapStore);

        // Listen only for local events and omit values; keys identify the affected document.
        boolean listenerAlreadyConfigured = annotationMapConfig.getEntryListenerConfigs().stream()
                        .anyMatch(existing -> existing.getImplementation() == annotationMapListener);
        if (!listenerAlreadyConfigured) {
            annotationMapConfig.addEntryListenerConfig(new EntryListenerConfig(annotationMapListener, true, false));
        }

        // Fetch-marker TTL controls when Datawave is checked again; reads do not extend retention.
        MapConfig fetchMapConfig = exactMapConfig(config, FETCH_MAP);
        fetchMapConfig.setTimeToLiveSeconds(maxFetchAgeSeconds);
        fetchMapConfig.setMaxIdleSeconds(0);
        addHashIndex(fetchMapConfig, ID_TYPE_KEY_ATTRIBUTE, DOCUMENT_ID_KEY_ATTRIBUTE);
        addHashIndex(fetchMapConfig, DOCUMENT_ID_KEY_ATTRIBUTE);
    }

    private MapConfig exactMapConfig(Config config, String mapName) {
        MapConfig mapConfig = config.getMapConfigs().get(mapName);
        if (mapConfig == null) {
            mapConfig = new MapConfig(mapName);
            config.addMapConfig(mapConfig);
        }
        return mapConfig;
    }

    private void addHashIndex(MapConfig mapConfig, String... attributes) {
        boolean configured = mapConfig.getIndexConfigs().stream()
                        .anyMatch(index -> index.getType() == IndexType.HASH && index.getAttributes().equals(Arrays.asList(attributes)));
        if (!configured) {
            mapConfig.addIndexConfig(new IndexConfig(IndexType.HASH, attributes));
        }
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
