package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.DOCUMENT_ID_KEY_ATTRIBUTE;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static datawave.microservice.annotationCache.api.Constants.ID_TYPE_KEY_ATTRIBUTE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;

import datawave.microservice.annotationCache.AnnotationMapStore;
import datawave.microservice.annotationCache.AnnotationSyncListener;

/** Tests the cache maps, indexes, and TTL settings created by the service configuration. */
class AnnotationCacheConfigurationTest {
    private AnnotationCacheProperties properties;
    private Config config;
    private AnnotationMapStore mapStore;
    private AnnotationSyncListener listener;

    @BeforeEach
    void setUp() {
        properties = new AnnotationCacheProperties();
        config = new Config();
        mapStore = mock(AnnotationMapStore.class);
        listener = mock(AnnotationSyncListener.class);
    }

    /** Checks the two map configurations, their policies, listeners, and indexes. */
    @Test
    void configuresOnlyTheTwoFixedMapsWithExpectedPoliciesAndIndexes() {
        configure();

        MapConfig annotationConfig = config.getMapConfigs().get(ANNOTATIONS_MAP);
        assertEquals(3600, annotationConfig.getTimeToLiveSeconds());
        assertEquals(0, annotationConfig.getMaxIdleSeconds());
        assertTrue(annotationConfig.getMapStoreConfig().isEnabled());
        assertSame(mapStore, annotationConfig.getMapStoreConfig().getImplementation());
        assertEquals(1, annotationConfig.getEntryListenerConfigs().size());
        EntryListenerConfig listenerConfig = annotationConfig.getEntryListenerConfigs().iterator().next();
        assertSame(listener, listenerConfig.getImplementation());
        assertTrue(listenerConfig.isLocal());
        assertFalse(listenerConfig.isIncludeValue());
        assertIndexes(annotationConfig);

        MapConfig fetchConfig = config.getMapConfigs().get(FETCH_MAP);
        assertEquals(300, fetchConfig.getTimeToLiveSeconds());
        assertEquals(0, fetchConfig.getMaxIdleSeconds());
        assertFalse(fetchConfig.getMapStoreConfig().isEnabled());
        assertTrue(fetchConfig.getEntryListenerConfigs().isEmpty());
        assertIndexes(fetchConfig);
        assertEquals(2, config.getMapConfigs().size());
        assertFalse(config.getMapConfigs().containsKey(ANNOTATIONS_MAP + "*"));
    }

    /** Checks that configuring maps twice does not duplicate indexes or listeners. */
    @Test
    void reconfiguringMapsDoesNotDuplicateIndexesOrAnnotationListener() {
        configure();
        configure();

        assertIndexes(config.getMapConfigs().get(ANNOTATIONS_MAP));
        assertIndexes(config.getMapConfigs().get(FETCH_MAP));
        assertEquals(1, config.getMapConfigs().get(ANNOTATIONS_MAP).getEntryListenerConfigs().size());
    }

    /** Checks that configured cache and fetch TTL values are applied. */
    @Test
    void configuresCustomAnnotationAndFetchTtls() {
        properties.setMaxCacheAge(Duration.ofMinutes(30));
        properties.setMaxFetchAge(Duration.ofSeconds(45));

        configure();

        assertEquals(1800, config.getMapConfigs().get(ANNOTATIONS_MAP).getTimeToLiveSeconds());
        assertEquals(45, config.getMapConfigs().get(FETCH_MAP).getTimeToLiveSeconds());
    }

    /** Checks that null, zero, and negative TTL values are rejected. */
    @Test
    void rejectsNullZeroAndNegativeDurations() {
        properties.setMaxCacheAge(null);
        assertThrows(IllegalStateException.class, this::configure);

        properties.setMaxCacheAge(Duration.ZERO);
        assertThrows(IllegalStateException.class, this::configure);

        properties.setMaxCacheAge(Duration.ofSeconds(-1));
        assertThrows(IllegalStateException.class, this::configure);

        properties.setMaxCacheAge(Duration.ofHours(1));
        properties.setMaxFetchAge(null);
        assertThrows(IllegalStateException.class, this::configure);
    }

    /** Checks that fractional-second and too-large TTL values are rejected. */
    @Test
    void rejectsSubsecondAndExcessiveDurations() {
        properties.setMaxCacheAge(Duration.ofMillis(1500));
        assertThrows(IllegalStateException.class, this::configure);

        properties.setMaxCacheAge(Duration.ofSeconds((long) Integer.MAX_VALUE + 1));
        assertThrows(IllegalStateException.class, this::configure);
    }

    /** Checks that the cache TTL cannot be shorter than the fetch TTL. */
    @Test
    void rejectsCacheAgeShorterThanFetchAge() {
        properties.setMaxCacheAge(Duration.ofMinutes(4));
        properties.setMaxFetchAge(Duration.ofMinutes(5));

        assertThrows(IllegalStateException.class, this::configure);
    }

    /** Checks that cache and fetch TTLs may be equal. */
    @Test
    void permitsEqualCacheAndFetchAges() {
        properties.setMaxCacheAge(Duration.ofMinutes(5));
        properties.setMaxFetchAge(Duration.ofMinutes(5));

        configure();

        assertEquals(300, config.getMapConfigs().get(ANNOTATIONS_MAP).getTimeToLiveSeconds());
        assertEquals(300, config.getMapConfigs().get(FETCH_MAP).getTimeToLiveSeconds());
    }

    private void assertIndexes(MapConfig mapConfig) {
        assertEquals(2, mapConfig.getIndexConfigs().size());
        assertTrue(mapConfig.getIndexConfigs().stream().anyMatch(index -> hashIndex(index, ID_TYPE_KEY_ATTRIBUTE, DOCUMENT_ID_KEY_ATTRIBUTE)));
        assertTrue(mapConfig.getIndexConfigs().stream().anyMatch(index -> hashIndex(index, DOCUMENT_ID_KEY_ATTRIBUTE)));
    }

    private boolean hashIndex(IndexConfig index, String... attributes) {
        return index.getType() == IndexType.HASH && index.getAttributes().equals(Arrays.asList(attributes));
    }

    private void configure() {
        new AnnotationCacheConfiguration(properties).configureMaps(config, mapStore, listener);
    }
}
