package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.DOC_ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;

import datawave.microservice.annotationCache.AnnotationMapStore;
import datawave.microservice.annotationCache.AnnotationSyncListener;

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

    @Test
    void configuresDefaultAnnotationAndFetchTtls() {
        configure();

        MapConfig annotationConfig = config.getMapConfig(ANNOTATIONS_MAP + "*");
        assertEquals(3600, annotationConfig.getTimeToLiveSeconds());
        assertEquals(0, annotationConfig.getMaxIdleSeconds());
        assertTrue(annotationConfig.getMapStoreConfig().isEnabled());
        assertSame(mapStore, annotationConfig.getMapStoreConfig().getImplementation());
        assertEquals(1, annotationConfig.getEntryListenerConfigs().size());
        EntryListenerConfig listenerConfig = annotationConfig.getEntryListenerConfigs().iterator().next();
        assertSame(listener, listenerConfig.getImplementation());
        assertTrue(listenerConfig.isLocal());
        assertTrue(listenerConfig.isIncludeValue());

        MapConfig fetchConfig = config.getMapConfig(FETCH_MAP + "*");
        assertEquals(300, fetchConfig.getTimeToLiveSeconds());
        assertEquals(0, fetchConfig.getMaxIdleSeconds());

        MapConfig documentIndexConfig = config.getMapConfig(DOC_ANNOTATIONS_MAP);
        assertEquals(0, documentIndexConfig.getTimeToLiveSeconds());
        assertEquals(0, documentIndexConfig.getMaxIdleSeconds());
        assertEquals(EvictionPolicy.NONE, documentIndexConfig.getEvictionConfig().getEvictionPolicy());
    }

    @Test
    void configuresCustomAnnotationAndFetchTtls() {
        properties.setMaxCacheAge(Duration.ofMinutes(30));
        properties.setMaxFetchAge(Duration.ofSeconds(45));

        configure();

        assertEquals(1800, config.getMapConfig(ANNOTATIONS_MAP + "*").getTimeToLiveSeconds());
        assertEquals(45, config.getMapConfig(FETCH_MAP + "*").getTimeToLiveSeconds());
    }

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

    @Test
    void rejectsSubsecondAndExcessiveDurations() {
        properties.setMaxCacheAge(Duration.ofMillis(1500));
        assertThrows(IllegalStateException.class, this::configure);

        properties.setMaxCacheAge(Duration.ofSeconds((long) Integer.MAX_VALUE + 1));
        assertThrows(IllegalStateException.class, this::configure);
    }

    @Test
    void rejectsCacheAgeShorterThanFetchAge() {
        properties.setMaxCacheAge(Duration.ofMinutes(4));
        properties.setMaxFetchAge(Duration.ofMinutes(5));

        assertThrows(IllegalStateException.class, this::configure);
    }

    @Test
    void permitsEqualCacheAndFetchAges() {
        properties.setMaxCacheAge(Duration.ofMinutes(5));
        properties.setMaxFetchAge(Duration.ofMinutes(5));

        configure();

        assertEquals(300, config.getMapConfig(ANNOTATIONS_MAP + "*").getTimeToLiveSeconds());
        assertEquals(300, config.getMapConfig(FETCH_MAP + "*").getTimeToLiveSeconds());
    }

    private void configure() {
        new AnnotationCacheConfiguration(properties).configureMaps(config, mapStore, listener);
    }
}
