package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.DOCUMENT_ID_KEY_ATTRIBUTE;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static datawave.microservice.annotationCache.api.Constants.ID_TYPE_KEY_ATTRIBUTE;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.XmlConfigBuilder;

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

    /** Checks the fixed shared map configurations, their policies, listeners, and indexes. */
    @Test
    void configuresSharedMapsWithRetentionPersistenceListenersAndIndexes() {
        configure();

        MapConfig annotationConfig = config.getMapConfigs().get(ANNOTATIONS_MAP);
        assertEquals(3600, annotationConfig.getTimeToLiveSeconds());
        assertEquals(0, annotationConfig.getMaxIdleSeconds());
        assertTrue(annotationConfig.getMapStoreConfig().isEnabled());
        assertSame(mapStore, annotationConfig.getMapStoreConfig().getImplementation());
        assertEquals(0, annotationConfig.getMapStoreConfig().getWriteDelaySeconds());
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
        assertFalse(config.getMapConfigs().containsKey(ANNOTATIONS_MAP + "*"));
    }

    /** Warns when non-default Hazelcast settings are overridden, but not for built-in defaults. */
    @Test
    void warnsWhenConfiguredHazelcastSettingsAreOverridden() {
        Logger logger = (Logger) LogManager.getLogger(AnnotationCacheConfiguration.class);
        List<String> warnings = new ArrayList<>();
        AbstractAppender appender = new AbstractAppender("annotation-cache-config-test", null, null, false, Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                if (event.getLevel() == Level.WARN) {
                    warnings.add(event.getMessage().getFormattedMessage());
                }
            }
        };
        appender.start();
        logger.addAppender(appender);
        try {
            configure();
            assertTrue(warnings.isEmpty(), "Hazelcast built-in defaults should not be reported as overridden");

            warnings.clear();
            config = new Config();
            config.addMapConfig(new MapConfig(ANNOTATIONS_MAP).setTimeToLiveSeconds(120).setMaxIdleSeconds(10));
            config.getMapConfigs().get(ANNOTATIONS_MAP).getMapStoreConfig().setWriteDelaySeconds(2);
            config.addMapConfig(new MapConfig(FETCH_MAP).setTimeToLiveSeconds(60).setMaxIdleSeconds(5));

            configure();

            assertEquals(5, warnings.size());
            assertTrue(warnings.stream().anyMatch(warning -> warning.contains("timeToLiveSeconds=120") && warning.contains("map annotations")));
            assertTrue(warnings.stream().anyMatch(warning -> warning.contains("maxIdleSeconds=10") && warning.contains("map annotations")));
            assertTrue(warnings.stream().anyMatch(warning -> warning.contains("mapStore.writeDelaySeconds=2") && warning.contains("map annotations")));
            assertTrue(warnings.stream().anyMatch(warning -> warning.contains("timeToLiveSeconds=60") && warning.contains("map doc-fetch-record")));
            assertTrue(warnings.stream().anyMatch(warning -> warning.contains("maxIdleSeconds=5") && warning.contains("map doc-fetch-record")));
        } finally {
            logger.removeAppender(appender);
            appender.stop();
        }
    }

    /** A configured MapStore write-behind delay must not defer annotation publication. */
    @Test
    void forcesWriteThroughWhenAnnotationMapHasConfiguredWriteDelay() {
        MapConfig annotationConfig = new MapConfig(ANNOTATIONS_MAP);
        annotationConfig.getMapStoreConfig().setWriteDelaySeconds(2);
        config.addMapConfig(annotationConfig);

        configure();

        assertEquals(0, config.getMapConfigs().get(ANNOTATIONS_MAP).getMapStoreConfig().getWriteDelaySeconds());
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

    /** Verifies that materializing the shared maps preserves policies inherited from the default map configuration. */
    @Test
    void preservesDefaultMapBackupAndEvictionPolicies() {
        MapConfig defaultConfig = mapWithPolicy("default", 2, EvictionPolicy.LRU, 100);
        config.addMapConfig(defaultConfig);

        // Establish that Hazelcast resolves both names through this policy before the service configures its exact maps.
        assertFalse(config.getMapConfigs().containsKey(ANNOTATIONS_MAP));
        assertFalse(config.getMapConfigs().containsKey(FETCH_MAP));
        assertMapPolicy(defaultConfig, config.findMapConfig(ANNOTATIONS_MAP));
        assertMapPolicy(defaultConfig, config.findMapConfig(FETCH_MAP));

        configure();

        assertAll("Shared maps should retain their default backup and eviction policies",
                        () -> assertMapPolicy(defaultConfig, config.findMapConfig(ANNOTATIONS_MAP)),
                        () -> assertMapPolicy(defaultConfig, config.findMapConfig(FETCH_MAP)));
    }

    /** Verifies that a matching wildcard's policy takes precedence over the default and survives service setup. */
    @Test
    void preservesWildcardMapBackupAndEvictionPolicies() {
        config.addMapConfig(mapWithPolicy("default", 1, EvictionPolicy.LRU, 100));
        MapConfig annotationPattern = mapWithPolicy(ANNOTATIONS_MAP + "*", 2, EvictionPolicy.LFU, 200);
        MapConfig fetchPattern = mapWithPolicy(FETCH_MAP + "*", 2, EvictionPolicy.LFU, 300);
        config.addMapConfig(annotationPattern);
        config.addMapConfig(fetchPattern);

        // Use distinct sizes so a default policy cannot accidentally satisfy the wildcard assertions.
        assertFalse(config.getMapConfigs().containsKey(ANNOTATIONS_MAP));
        assertFalse(config.getMapConfigs().containsKey(FETCH_MAP));
        assertMapPolicy(annotationPattern, config.findMapConfig(ANNOTATIONS_MAP));
        assertMapPolicy(fetchPattern, config.findMapConfig(FETCH_MAP));

        configure();

        assertAll("Shared maps should retain their matching wildcard backup and eviction policies",
                        () -> assertMapPolicy(annotationPattern, config.findMapConfig(ANNOTATIONS_MAP)),
                        () -> assertMapPolicy(fetchPattern, config.findMapConfig(FETCH_MAP)));
    }

    /** Control case: policies provided under the exact shared map names should also survive service setup. */
    @Test
    void preservesExactMapBackupAndEvictionPolicies() {
        config.addMapConfig(mapWithPolicy("default", 1, EvictionPolicy.LRU, 100));
        config.addMapConfig(mapWithPolicy(ANNOTATIONS_MAP + "*", 1, EvictionPolicy.LRU, 150));
        config.addMapConfig(mapWithPolicy(FETCH_MAP + "*", 1, EvictionPolicy.LRU, 150));
        MapConfig annotationConfig = mapWithPolicy(ANNOTATIONS_MAP, 2, EvictionPolicy.LFU, 200);
        MapConfig fetchConfig = mapWithPolicy(FETCH_MAP, 2, EvictionPolicy.LFU, 300);
        config.addMapConfig(annotationConfig);
        config.addMapConfig(fetchConfig);

        configure();

        assertAll("Exact shared map configurations should retain their policies",
                        () -> assertSame(annotationConfig, config.getMapConfigs().get(ANNOTATIONS_MAP)),
                        () -> assertSame(fetchConfig, config.getMapConfigs().get(FETCH_MAP)),
                        () -> assertMapPolicy(mapWithPolicy(ANNOTATIONS_MAP, 2, EvictionPolicy.LFU, 200), config.findMapConfig(ANNOTATIONS_MAP)),
                        () -> assertMapPolicy(mapWithPolicy(FETCH_MAP, 2, EvictionPolicy.LFU, 300), config.findMapConfig(FETCH_MAP)));
    }

    /** Matching wildcard policies are inherited, while service-owned retention and persistence settings remain authoritative. */
    @Test
    void inheritsWildcardPoliciesButAppliesServiceOwnedSettings() {
        config.addMapConfig(mapWithPolicy("default", 1, EvictionPolicy.LRU, 100));
        MapConfig annotationPattern = mapWithPolicy(ANNOTATIONS_MAP + "*", 2, EvictionPolicy.LFU, 200).setTimeToLiveSeconds(111).setMaxIdleSeconds(11);
        MapConfig fetchPattern = mapWithPolicy(FETCH_MAP + "*", 2, EvictionPolicy.LFU, 300).setTimeToLiveSeconds(222).setMaxIdleSeconds(22);
        AnnotationMapStore wildcardStore = mock(AnnotationMapStore.class);
        annotationPattern.getMapStoreConfig().setEnabled(true).setImplementation(wildcardStore).setWriteDelaySeconds(2);
        config.addMapConfig(annotationPattern);
        config.addMapConfig(fetchPattern);

        // Before service setup, the specific wildcard policies win over the less-specific default.
        assertFalse(config.getMapConfigs().containsKey(ANNOTATIONS_MAP));
        assertFalse(config.getMapConfigs().containsKey(FETCH_MAP));
        assertMapPolicy(annotationPattern, config.findMapConfig(ANNOTATIONS_MAP));
        assertMapPolicy(fetchPattern, config.findMapConfig(FETCH_MAP));

        configure();

        MapConfig annotationConfig = config.getMapConfigs().get(ANNOTATIONS_MAP);
        MapConfig fetchConfig = config.getMapConfigs().get(FETCH_MAP);
        //  @formatter:off
        assertAll("Wildcard backup and eviction policies are inherited; service-owned settings override inherited values",
                        // General policies inherit from the matching wildcard.
                        () -> assertMapPolicy(annotationPattern, annotationConfig),
                        () -> assertMapPolicy(fetchPattern, fetchConfig),
                        // Service-controlled retention values replace wildcard TTL and max-idle values.
                        () -> assertEquals(3600, annotationConfig.getTimeToLiveSeconds(), "annotation TTL is service-controlled"),
                        () -> assertEquals(300, fetchConfig.getTimeToLiveSeconds(), "fetch TTL is service-controlled"),
                        () -> assertEquals(0, annotationConfig.getMaxIdleSeconds(), "annotation max-idle is disabled by the service"),
                        () -> assertEquals(0, fetchConfig.getMaxIdleSeconds(), "fetch max-idle is disabled by the service"),
                        // Annotation persistence is wired to the service's MapStore, not the wildcard's implementation.
                        () -> assertTrue(annotationConfig.getMapStoreConfig().isEnabled()),
                        () -> assertSame(mapStore, annotationConfig.getMapStoreConfig().getImplementation()),
                        () -> assertEquals(0, annotationConfig.getMapStoreConfig().getWriteDelaySeconds(), "write-through is forced"),
                        // Materializing exact configs must not rename or mutate the shared wildcard configs.
                        () -> assertSame(annotationPattern, config.getMapConfigs().get(ANNOTATIONS_MAP + "*")),
                        () -> assertSame(fetchPattern, config.getMapConfigs().get(FETCH_MAP + "*")),
                        () -> assertEquals(111, annotationPattern.getTimeToLiveSeconds()),
                        () -> assertEquals(222, fetchPattern.getTimeToLiveSeconds()));
        //  @formatter:on
    }

    /** Fetch markers inherit MapStore and listener settings from an explicit global default. */
    @Test
    void fetchMapInheritsMapStoreAndListenersFromGlobalDefault() {
        MapConfig globalDefault = new MapConfig("default");
        AnnotationMapStore defaultStore = mock(AnnotationMapStore.class);
        AnnotationSyncListener defaultListener = mock(AnnotationSyncListener.class);
        EntryListenerConfig defaultListenerConfig = new EntryListenerConfig(defaultListener, false, true);
        globalDefault.getMapStoreConfig().setEnabled(true).setImplementation(defaultStore);
        globalDefault.addEntryListenerConfig(defaultListenerConfig);
        config.addMapConfig(globalDefault);

        configure();

        assertInheritedStoreAndListener(config.getMapConfigs().get(FETCH_MAP), defaultStore, defaultListenerConfig);
    }

    /** A matching fetch-map wildcard's MapStore and listener settings are inherited instead of the global default's. */
    @Test
    void fetchMapInheritsMapStoreAndListenersFromWildcard() {
        MapConfig globalDefault = new MapConfig("default");
        globalDefault.getMapStoreConfig().setEnabled(true).setImplementation(mock(AnnotationMapStore.class));
        globalDefault.addEntryListenerConfig(new EntryListenerConfig(mock(AnnotationSyncListener.class), false, false));
        config.addMapConfig(globalDefault);

        MapConfig fetchPattern = new MapConfig(FETCH_MAP + "*");
        AnnotationMapStore wildcardStore = mock(AnnotationMapStore.class);
        AnnotationSyncListener wildcardListener = mock(AnnotationSyncListener.class);
        EntryListenerConfig wildcardListenerConfig = new EntryListenerConfig(wildcardListener, true, true);
        fetchPattern.getMapStoreConfig().setEnabled(true).setImplementation(wildcardStore);
        fetchPattern.addEntryListenerConfig(wildcardListenerConfig);
        config.addMapConfig(fetchPattern);

        configure();

        assertInheritedStoreAndListener(config.getMapConfigs().get(FETCH_MAP), wildcardStore, wildcardListenerConfig);
    }

    /** Verifies that an explicit global default is copied to both fixed maps when no wildcard or exact config matches. */
    @Test
    void inheritsGlobalDefaultWhenNoWildcardOrExactConfigExists() throws IOException {
        config = loadXmlConfig();
        MapConfig globalDefault = config.getMapConfigs().get("default");

        configure();

        MapConfig annotationConfig = config.getMapConfigs().get(ANNOTATIONS_MAP);
        MapConfig fetchConfig = config.getMapConfigs().get(FETCH_MAP);
        //  @formatter:off
        assertAll("Both maps inherit global backup and eviction settings while service-owned retention remains authoritative",
                        () -> assertMapPolicy(globalDefault, annotationConfig),
                        () -> assertMapPolicy(globalDefault, fetchConfig),
                        () -> assertEquals(3600, annotationConfig.getTimeToLiveSeconds()),
                        () -> assertEquals(300, fetchConfig.getTimeToLiveSeconds()),
                        () -> assertEquals(0, annotationConfig.getMaxIdleSeconds()),
                        () -> assertEquals(0, fetchConfig.getMaxIdleSeconds()),
                        () -> assertSame(globalDefault, config.getMapConfigs().get("default")));
        //  @formatter:on
    }

    /** A matching wildcard replaces the global default config; fields absent from the wildcard are not merged from default. */
    @Test
    void wildcardConfigReplacesRatherThanMergesGlobalDefault() {
        MapConfig globalDefault = mapWithPolicy("default", 2, EvictionPolicy.LRU, 100);
        MapConfig annotationPattern = new MapConfig(ANNOTATIONS_MAP + "*");
        annotationPattern.getEvictionConfig().setEvictionPolicy(EvictionPolicy.LFU).setMaxSizePolicy(MaxSizePolicy.PER_NODE).setSize(50);
        config.addMapConfig(globalDefault);
        config.addMapConfig(annotationPattern);

        // The annotation wildcard supplies only eviction settings; its backup count is Hazelcast's own default, not the global value.
        assertNotEquals(globalDefault.getBackupCount(), config.findMapConfig(ANNOTATIONS_MAP).getBackupCount());
        assertMapPolicy(globalDefault, config.findMapConfig(FETCH_MAP));

        configure();

        MapConfig annotationConfig = config.getMapConfigs().get(ANNOTATIONS_MAP);
        MapConfig fetchConfig = config.getMapConfigs().get(FETCH_MAP);
        //  @formatter:off
        assertAll("A wildcard config is selected as a whole rather than merged field-by-field with the global default",
                        () -> assertEquals(annotationPattern.getBackupCount(), annotationConfig.getBackupCount(), "wildcard's default backup count is used"),
                        () -> assertNotEquals(globalDefault.getBackupCount(), annotationConfig.getBackupCount()),
                        () -> assertEquals(EvictionPolicy.LFU, annotationConfig.getEvictionConfig().getEvictionPolicy()),
                        () -> assertEquals(50, annotationConfig.getEvictionConfig().getSize()),
                        () -> assertMapPolicy(globalDefault, fetchConfig));
        //  @formatter:on
    }

    /** Verifies that exact map settings take precedence over wildcard settings, which take precedence over global defaults. */
    @Test
    void selectsMostSpecificConfigOverWildcardAndGlobalDefault() throws IOException {
        config = loadXmlConfig();
        MapConfig defaultConfig = config.getMapConfigs().get("default");
        assertMapPolicy(mapWithPolicy("default", 1, EvictionPolicy.LRU, 100), defaultConfig);
        MapConfig annotationPattern = mapWithPolicy(ANNOTATIONS_MAP + "*", 2, EvictionPolicy.LFU, 200);
        MapConfig fetchPattern = mapWithPolicy(FETCH_MAP + "*", 2, EvictionPolicy.LFU, 300);
        config.addMapConfig(defaultConfig);
        config.addMapConfig(annotationPattern);
        config.addMapConfig(fetchPattern);

        // Without exact configs, the matching wildcard wins over the custom global default.
        assertMapPolicy(annotationPattern, config.findMapConfig(ANNOTATIONS_MAP));
        assertMapPolicy(fetchPattern, config.findMapConfig(FETCH_MAP));

        MapConfig annotationExact = mapWithPolicy(ANNOTATIONS_MAP, 3, EvictionPolicy.LRU, 400);
        MapConfig fetchExact = mapWithPolicy(FETCH_MAP, 3, EvictionPolicy.LRU, 500);
        config.addMapConfig(annotationExact);
        config.addMapConfig(fetchExact);

        // Once present, exact map settings win over both the wildcard and global default and survive service setup.
        assertMapPolicy(annotationExact, config.findMapConfig(ANNOTATIONS_MAP));
        assertMapPolicy(fetchExact, config.findMapConfig(FETCH_MAP));
        configure();

        //  @formatter:off
        assertAll("Exact settings override wildcard and global defaults",
                        () -> assertMapPolicy(annotationExact, config.findMapConfig(ANNOTATIONS_MAP)),
                        () -> assertMapPolicy(fetchExact, config.findMapConfig(FETCH_MAP)),
                        () -> assertSame(defaultConfig, config.getMapConfigs().get("default")),
                        () -> assertSame(annotationPattern, config.getMapConfigs().get(ANNOTATIONS_MAP + "*")),
                        () -> assertSame(fetchPattern, config.getMapConfigs().get(FETCH_MAP + "*")));
        //  @formatter:on
    }

    /** Exact map policies survive setup while service-owned retention and persistence settings remain authoritative. */
    @Test
    void preservesExactPoliciesAndAppliesServiceOwnedSettings() {
        config.addMapConfig(mapWithPolicy("default", 1, EvictionPolicy.LRU, 100));
        config.addMapConfig(mapWithPolicy(ANNOTATIONS_MAP + "*", 1, EvictionPolicy.LRU, 150));
        config.addMapConfig(mapWithPolicy(FETCH_MAP + "*", 1, EvictionPolicy.LRU, 150));
        MapConfig annotationConfig = mapWithPolicy(ANNOTATIONS_MAP, 2, EvictionPolicy.LFU, 200).setTimeToLiveSeconds(111).setMaxIdleSeconds(11);
        MapConfig fetchConfig = mapWithPolicy(FETCH_MAP, 2, EvictionPolicy.LFU, 300).setTimeToLiveSeconds(222).setMaxIdleSeconds(22);
        AnnotationMapStore exactStore = mock(AnnotationMapStore.class);
        annotationConfig.getMapStoreConfig().setEnabled(true).setImplementation(exactStore);
        config.addMapConfig(annotationConfig);
        config.addMapConfig(fetchConfig);

        configure();

        //  @formatter:off
        assertAll("Exact map policies survive while service-owned settings are applied",
                        () -> assertSame(annotationConfig, config.getMapConfigs().get(ANNOTATIONS_MAP)),
                        () -> assertSame(fetchConfig, config.getMapConfigs().get(FETCH_MAP)),
                        () -> assertMapPolicy(mapWithPolicy(ANNOTATIONS_MAP, 2, EvictionPolicy.LFU, 200), config.findMapConfig(ANNOTATIONS_MAP)),
                        () -> assertMapPolicy(mapWithPolicy(FETCH_MAP, 2, EvictionPolicy.LFU, 300), config.findMapConfig(FETCH_MAP)),
                        () -> assertEquals(3600, annotationConfig.getTimeToLiveSeconds()),
                        () -> assertEquals(300, fetchConfig.getTimeToLiveSeconds()),
                        () -> assertEquals(0, annotationConfig.getMaxIdleSeconds()),
                        () -> assertEquals(0, fetchConfig.getMaxIdleSeconds()),
                        () -> assertTrue(annotationConfig.getMapStoreConfig().isEnabled()),
                        () -> assertSame(mapStore, annotationConfig.getMapStoreConfig().getImplementation()));
        //  @formatter:on
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

        properties.setMaxFetchAge(Duration.ZERO);
        assertThrows(IllegalStateException.class, this::configure);

        properties.setMaxFetchAge(Duration.ofSeconds(-1));
        assertThrows(IllegalStateException.class, this::configure);
    }

    /** Checks that fractional-second and too-large TTL values are rejected. */
    @Test
    void rejectsSubsecondAndExcessiveMaxCacheAge() {
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

    private void assertInheritedStoreAndListener(MapConfig actual, AnnotationMapStore expectedStore, EntryListenerConfig expectedListener) {
        assertTrue(actual.getMapStoreConfig().isEnabled());
        assertSame(expectedStore, actual.getMapStoreConfig().getImplementation());
        assertEquals(1, actual.getEntryListenerConfigs().size());
        EntryListenerConfig actualListener = actual.getEntryListenerConfigs().iterator().next();
        assertSame(expectedListener.getImplementation(), actualListener.getImplementation());
        assertEquals(expectedListener.isLocal(), actualListener.isLocal());
        assertEquals(expectedListener.isIncludeValue(), actualListener.isIncludeValue());
    }

    private Config loadXmlConfig() throws IOException {
        InputStream xmlConfig = getClass().getResourceAsStream("/config/annotation-cache-default-map.xml");
        assertNotNull(xmlConfig, "test Hazelcast XML config should be present");
        try (xmlConfig) {
            return new XmlConfigBuilder(xmlConfig).build();
        }
    }

    private MapConfig mapWithPolicy(String name, int backupCount, EvictionPolicy evictionPolicy, int size) {
        MapConfig mapConfig = new MapConfig(name).setBackupCount(backupCount);
        mapConfig.getEvictionConfig().setEvictionPolicy(evictionPolicy).setMaxSizePolicy(MaxSizePolicy.PER_NODE).setSize(size);
        return mapConfig;
    }

    private void assertMapPolicy(MapConfig expected, MapConfig actual) {
        //  @formatter:off
        assertAll("Map policy for " + actual.getName(),
                        () -> assertEquals(expected.getBackupCount(), actual.getBackupCount(), "backup count"),
                        () -> assertEquals(expected.getEvictionConfig().getEvictionPolicy(), actual.getEvictionConfig().getEvictionPolicy(), "eviction policy"),
                        () -> assertEquals(expected.getEvictionConfig().getMaxSizePolicy(), actual.getEvictionConfig().getMaxSizePolicy(), "maximum size policy"),
                        () -> assertEquals(expected.getEvictionConfig().getSize(), actual.getEvictionConfig().getSize(), "eviction size limit"));
        //  @formatter:on
    }

    private boolean hashIndex(IndexConfig index, String... attributes) {
        return index.getType() == IndexType.HASH && index.getAttributes().equals(Arrays.asList(attributes));
    }

    private void configure() {
        new AnnotationCacheConfiguration(properties).configureMaps(config, mapStore, listener);
    }
}
