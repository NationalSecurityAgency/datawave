package datawave.microservice.annotationCache;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import datawave.microservice.annotationCache.config.AnnotationCacheProperties;

class AnnotationMapCleanupTest {
    private static final String CACHE_KEY = "UUID:document";

    private HazelcastInstance hazelcastInstance;

    @AfterEach
    void tearDown() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Test
    void destroysAnnotationAndFetchMapsAfterGracePeriod() {
        hazelcastInstance = newHazelcastInstance();
        String annotationMapName = ANNOTATIONS_MAP + CACHE_KEY;
        String fetchMapName = FETCH_MAP + CACHE_KEY;
        hazelcastInstance.getMap(annotationMapName);
        hazelcastInstance.getMap(fetchMapName);
        AnnotationMapCleanup cleanup = cleanup(true);

        cleanup.cleanEmptyMaps();
        cleanup.cleanEmptyMaps();

        assertThat(distributedObjectExists(annotationMapName)).isFalse();
        assertThat(distributedObjectExists(fetchMapName)).isFalse();
    }

    @Test
    void doesNotDestroyNonEmptyMap() {
        hazelcastInstance = newHazelcastInstance();
        String mapName = ANNOTATIONS_MAP + CACHE_KEY;
        IMap<String,String> map = hazelcastInstance.getMap(mapName);
        map.put("annotation", "value");
        AnnotationMapCleanup cleanup = cleanup(true);

        cleanup.cleanEmptyMaps();
        cleanup.cleanEmptyMaps();

        assertThat(distributedObjectExists(mapName)).isTrue();
        assertThat(map.get("annotation")).isEqualTo("value");
    }

    @Test
    void documentOperationRestartsEmptyGracePeriod() {
        hazelcastInstance = newHazelcastInstance();
        String mapName = ANNOTATIONS_MAP + CACHE_KEY;
        hazelcastInstance.getMap(mapName);
        AnnotationMapCleanup cleanup = cleanup(true);

        cleanup.cleanEmptyMaps();
        cleanup.withDocumentLock(CACHE_KEY, () -> null);
        cleanup.cleanEmptyMaps();

        assertThat(distributedObjectExists(mapName)).isTrue();

        cleanup.cleanEmptyMaps();

        assertThat(distributedObjectExists(mapName)).isFalse();
    }

    @Test
    void leavesEmptyMapsWhenCleanupIsDisabled() {
        hazelcastInstance = newHazelcastInstance();
        String mapName = ANNOTATIONS_MAP + CACHE_KEY;
        hazelcastInstance.getMap(mapName);
        AnnotationMapCleanup cleanup = cleanup(false);

        cleanup.cleanEmptyMaps();
        cleanup.cleanEmptyMaps();

        assertThat(distributedObjectExists(mapName)).isTrue();
    }

    @Test
    void defaultsEmptyMapGracePeriodToThirtyMinutes() {
        AnnotationCacheProperties properties = new AnnotationCacheProperties();

        assertThat(properties.getEmptyMapGracePeriod()).isEqualTo(Duration.ofMinutes(30));
    }

    private AnnotationMapCleanup cleanup(boolean enabled) {
        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setMapCleanupEnabled(enabled);
        properties.setEmptyMapGracePeriod(Duration.ZERO);
        properties.setCleanupLockWait(Duration.ofSeconds(1));
        return new AnnotationMapCleanup(hazelcastInstance, properties);
    }

    private boolean distributedObjectExists(String mapName) {
        return hazelcastInstance.getDistributedObjects().stream().map(DistributedObject::getName).anyMatch(mapName::equals);
    }

    private HazelcastInstance newHazelcastInstance() {
        Config config = new Config();
        config.setClusterName("annotation-map-cleanup-" + UUID.randomUUID());
        config.setProperty("hazelcast.logging.type", "none");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(true);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        return Hazelcast.newHazelcastInstance(config);
    }
}
