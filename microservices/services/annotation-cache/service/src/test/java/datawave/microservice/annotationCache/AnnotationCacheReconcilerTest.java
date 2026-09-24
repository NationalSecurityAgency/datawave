package datawave.microservice.annotationCache;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.DOC_ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.PartitionLostEvent;

import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.microservice.annotationCache.config.AnnotationCacheProperties;

class AnnotationCacheReconcilerTest {
    private HazelcastInstance hazelcastInstance;
    private AnnotationCacheReconciler reconciler;

    @AfterEach
    void tearDown() {
        if (reconciler != null) {
            reconciler.close();
        }
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Test
    void repairsMissingIdsWithoutRemovingStaleIdsAndInvalidatesFetchRecords() {
        hazelcastInstance = newHazelcastInstance();
        String cacheKey = "uuid:document";
        IMap<String,AnnotationMessage> annotations = hazelcastInstance.getMap(ANNOTATIONS_MAP + cacheKey);
        annotations.put("present", AnnotationMessage.getDefaultInstance());
        annotations.put("missing", AnnotationMessage.getDefaultInstance());

        IMap<String,Set<String>> index = hazelcastInstance.getMap(DOC_ANNOTATIONS_MAP);
        index.put(cacheKey, new HashSet<>(Set.of("present", "stale")));
        IMap<String,String> fetch = hazelcastInstance.getMap(FETCH_MAP + cacheKey);
        fetch.put("auth", "record");

        reconciler = new AnnotationCacheReconciler(hazelcastInstance, properties(1000));
        AnnotationCacheReconciler.ReconciliationResult result = reconciler.reconcileBatch(hazelcastInstance.getMap(AnnotationCacheReconciler.CONTROL_MAP));

        assertEquals(Set.of("present", "missing", "stale"), index.get(cacheKey));
        assertTrue(fetch.isEmpty());
        assertEquals(1, result.mapsScanned);
        assertEquals(1, result.missingIdsRepaired);
        assertEquals(1, result.staleIdsObserved);
        assertEquals(0, result.failures);
        assertTrue(result.cycleComplete);
    }

    @Test
    void boundsWorkAndContinuesFromDistributedCursor() {
        hazelcastInstance = newHazelcastInstance();
        hazelcastInstance.<String,AnnotationMessage> getMap(ANNOTATIONS_MAP + "uuid:first").put("first", AnnotationMessage.getDefaultInstance());
        hazelcastInstance.<String,AnnotationMessage> getMap(ANNOTATIONS_MAP + "uuid:second").put("second", AnnotationMessage.getDefaultInstance());

        reconciler = new AnnotationCacheReconciler(hazelcastInstance, properties(1));
        IMap<String,Long> control = hazelcastInstance.getMap(AnnotationCacheReconciler.CONTROL_MAP);

        AnnotationCacheReconciler.ReconciliationResult first = reconciler.reconcileBatch(control);
        AnnotationCacheReconciler.ReconciliationResult second = reconciler.reconcileBatch(control);

        assertEquals(1, first.mapsScanned);
        assertEquals(1, second.mapsScanned);
        assertTrue(!first.cycleComplete);
        assertTrue(second.cycleComplete);
        IMap<String,Set<String>> index = hazelcastInstance.getMap(DOC_ANNOTATIONS_MAP);
        assertEquals(Set.of("first"), index.get("uuid:first"));
        assertEquals(Set.of("second"), index.get("uuid:second"));
    }

    @Test
    void partitionLossRequestsGlobalFetchInvalidation() {
        hazelcastInstance = newHazelcastInstance();
        IMap<String,String> firstFetch = hazelcastInstance.getMap(FETCH_MAP + "uuid:first");
        IMap<String,String> secondFetch = hazelcastInstance.getMap(FETCH_MAP + "uuid:second");
        firstFetch.put("auth", "record");
        secondFetch.put("auth", "record");

        AnnotationCacheProperties properties = properties(1000);
        properties.setReconciliationSettleDelay(Duration.ZERO);
        reconciler = new AnnotationCacheReconciler(hazelcastInstance, properties);

        PartitionLostEvent event = mock(PartitionLostEvent.class);
        when(event.getPartitionId()).thenReturn(1);
        reconciler.partitionLost(event);
        reconciler.poll();

        assertTrue(firstFetch.isEmpty());
        assertTrue(secondFetch.isEmpty());
    }

    private HazelcastInstance newHazelcastInstance() {
        Config config = new Config();
        config.setClusterName("annotation-reconciliation-" + UUID.randomUUID());
        config.setProperty("hazelcast.logging.type", "none");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        return Hazelcast.newHazelcastInstance(config);
    }

    private AnnotationCacheProperties properties(int maxMaps) {
        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setReconciliationMaxMapsPerRun(maxMaps);
        return properties;
    }
}
