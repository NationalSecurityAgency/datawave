package datawave.microservice.annotationCache;

import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.PartitionLostEvent;

import datawave.microservice.annotationCache.config.AnnotationCacheProperties;

class AnnotationCacheTopologyListenerTest {
    private HazelcastInstance hazelcastInstance;
    private AnnotationCacheTopologyListener listener;

    @AfterEach
    void tearDown() {
        if (listener != null) {
            listener.close();
        }
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Test
    void partitionLossInvalidatesAllTrackedFetchMaps() {
        hazelcastInstance = newHazelcastInstance();
        IMap<String,String> firstFetch = hazelcastInstance.getMap(FETCH_MAP + "UUID:first");
        IMap<String,String> secondFetch = hazelcastInstance.getMap(FETCH_MAP + "PAGE_ID:second");
        firstFetch.put("auth", "record");
        secondFetch.put("auth", "record");

        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setTopologySettleDelay(Duration.ZERO);
        listener = new AnnotationCacheTopologyListener(hazelcastInstance, properties);
        PartitionLostEvent event = mock(PartitionLostEvent.class);
        when(event.getPartitionId()).thenReturn(1);

        listener.partitionLost(event);
        listener.poll();

        assertTrue(firstFetch.isEmpty());
        assertTrue(secondFetch.isEmpty());
    }

    @Test
    void tracksFetchMapsCreatedAfterListenerRegistration() {
        hazelcastInstance = newHazelcastInstance();
        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setTopologySettleDelay(Duration.ZERO);
        listener = new AnnotationCacheTopologyListener(hazelcastInstance, properties);

        IMap<String,String> fetch = hazelcastInstance.getMap(FETCH_MAP + "UUID:late");
        fetch.put("auth", "record");
        PartitionLostEvent event = mock(PartitionLostEvent.class);
        when(event.getPartitionId()).thenReturn(2);

        listener.partitionLost(event);
        listener.poll();

        assertTrue(fetch.isEmpty());
    }

    @Test
    void validatesTopologyTimingSettings() {
        hazelcastInstance = newHazelcastInstance();
        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setTopologySettleDelay(Duration.ofNanos(-1));
        assertThrows(IllegalStateException.class, () -> new AnnotationCacheTopologyListener(hazelcastInstance, properties));

        properties.setTopologySettleDelay(Duration.ZERO);
        properties.setTopologyPollIntervalMs(0);
        assertThrows(IllegalStateException.class, () -> new AnnotationCacheTopologyListener(hazelcastInstance, properties));
    }

    private HazelcastInstance newHazelcastInstance() {
        Config config = new Config();
        config.setClusterName("annotation-topology-" + UUID.randomUUID());
        config.setProperty("hazelcast.logging.type", "none");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        return Hazelcast.newHazelcastInstance(config);
    }
}
