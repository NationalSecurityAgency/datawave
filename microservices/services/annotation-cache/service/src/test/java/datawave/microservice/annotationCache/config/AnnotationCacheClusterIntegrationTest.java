package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.microservice.annotationCache.AnnotationMapStore;
import datawave.microservice.annotationCache.AnnotationSyncListener;

/**
 * Verifies removal-listener execution and fetch invalidation across two real Hazelcast members. TTL durations are covered by the single-member expiration test.
 */
class AnnotationCacheClusterIntegrationTest {
    private static final String CACHE_KEY = "UUID:document";
    private static final String ANNOTATION_MAP = ANNOTATIONS_MAP + CACHE_KEY;

    private final List<HazelcastInstance> instances = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (HazelcastInstance instance : instances) {
            instance.shutdown();
        }
    }

    /**
     * Writes through one member, removes the entry there, and verifies that the local listener runs once and clears the fetch map cluster-wide. This isolates
     * cross-member event propagation from expiration timing.
     */
    @Test
    void ownerMemberInvalidatesFetchRecordsAfterAnnotationRemoval() throws IOException, InterruptedException {
        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setMaxCacheAge(Duration.ofSeconds(30));
        properties.setMaxFetchAge(Duration.ofSeconds(10));

        String clusterName = "annotation-cluster-test-" + UUID.randomUUID();
        int[] ports = availablePorts();
        CountingSyncListener firstListener = new CountingSyncListener();
        CountingSyncListener secondListener = new CountingSyncListener();

        Config firstConfig = clusterConfig(clusterName, ports[0], ports, properties, firstListener);
        Config secondConfig = clusterConfig(clusterName, ports[1], ports, properties, secondListener);

        HazelcastInstance first = Hazelcast.newHazelcastInstance(firstConfig);
        instances.add(first);
        firstListener.setHazelcastInstance(first);
        HazelcastInstance second = Hazelcast.newHazelcastInstance(secondConfig);
        instances.add(second);
        secondListener.setHazelcastInstance(second);

        await("both Hazelcast members to form one cluster", () -> first.getCluster().getMembers().size() == 2 && second.getCluster().getMembers().size() == 2);

        IMap<String,AnnotationMessage> firstAnnotations = first.getMap(ANNOTATION_MAP);
        IMap<String,AnnotationMessage> secondAnnotations = second.getMap(ANNOTATION_MAP);
        IMap<String,String> fetchRecords = second.getMap(FETCH_MAP + CACHE_KEY);

        firstAnnotations.set("annotation", annotationMessage());
        await("annotation to be visible from both members", () -> firstAnnotations.containsKey("annotation") && secondAnnotations.containsKey("annotation"));

        fetchRecords.set("long-lived", "record", 30, TimeUnit.SECONDS);
        assertNotNull(firstAnnotations.remove("annotation"));
        await("annotation removal to be visible from both members",
                        () -> firstAnnotations.get("annotation") == null && secondAnnotations.get("annotation") == null);
        await("the owner listener to invalidate fetch records", fetchRecords::isEmpty);

        assertEquals(1, firstListener.removedCount.get() + secondListener.removedCount.get(),
                        "a local listener should process removal on only the member that performed it");
        assertNull(firstAnnotations.get("annotation"));
        assertNull(secondAnnotations.get("annotation"));
    }

    private Config clusterConfig(String clusterName, int port, int[] memberPorts, AnnotationCacheProperties properties, AnnotationSyncListener listener) {
        Config config = new Config();
        config.setClusterName(clusterName);
        config.setProperty("hazelcast.logging.type", "none");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().setPort(port).setPortAutoIncrement(false);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true);
        for (int memberPort : memberPorts) {
            join.getTcpIpConfig().addMember("127.0.0.1:" + memberPort);
        }
        new AnnotationCacheConfiguration(properties).configureMaps(config, mock(AnnotationMapStore.class), listener);
        return config;
    }

    private int[] availablePorts() throws IOException {
        try (ServerSocket first = new ServerSocket(0); ServerSocket second = new ServerSocket(0)) {
            return new int[] {first.getLocalPort(), second.getLocalPort()};
        }
    }

    private AnnotationMessage annotationMessage() {
        Annotation annotation = Annotation.newBuilder().setDocumentId("document").setAnnotationId("annotation").build();
        return AnnotationMessage.newBuilder().addAnnotations(annotation).build();
    }

    private void await(String description, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25);
        }
        throw new AssertionError("Timed out waiting for " + description);
    }

    private static class CountingSyncListener extends AnnotationSyncListener {
        private final AtomicInteger removedCount = new AtomicInteger();

        @Override
        public void entryRemoved(EntryEvent<String,Object> event) {
            removedCount.incrementAndGet();
            super.entryRemoved(event);
        }
    }
}
