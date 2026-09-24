package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.DOC_ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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

/** Exercises local listener registration and synchronization across two real Hazelcast members. */
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

    @Test
    void ownerMemberSynchronizesIndexAndExpirationForTheCluster() throws IOException, InterruptedException {
        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setMaxCacheAge(Duration.ofSeconds(2));
        properties.setMaxFetchAge(Duration.ofSeconds(1));

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
        IMap<String,Set<String>> firstIndex = first.getMap(DOC_ANNOTATIONS_MAP);
        IMap<String,Set<String>> secondIndex = second.getMap(DOC_ANNOTATIONS_MAP);
        IMap<String,String> fetchRecords = second.getMap(FETCH_MAP + CACHE_KEY);

        firstAnnotations.set("annotation", annotationMessage());
        await("the owner listener to update the distributed index",
                        () -> Set.of("annotation").equals(firstIndex.get(CACHE_KEY)) && Set.of("annotation").equals(secondIndex.get(CACHE_KEY)));
        assertEquals(1, firstListener.addedCount.get() + secondListener.addedCount.get(), "a local listener should process the add on only the owner member");

        fetchRecords.set("long-lived", "record", 30, TimeUnit.SECONDS);
        await("the annotation to expire on both members", () -> firstAnnotations.get("annotation") == null && secondAnnotations.get("annotation") == null);
        await("the owner listener to synchronize expiration across the cluster",
                        () -> firstIndex.get(CACHE_KEY) == null && secondIndex.get(CACHE_KEY) == null && fetchRecords.isEmpty());

        assertEquals(1, firstListener.expiredCount.get() + secondListener.expiredCount.get(),
                        "a local listener should process expiration on only the owner member");
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
        private final AtomicInteger addedCount = new AtomicInteger();
        private final AtomicInteger expiredCount = new AtomicInteger();

        @Override
        public void entryAdded(EntryEvent<String,Object> event) {
            addedCount.incrementAndGet();
            super.entryAdded(event);
        }

        @Override
        public void entryExpired(EntryEvent<String,Object> event) {
            expiredCount.incrementAndGet();
            super.entryExpired(event);
        }
    }
}
