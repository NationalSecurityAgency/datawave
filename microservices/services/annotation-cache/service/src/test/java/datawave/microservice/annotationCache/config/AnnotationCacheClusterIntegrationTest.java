package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.microservice.annotationCache.AnnotationMapStore;
import datawave.microservice.annotationCache.AnnotationSyncListener;
import datawave.microservice.annotationCache.api.AnnotationKey;
import datawave.microservice.annotationCache.api.FetchKey;
import datawave.microservice.annotationCache.api.FetchRecord;

/** Verifies shared-map removal invalidation across two real Hazelcast members. */
class AnnotationCacheClusterIntegrationTest {
    private static final AnnotationKey UUID_DOCUMENT_KEY = new AnnotationKey("UUID", "document", "annotation");
    private final List<HazelcastInstance> instances = new ArrayList<>();
    private HazelcastInstance client;

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.shutdown();
        }
        for (HazelcastInstance instance : instances) {
            instance.shutdown();
        }
    }

    @Test
    void ownerMemberInvalidatesOnlyMatchingDocumentFetchRecordsAfterAnnotationRemoval() throws IOException, InterruptedException {
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

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(clusterName);
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:" + ports[0], "127.0.0.1:" + ports[1]);
        client = HazelcastClient.newHazelcastClient(clientConfig);

        IMap<AnnotationKey,AnnotationMessage> clientAnnotations = client.getMap(ANNOTATIONS_MAP);
        IMap<FetchKey,FetchRecord> clientFetchRecords = client.getMap(FETCH_MAP);
        IMap<AnnotationKey,AnnotationMessage> firstAnnotations = first.getMap(ANNOTATIONS_MAP);
        IMap<AnnotationKey,AnnotationMessage> secondAnnotations = second.getMap(ANNOTATIONS_MAP);
        IMap<FetchKey,FetchRecord> fetchRecords = second.getMap(FETCH_MAP);

        AnnotationKey delimiterKey = new AnnotationKey("UUID|alternate", "document|part", "annotation");
        AnnotationMessage delimiterValue = annotationMessage();
        clientAnnotations.set(delimiterKey, delimiterValue);
        assertEquals(delimiterValue, secondAnnotations.get(delimiterKey), "client writes must serialize keys and values for member reads");
        assertEquals(delimiterKey, secondAnnotations.keySet().stream().filter(delimiterKey::equals).findFirst().orElseThrow(),
                        "keys must retain value equality after wire deserialization");
        AnnotationKey memberCreatedKey = new AnnotationKey("UUID", "member-write-doc", "member-write-annotation");
        AnnotationMessage memberCreatedValue = annotationMessage();
        secondAnnotations.put(memberCreatedKey, memberCreatedValue);
        assertEquals(memberCreatedValue, clientAnnotations.get(memberCreatedKey), "member values must serialize back to a real client");

        clientAnnotations.set(new AnnotationKey("UUID", "other-document", "annotation"), annotationMessage());
        clientAnnotations.set(new AnnotationKey("PAGE_ID", "document", "annotation"), annotationMessage());
        clientAnnotations.set(UUID_DOCUMENT_KEY, annotationMessage());
        await("annotation to be visible from both members",
                        () -> firstAnnotations.containsKey(UUID_DOCUMENT_KEY) && secondAnnotations.containsKey(UUID_DOCUMENT_KEY));

        FetchKey affected = fetchKey("UUID", "document", "auth-a");
        clientFetchRecords.set(affected, new FetchRecord(System.currentTimeMillis(), 1), 30, TimeUnit.SECONDS);
        clientFetchRecords.set(fetchKey("UUID", "document", "auth-b"), new FetchRecord(System.currentTimeMillis(), 0), 30, TimeUnit.SECONDS);
        FetchKey otherType = fetchKey("PAGE_ID", "document", "auth-a");
        FetchKey otherDocument = fetchKey("UUID", "other-document", "auth-a");
        clientFetchRecords.set(otherType, new FetchRecord(System.currentTimeMillis(), 1), 30, TimeUnit.SECONDS);
        clientFetchRecords.set(otherDocument, new FetchRecord(System.currentTimeMillis(), 1), 30, TimeUnit.SECONDS);
        clientFetchRecords.set(fetchKey("UUID|alternate", "document|part", "auth|one"), new FetchRecord(System.currentTimeMillis(), 2), 30, TimeUnit.SECONDS);
        FetchKey memberFetchKey = fetchKey("UUID|alternate", "document|part", "auth|one");
        fetchRecords.put(memberFetchKey, new FetchRecord(123456789L, 4));
        assertEquals(123456789L, clientFetchRecords.get(memberFetchKey).getFetchedAt());
        assertEquals(4, clientFetchRecords.get(memberFetchKey).getAnnotationCountReturned());
        assertEquals(1, fetchRecords.get(affected).getAnnotationCountReturned());
        long fetchedAt = fetchRecords.get(affected).getFetchedAt();
        assertTrue(Math.abs(System.currentTimeMillis() - fetchedAt) < TimeUnit.SECONDS.toMillis(5), "fetch DTO must retain its serialized timestamp");
        assertEquals(1, clientAnnotations.keySet(Predicates.and(Predicates.equal("__key.idType", "UUID"), Predicates.equal("__key.documentId", "document")))
                        .size());
        assertEquals(2, clientAnnotations.keySet(Predicates.equal("__key.documentId", "document")).size());
        assertEquals(1, clientAnnotations.keySet(Predicates.equal("__key.documentId", "document|part")).size());
        long indexedQueries = firstAnnotations.getLocalMapStats().getIndexedQueryCount() + secondAnnotations.getLocalMapStats().getIndexedQueryCount();
        assertTrue(indexedQueries > 0, "member map statistics should record use of configured key indexes");

        AnnotationKey removedByPredicate = new AnnotationKey("UUID", "remove-all-doc", "removed");
        clientAnnotations.set(removedByPredicate, annotationMessage());
        FetchKey predicateMarker = fetchKey("UUID", "remove-all-doc", "auth");
        clientFetchRecords.set(predicateMarker, new FetchRecord(System.currentTimeMillis(), 1), 30, TimeUnit.SECONDS);
        clientAnnotations.removeAll(Predicates.and(Predicates.equal("__key.idType", "UUID"), Predicates.equal("__key.documentId", "remove-all-doc")));
        await("removeAll to invalidate marker for the exact document", () -> !fetchRecords.containsKey(predicateMarker));
        assertNull(secondAnnotations.get(removedByPredicate));

        assertNotNull(clientAnnotations.remove(UUID_DOCUMENT_KEY));
        await("annotation removal to be visible from both members",
                        () -> firstAnnotations.get(UUID_DOCUMENT_KEY) == null && secondAnnotations.get(UUID_DOCUMENT_KEY) == null);
        await("matching document fetch records to be invalidated",
                        () -> !fetchRecords.containsKey(affected) && !fetchRecords.containsKey(fetchKey("UUID", "document", "auth-b")));

        assertEquals(3, fetchRecords.size(), "other identifier types, documents and delimiter identities must remain fresh");
        assertEquals(2, firstListener.removedCount.get() + secondListener.removedCount.get(),
                        "each client-originated removal should be processed by one member-local listener");
        assertTrue(clientAnnotations.containsKey(delimiterKey), "removing one document must preserve delimiter-containing other identities");
        assertNull(firstAnnotations.get(UUID_DOCUMENT_KEY));
        assertNull(secondAnnotations.get(UUID_DOCUMENT_KEY));
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

    private FetchKey fetchKey(String idType, String documentId, String authHash) {
        return new FetchKey(idType, documentId, authHash);
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
        public void entryRemoved(EntryEvent<AnnotationKey,Object> event) {
            removedCount.incrementAndGet();
            super.entryRemoved(event);
        }
    }
}
