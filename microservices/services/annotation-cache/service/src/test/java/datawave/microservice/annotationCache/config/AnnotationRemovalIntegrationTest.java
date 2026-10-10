package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.DOCUMENT_ID_KEY_ATTRIBUTE;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static datawave.microservice.annotationCache.api.Constants.ID_TYPE_KEY_ATTRIBUTE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
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

/** Exercises shared key indexes, targeted entry invalidation, and whole-map invalidation on a real Hazelcast member. */
class AnnotationRemovalIntegrationTest {
    private static final AnnotationKey UUID_DOCUMENT_FIRST = new AnnotationKey("UUID", "document", "first");
    private static final AnnotationKey UUID_DOCUMENT_SECOND = new AnnotationKey("UUID", "document", "second");
    private static final AnnotationKey PAGE_DOCUMENT = new AnnotationKey("PAGE_ID", "document", "third");
    private static final AnnotationKey UUID_OTHER_DOCUMENT = new AnnotationKey("UUID", "other-document", "fourth");

    private HazelcastInstance hazelcastInstance;

    @AfterEach
    void tearDown() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }

    @Test
    void entryRemovalAndEvictionInvalidateOnlyMatchingDocumentWhileMapEventsInvalidateAll() throws InterruptedException {
        IMap<AnnotationKey,AnnotationMessage> annotations = newConfiguredAnnotationMap();
        IMap<FetchKey,FetchRecord> fetchRecords = hazelcastInstance.getMap(FETCH_MAP);
        annotations.put(UUID_DOCUMENT_FIRST, annotationMessage("first"));
        annotations.put(UUID_DOCUMENT_SECOND, annotationMessage("second"));
        annotations.put(PAGE_DOCUMENT, annotationMessage("third"));
        annotations.put(UUID_OTHER_DOCUMENT, annotationMessage("fourth"));

        FetchKey affectedAuthA = fetchKey("UUID", "document", "auth-a");
        FetchKey affectedAuthB = fetchKey("UUID", "document", "auth-b");
        FetchKey otherType = fetchKey("PAGE_ID", "document", "auth-a");
        FetchKey otherDocument = fetchKey("UUID", "other-document", "auth-a");
        put(fetchRecords, affectedAuthA, affectedAuthB, otherType, otherDocument);

        assertTrue(annotations.remove(UUID_DOCUMENT_FIRST) != null);
        await("all authorization markers for only UUID/document to be removed",
                        () -> !fetchRecords.containsKey(affectedAuthA) && !fetchRecords.containsKey(affectedAuthB));
        assertTrue(fetchRecords.containsKey(otherType));
        assertTrue(fetchRecords.containsKey(otherDocument));
        assertTrue(annotations.containsKey(UUID_DOCUMENT_SECOND));

        fetchRecords.put(affectedAuthA, record(), 30, TimeUnit.SECONDS);
        fetchRecords.put(affectedAuthB, record(), 30, TimeUnit.SECONDS);
        assertTrue(annotations.evict(UUID_DOCUMENT_SECOND));
        await("eviction to remove all matching authorization markers",
                        () -> !fetchRecords.containsKey(affectedAuthA) && !fetchRecords.containsKey(affectedAuthB));
        assertTrue(fetchRecords.containsKey(otherType));
        assertTrue(fetchRecords.containsKey(otherDocument));

        fetchRecords.put(affectedAuthA, record(), 30, TimeUnit.SECONDS);
        annotations.clear();
        await("map clear to invalidate the entire fetch map", fetchRecords::isEmpty);
        assertTrue(annotations.isEmpty());

        annotations.put(UUID_DOCUMENT_FIRST, annotationMessage("first"));
        fetchRecords.put(otherType, record(), 30, TimeUnit.SECONDS);
        annotations.evictAll();
        await("map eviction to invalidate the entire fetch map", fetchRecords::isEmpty);
        assertTrue(annotations.isEmpty());
        assertTrue(fetchRecords.isEmpty());
    }

    @Test
    void keyIndexesReturnExactDocumentIdentitiesAndDocumentIdQueriesCrossIdentifierTypes() {
        IMap<AnnotationKey,AnnotationMessage> annotations = newConfiguredAnnotationMap();
        AnnotationKey samePrefixButOtherDocument = new AnnotationKey("UUID", "document-extra", "other");
        AnnotationKey pageOtherDocument = new AnnotationKey("PAGE_ID", "other-document", "fifth");
        annotations.put(UUID_DOCUMENT_FIRST, annotationMessage("first"));
        annotations.put(UUID_DOCUMENT_SECOND, annotationMessage("second"));
        annotations.put(PAGE_DOCUMENT, annotationMessage("third"));
        annotations.put(UUID_OTHER_DOCUMENT, annotationMessage("fourth"));
        annotations.put(samePrefixButOtherDocument, annotationMessage("other"));
        annotations.put(pageOtherDocument, annotationMessage("fifth"));

        Set<AnnotationKey> exactPair = annotations
                        .keySet(Predicates.and(Predicates.equal(ID_TYPE_KEY_ATTRIBUTE, "UUID"), Predicates.equal(DOCUMENT_ID_KEY_ATTRIBUTE, "document")));
        assertEquals(Set.of(UUID_DOCUMENT_FIRST, UUID_DOCUMENT_SECOND), exactPair);

        Set<AnnotationKey> documentAcrossTypes = annotations.keySet(Predicates.equal(DOCUMENT_ID_KEY_ATTRIBUTE, "document"));
        assertEquals(Set.of(UUID_DOCUMENT_FIRST, UUID_DOCUMENT_SECOND, PAGE_DOCUMENT), documentAcrossTypes);
        assertFalse(documentAcrossTypes.contains(samePrefixButOtherDocument), "documentId queries use exact equality, not substring matching");
    }

    private IMap<AnnotationKey,AnnotationMessage> newConfiguredAnnotationMap() {
        AnnotationCacheProperties properties = new AnnotationCacheProperties();
        properties.setMaxCacheAge(Duration.ofSeconds(30));
        properties.setMaxFetchAge(Duration.ofSeconds(10));

        AnnotationSyncListener listener = new AnnotationSyncListener();
        Config config = isolatedConfig();
        new AnnotationCacheConfiguration(properties).configureMaps(config, mock(AnnotationMapStore.class), listener);
        hazelcastInstance = Hazelcast.newHazelcastInstance(config);
        listener.setHazelcastInstance(hazelcastInstance);
        return hazelcastInstance.getMap(ANNOTATIONS_MAP);
    }

    private void put(IMap<FetchKey,FetchRecord> map, FetchKey... keys) {
        for (FetchKey key : keys) {
            map.put(key, record(), 30, TimeUnit.SECONDS);
        }
    }

    private FetchKey fetchKey(String idType, String documentId, String authHash) {
        return new FetchKey(idType, documentId, authHash);
    }

    private FetchRecord record() {
        return new FetchRecord(System.currentTimeMillis(), 1);
    }

    private Config isolatedConfig() {
        Config config = new Config();
        config.setClusterName("annotation-removal-test-" + UUID.randomUUID());
        config.setProperty("hazelcast.logging.type", "none");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(true);
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        return config;
    }

    private AnnotationMessage annotationMessage(String annotationId) {
        Annotation annotation = Annotation.newBuilder().setDocumentId("document").setAnnotationId(annotationId).build();
        return AnnotationMessage.newBuilder().addAnnotations(annotation).build();
    }

    private void await(String description, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25);
        }
        throw new AssertionError("Timed out waiting for " + description);
    }
}
