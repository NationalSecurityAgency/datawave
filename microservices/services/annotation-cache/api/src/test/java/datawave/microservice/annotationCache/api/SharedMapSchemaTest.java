package datawave.microservice.annotationCache.api;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.impl.SerializationServiceSupport;

/** Tests key identity, serialization, and map behavior shared by Hazelcast members and clients. */
class SharedMapSchemaTest {
    private HazelcastInstance member;
    private HazelcastInstance client;

    @AfterEach
    void shutDownHazelcast() {
        if (client != null) {
            client.shutdown();
        }
        if (member != null) {
            member.shutdown();
        }
    }

    /** Checks that every annotation-key field affects equality and delimiter values stay distinct. */
    @Test
    void annotationKeyUsesAllIdentityComponentsAndKeepsDelimiterValuesDistinct() {
        AnnotationKey key = new AnnotationKey("type:one", "document:two", "annotation:three");

        assertEquals(key, new AnnotationKey("type:one", "document:two", "annotation:three"));
        assertEquals(key.hashCode(), new AnnotationKey("type:one", "document:two", "annotation:three").hashCode());
        assertNotEquals(key, new AnnotationKey("type", "one:document:two", "annotation:three"));
        assertNotEquals(key, new AnnotationKey("type:one", "document:two", "different-annotation"));
        assertNotEquals(new AnnotationKey("type-a", "same-document", "same-annotation"), new AnnotationKey("type-b", "same-document", "same-annotation"));
        assertNotEquals(new AnnotationKey("same-type", "document-a", "same-annotation"), new AnnotationKey("same-type", "document-b", "same-annotation"));
    }

    /** Checks that every fetch-key field affects equality and delimiter values stay distinct. */
    @Test
    void fetchKeyUsesAllIdentityComponentsAndKeepsDelimiterValuesDistinct() {
        FetchKey key = new FetchKey("type:one", "document:two", "authorization:three");

        assertEquals(key, new FetchKey("type:one", "document:two", "authorization:three"));
        assertEquals(key.hashCode(), new FetchKey("type:one", "document:two", "authorization:three").hashCode());
        assertNotEquals(key, new FetchKey("type", "one:document:two", "authorization:three"));
        assertNotEquals(key, new FetchKey("type:one", "document:two", "different-authorization"));
        assertNotEquals(new FetchKey("type-a", "same-document", "same-authorization"), new FetchKey("type-b", "same-document", "same-authorization"));
        assertNotEquals(new FetchKey("same-type", "document-a", "same-authorization"), new FetchKey("same-type", "document-b", "same-authorization"));

    }

    /** Checks that both key types reject null or blank identity fields. */
    @Test
    void rejectsNullAndBlankIdentityComponents() {
        assertThrows(IllegalArgumentException.class, () -> new AnnotationKey(null, "document", "annotation"));
        assertThrows(IllegalArgumentException.class, () -> new AnnotationKey("type", " \t", "annotation"));
        assertThrows(IllegalArgumentException.class, () -> new AnnotationKey("type", "document", ""));
        assertThrows(IllegalArgumentException.class, () -> new FetchKey(" ", "document", "authorization"));
        assertThrows(IllegalArgumentException.class, () -> new FetchKey("type", null, "authorization"));
        assertThrows(IllegalArgumentException.class, () -> new FetchKey("type", "document", "\n"));
    }

    /** Checks that key proxies serialize equal tuples identically despite different string sharing. */
    @Test
    void defaultJavaSerializationReproducesReferenceSharingDefectButProxiesAreCanonical() throws Exception {
        String shared = new String("same-value");
        LegacyCompositeKey sharedTuple = new LegacyCompositeKey("UUID", shared, shared);
        LegacyCompositeKey distinctTuple = new LegacyCompositeKey("UUID", new String("same-value"), new String("same-value"));

        assertEquals(sharedTuple, distinctTuple);
        assertFalse(Arrays.equals(javaBytes(sharedTuple), javaBytes(distinctTuple)),
                        "ordinary Java serialization encodes reference sharing in String-field tuples");
        assertArrayEquals(javaBytes(new AnnotationKey("UUID", shared, shared)),
                        javaBytes(new AnnotationKey("UUID", new String("same-value"), new String("same-value"))));
        assertArrayEquals(javaBytes(new FetchKey("UUID", shared, shared)), javaBytes(new FetchKey("UUID", new String("same-value"), new String("same-value"))));
    }

    /** Checks that Java serialization preserves both fields in a fetch record. */
    @Test
    void fetchRecordJavaRoundTripPreservesDiagnosticFields() throws Exception {
        FetchRecord record = new FetchRecord(123456789L, 17);
        FetchRecord copy = javaRoundTrip(record);

        assertEquals(123456789L, copy.getFetchedAt());
        assertEquals(17, copy.getAnnotationCountReturned());
    }

    /** Starts a member and client with the map indexes used by the integration checks and no custom serializers. */
    private void startHazelcast() {
        String clusterName = "annotation-schema-" + UUID.randomUUID();
        Config config = new Config();
        config.setClusterName(clusterName);
        config.getNetworkConfig().setPort(0);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        for (String mapName : new String[] {Constants.ANNOTATIONS_MAP, Constants.FETCH_MAP}) {
            config.addMapConfig(new MapConfig(mapName)
                            .addIndexConfig(new IndexConfig(IndexType.HASH, Constants.ID_TYPE_KEY_ATTRIBUTE, Constants.DOCUMENT_ID_KEY_ATTRIBUTE))
                            .addIndexConfig(new IndexConfig(IndexType.HASH, Constants.DOCUMENT_ID_KEY_ATTRIBUTE)));
        }
        assertTrue(config.getSerializationConfig().getSerializerConfigs().isEmpty());
        member = Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        assertTrue(clientConfig.getSerializationConfig().getSerializerConfigs().isEmpty());
        clientConfig.setClusterName(clusterName);
        clientConfig.getNetworkConfig().addAddress(
                        member.getCluster().getLocalMember().getAddress().getHost() + ":" + member.getCluster().getLocalMember().getAddress().getPort());
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(10_000);
        client = HazelcastClient.newHazelcastClient(clientConfig);
    }

    /** Checks that a Hazelcast client can read, write, and query entries using key attributes. */
    @Test
    void hazelcastClientRoundTripsEntriesAndQueriesKeyAttributes() {
        startHazelcast();
        AnnotationKey firstAnnotation = new AnnotationKey("TYPE_A", "DOC_1", "ANN_1");
        AnnotationKey otherTypeSameDocument = new AnnotationKey("TYPE_B", "DOC_1", "ANN_1");
        AnnotationKey otherDocument = new AnnotationKey("TYPE_A", "DOC_2", "ANN_1");
        client.<AnnotationKey,String> getMap(Constants.ANNOTATIONS_MAP).put(firstAnnotation, "annotation-a");
        client.<AnnotationKey,String> getMap(Constants.ANNOTATIONS_MAP).put(otherTypeSameDocument, "annotation-b");
        client.<AnnotationKey,String> getMap(Constants.ANNOTATIONS_MAP).put(otherDocument, "annotation-c");

        assertEquals("annotation-a", member.<AnnotationKey,String> getMap(Constants.ANNOTATIONS_MAP).get(new AnnotationKey("TYPE_A", "DOC_1", "ANN_1")));
        assertEquals("annotation-a", client.<AnnotationKey,String> getMap(Constants.ANNOTATIONS_MAP).get(firstAnnotation));
        assertEquals(new HashSet<>(Arrays.asList(firstAnnotation, otherTypeSameDocument)),
                        client.<AnnotationKey,String> getMap(Constants.ANNOTATIONS_MAP).keySet(Predicates.equal(Constants.DOCUMENT_ID_KEY_ATTRIBUTE, "DOC_1")));
        assertEquals(new HashSet<>(Arrays.asList(firstAnnotation, otherTypeSameDocument)),
                        member.<AnnotationKey,String> getMap(Constants.ANNOTATIONS_MAP).keySet(Predicates.equal(Constants.DOCUMENT_ID_KEY_ATTRIBUTE, "DOC_1")));
        assertEquals(new HashSet<>(Arrays.asList(firstAnnotation, otherDocument)),
                        member.<AnnotationKey,String> getMap(Constants.ANNOTATIONS_MAP).keySet(Predicates.equal(Constants.ID_TYPE_KEY_ATTRIBUTE, "TYPE_A")));

        FetchKey firstFetch = new FetchKey("TYPE_A", "DOC_1", "AUTH_A");
        FetchKey otherAuthorization = new FetchKey("TYPE_A", "DOC_1", "AUTH_B");
        FetchKey otherTypeFetch = new FetchKey("TYPE_B", "DOC_1", "AUTH_A");
        FetchRecord fetchRecord = new FetchRecord(9876L, 3);
        client.<FetchKey,FetchRecord> getMap(Constants.FETCH_MAP).put(firstFetch, fetchRecord);
        client.<FetchKey,FetchRecord> getMap(Constants.FETCH_MAP).put(otherAuthorization, new FetchRecord(9877L, 4));
        client.<FetchKey,FetchRecord> getMap(Constants.FETCH_MAP).put(otherTypeFetch, new FetchRecord(9878L, 5));

        FetchRecord received = member.<FetchKey,FetchRecord> getMap(Constants.FETCH_MAP).get(new FetchKey("TYPE_A", "DOC_1", "AUTH_A"));
        assertEquals(9876L, received.getFetchedAt());
        assertEquals(3, received.getAnnotationCountReturned());
        FetchRecord clientReceived = client.<FetchKey,FetchRecord> getMap(Constants.FETCH_MAP).get(firstFetch);
        assertEquals(9876L, clientReceived.getFetchedAt());
        assertEquals(3, clientReceived.getAnnotationCountReturned());
        Set<FetchKey> documentKeys = member.<FetchKey,FetchRecord> getMap(Constants.FETCH_MAP)
                        .keySet(Predicates.equal(Constants.DOCUMENT_ID_KEY_ATTRIBUTE, "DOC_1"));
        assertEquals(new HashSet<>(Arrays.asList(firstFetch, otherAuthorization, otherTypeFetch)), documentKeys);
        assertTrue(member.<FetchKey,FetchRecord> getMap(Constants.FETCH_MAP).keySet(Predicates.equal(Constants.ID_TYPE_KEY_ATTRIBUTE, "TYPE_A"))
                        .containsAll(Arrays.asList(firstFetch, otherAuthorization)));
    }

    /** Checks that equivalent keys share wire identity and work with indexed map operations. */
    @Test
    void equivalentKeysHaveCanonicalHazelcastIdentityAndIndexedMapOperations() {
        startHazelcast();
        String same = new String("repeated");
        String longIdentity = "long-" + "x".repeat(70_000);
        String[] annotationCasesA = {same, "interned".intern(), new String("pair"), "left|:right/雪\uD83D\uDE80", longIdentity};
        String[] annotationCasesB = {new String("repeated"), new String("interned"), new String("pair"), new String("left|:right/雪\uD83D\uDE80"),
                new String(longIdentity)};
        for (int index = 0; index < annotationCasesA.length; index++) {
            verifyAnnotationKeyIdentity(new AnnotationKey(annotationCasesA[index], annotationCasesA[index], annotationCasesA[index]),
                            new AnnotationKey(annotationCasesB[index], annotationCasesB[index], annotationCasesB[index]));
        }
        verifyAnnotationKeyIdentity(new AnnotationKey(same, "pair", same),
                        new AnnotationKey(new String("repeated"), new String("pair"), new String("repeated")));
        assertFalse(Arrays.equals(memberData(new AnnotationKey("distinct-a", "doc", "ann")).toByteArray(),
                        memberData(new AnnotationKey("distinct-b", "doc", "ann")).toByteArray()));

        String repeated = new String("same-value");
        verifyFetchKeyIdentity(new FetchKey(repeated, repeated, repeated),
                        new FetchKey(new String("same-value"), new String("same-value"), new String("same-value")));
        verifyFetchKeyIdentity(new FetchKey(repeated, "pair", repeated), new FetchKey(new String("same-value"), new String("pair"), new String("same-value")));
        verifyFetchKeyIdentity(new FetchKey("interned".intern(), new String("pair"), "interned".intern()),
                        new FetchKey(new String("interned"), new String("pair"), new String("interned")));
        verifyFetchKeyIdentity(new FetchKey("left|:right/雪\uD83D\uDE80", "document", "authorization"),
                        new FetchKey(new String("left|:right/雪\uD83D\uDE80"), new String("document"), new String("authorization")));
        verifyFetchKeyIdentity(new FetchKey(longIdentity, "long-document", longIdentity),
                        new FetchKey(new String(longIdentity), new String("long-document"), new String(longIdentity)));
        assertFalse(Arrays.equals(clientData(new FetchKey("type", "doc", "auth-a")).toByteArray(),
                        clientData(new FetchKey("type", "doc", "auth-b")).toByteArray()));
    }

    /** Checks that equivalent keys share locks across threads and Hazelcast endpoints. */
    @Test
    void equivalentKeysShareLocksAcrossThreadsAndEndpoints() {
        startHazelcast();
        String same = new String("repeated");
        AnnotationKey lockA = new AnnotationKey("lock-type", "lock-doc", same);
        AnnotationKey lockB = new AnnotationKey(new String("lock-type"), new String("lock-doc"), new String("repeated"));
        assertWireEqual(lockA, lockB);
        verifyExclusiveLock(Constants.ANNOTATIONS_MAP, lockA, lockB);
        verifyExclusiveLock(Constants.FETCH_MAP, new FetchKey("lock-type", same, same),
                        new FetchKey(new String("lock-type"), new String("repeated"), new String("repeated")));
    }

    /** Verifies equal serialized keys share a lock across endpoints and threads, including unlock by an equivalent key. */
    private <K> void verifyExclusiveLock(String mapName, K first, K equivalent) {
        IMap<K,Object> map = client.getMap(mapName);
        IMap<K,Object> memberMap = member.getMap(mapName);
        ExecutorService otherThread = Executors.newSingleThreadExecutor();
        map.lock(first);
        try {
            assertEquals(Boolean.FALSE, otherThread.submit(() -> {
                boolean acquired = memberMap.tryLock(equivalent, 1, TimeUnit.SECONDS);
                if (acquired) {
                    memberMap.unlock(equivalent);
                }
                return acquired;
            }).get(5, TimeUnit.SECONDS), "equal serialized keys must share an exclusive lock across threads/endpoints");
            map.unlock(equivalent);
            assertEquals(Boolean.TRUE, otherThread.submit(() -> {
                boolean acquired = memberMap.tryLock(first, 1, TimeUnit.SECONDS);
                if (acquired) {
                    memberMap.unlock(first);
                }
                return acquired;
            }).get(5, TimeUnit.SECONDS), "unlock with an equal key must release the original lock");
        } catch (Exception e) {
            throw new AssertionError("Cross-thread lock identity check failed", e);
        } finally {
            if (map.isLocked(first)) {
                map.unlock(first);
            }
            otherThread.shutdownNow();
        }
    }

    /** Verifies equivalent annotation keys share wire identity and map entries across reads, writes, removes, and indexed queries. */
    private void verifyAnnotationKeyIdentity(AnnotationKey first, AnnotationKey equivalent) {
        assertEquals(first, equivalent);
        assertWireEqual(first, equivalent);
        var map = client.<AnnotationKey,String> getMap(Constants.ANNOTATIONS_MAP);
        int sizeBefore = map.size();
        map.put(first, "first");
        assertEquals("first", map.get(equivalent));
        assertEquals("first", map.putIfAbsent(equivalent, "duplicate"));
        assertEquals(sizeBefore + 1, map.size());
        assertEquals("first", map.get(first));
        assertEquals("first", map.remove(equivalent));
        assertFalse(map.containsKey(first));
        member.<AnnotationKey,String> getMap(Constants.ANNOTATIONS_MAP).put(equivalent, "reinserted");
        assertEquals("reinserted", map.get(first));
        assertTrue(map.keySet(Predicates.equal(Constants.DOCUMENT_ID_KEY_ATTRIBUTE, first.getDocumentId())).contains(first));
        assertTrue(map.keySet(Predicates.and(Predicates.equal(Constants.ID_TYPE_KEY_ATTRIBUTE, first.getIdType()),
                        Predicates.equal(Constants.DOCUMENT_ID_KEY_ATTRIBUTE, first.getDocumentId()))).contains(first));
        assertEquals("reinserted", map.remove(first));
        assertEquals(sizeBefore, map.size());
    }

    /** Verifies equivalent fetch keys share wire identity and map entries across reads, writes, removes, and indexed queries. */
    private void verifyFetchKeyIdentity(FetchKey first, FetchKey equivalent) {
        assertEquals(first, equivalent);
        assertWireEqual(first, equivalent);
        var map = client.<FetchKey,FetchRecord> getMap(Constants.FETCH_MAP);
        int sizeBefore = map.size();
        FetchRecord record = new FetchRecord(42L, 7);
        map.put(first, record);
        assertEquals(record.getFetchedAt(), map.get(equivalent).getFetchedAt());
        assertEquals(record.getAnnotationCountReturned(), map.get(equivalent).getAnnotationCountReturned());
        FetchRecord existing = map.putIfAbsent(equivalent, new FetchRecord(43L, 8));
        assertEquals(record.getFetchedAt(), existing.getFetchedAt());
        assertEquals(record.getAnnotationCountReturned(), existing.getAnnotationCountReturned());
        assertEquals(sizeBefore + 1, map.size());
        FetchRecord removed = map.remove(equivalent);
        assertEquals(record.getFetchedAt(), removed.getFetchedAt());
        assertFalse(map.containsKey(first));
        member.<FetchKey,FetchRecord> getMap(Constants.FETCH_MAP).put(equivalent, record);
        assertEquals(record.getFetchedAt(), map.get(first).getFetchedAt());
        assertTrue(map.keySet(Predicates.equal(Constants.DOCUMENT_ID_KEY_ATTRIBUTE, first.getDocumentId())).contains(first));
        assertTrue(map.keySet(Predicates.and(Predicates.equal(Constants.ID_TYPE_KEY_ATTRIBUTE, first.getIdType()),
                        Predicates.equal(Constants.DOCUMENT_ID_KEY_ATTRIBUTE, first.getDocumentId()))).contains(first));
        assertEquals(record.getFetchedAt(), map.remove(first).getFetchedAt());
        assertEquals(sizeBefore, map.size());
    }

    /** Verifies equivalent keys have identical member and client serialized bytes, types, and partition hashes. */
    private void assertWireEqual(Object first, Object equivalent) {
        Data memberFirst = memberData(first);
        Data memberEquivalent = memberData(equivalent);
        assertEquals(memberFirst, memberEquivalent, "member Hazelcast Data identity must match");
        assertEquals(memberFirst.getPartitionHash(), memberEquivalent.getPartitionHash());
        assertEquals(memberFirst.getType(), memberEquivalent.getType(), "member Hazelcast Data types must match");
        assertArrayEquals(memberFirst.toByteArray(), memberEquivalent.toByteArray(), "member Hazelcast Data bytes must be canonical");
        Data clientFirst = clientData(first);
        Data clientEquivalent = clientData(equivalent);
        assertEquals(clientFirst, clientEquivalent, "client Hazelcast Data identity must match");
        assertEquals(clientFirst.getPartitionHash(), clientEquivalent.getPartitionHash());
        assertArrayEquals(memberFirst.toByteArray(), clientFirst.toByteArray(), "member/client encoding must match");
        assertEquals(clientFirst.getType(), clientEquivalent.getType(), "client Hazelcast Data types must match");
        assertArrayEquals(clientFirst.toByteArray(), clientEquivalent.toByteArray(), "client Hazelcast Data bytes must be canonical");
    }

    /** Serializes a value with the embedded member to inspect its Hazelcast key representation. */
    private Data memberData(Object value) {
        return ((SerializationServiceSupport) member).getSerializationService().toData(value);
    }

    /** Serializes a value with the client to compare its Hazelcast key representation with the member's. */
    private Data clientData(Object value) {
        return ((SerializationServiceSupport) client).getSerializationService().toData(value);
    }

    /** Kept to show why proxies are needed: ordinary Java serialization can encode equal String fields differently based on reference sharing. */
    private static final class LegacyCompositeKey implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String idType;
        private final String documentId;
        private final String value;

        /** Creates a legacy key from three identity fields. */
        private LegacyCompositeKey(String idType, String documentId, String value) {
            this.idType = idType;
            this.documentId = documentId;
            this.value = value;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof LegacyCompositeKey)) {
                return false;
            }
            LegacyCompositeKey that = (LegacyCompositeKey) other;
            return idType.equals(that.idType) && documentId.equals(that.documentId) && value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(idType, documentId, value);
        }
    }

    /** Serializes and deserializes a value to check its Java serialization round trip. */
    private static <T> T javaRoundTrip(T value) throws Exception {
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(javaBytes(value)))) {
            @SuppressWarnings("unchecked")
            T copy = (T) input.readObject();
            return copy;
        }
    }

    /** Returns the Java serialization stream for comparing serialized values. */
    private static byte[] javaBytes(Object value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(value);
        }
        return bytes.toByteArray();
    }

}
