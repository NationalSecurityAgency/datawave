package datawave.microservice.annotationCache.config;

import static datawave.microservice.annotationCache.api.Constants.ANNOTATIONS_MAP;
import static datawave.microservice.annotationCache.api.Constants.DOCUMENT_ID_KEY_ATTRIBUTE;
import static datawave.microservice.annotationCache.api.Constants.FETCH_MAP;
import static datawave.microservice.annotationCache.api.Constants.ID_TYPE_KEY_ATTRIBUTE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.query.LocalIndexStats;
import com.hazelcast.query.Predicates;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationMessage;
import datawave.microservice.annotationCache.AnnotationMapStore;
import datawave.microservice.annotationCache.AnnotationSyncListener;
import datawave.microservice.annotationCache.api.AnnotationKey;
import datawave.microservice.annotationCache.api.FetchKey;
import datawave.microservice.annotationCache.api.FetchRecord;
import datawave.microservice.annotationCache.api.PersistenceMode;

/**
 * Opt-in raw Hazelcast shared-map scaling harness. It deliberately does not boot Sonicweb or Datawave. Run with -Dannotation.cache.scaling=true; normal test
 * runs skip this workload.
 */
class AnnotationCacheScalingHarnessTest {
    private static final String ID_TYPE = "UUID";
    private static final String OTHER_ID_TYPE = "PAGE_ID";
    private static final int MAX_LATENCY_SAMPLES = 1_000_000;

    @Test
    void collectConfigurableSharedMapMeasurements() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("annotation.cache.scaling"), "opt-in; see service/scaling-harness.md");
        Settings settings = Settings.fromSystemProperties();
        new Harness(settings).run();
    }

    @Test
    void datasetCheckpointAcceptsNegativeResultsButRejectsMissingNonemptyEntries() {
        String documentId = "checkpoint-doc";
        List<AnnotationKey> emptyKeys = Collections.emptyList();
        validateDocumentKeys(emptyKeys, documentId, 0);
        validateMessages(Collections.emptyList(), documentId, Collections.emptySet(), 0, "empty checkpoint", 16);
        validateNegativeMarkers(List.of(new FetchRecord(System.currentTimeMillis(), 0)), 1);

        List<AnnotationKey> nonemptyKeys = List.of(new AnnotationKey(ID_TYPE, documentId, "ann-0"),
                        new AnnotationKey(OTHER_ID_TYPE, documentId, "other-ann-0"));
        validateDocumentKeys(nonemptyKeys, documentId, 1);
        validateMessages(List.of(checkpointMessage(documentId, "ann-0", 16)), documentId, Set.of("ann-0"), 1, "pair checkpoint", 16);
        validateMessages(List.of(checkpointMessage(documentId, "ann-0", 16), checkpointMessage(documentId, "other-ann-0", 16)), documentId,
                        Set.of("ann-0", "other-ann-0"), 2, "document checkpoint", 16);

        assertThrows(AssertionError.class, () -> validateDocumentKeys(nonemptyKeys.subList(0, 1), documentId, 1),
                        "nonempty checkpoint must reject a missing identifier type entry");
        assertThrows(AssertionError.class, () -> validateMessages(Collections.emptyList(), documentId, Set.of("ann-0"), 1, "incomplete checkpoint", 16),
                        "nonempty checkpoint must reject missing values even when the result is empty");
        assertTrue(IntStream.range(0, 10).mapToObj(index -> "doc-42-" + index).noneMatch(Harness.mergeDocumentId(0, 42, 3)::equals),
                        "zero-result merge fixture must not collide with seeded document IDs");
    }

    @Test
    void mixedPhaseReadClassificationTracksConfiguredAndExecutedOperations() {
        assertTrue(!Harness.mixedReadSelected(0, 0), "0% must select writes only");
        assertTrue(Harness.mixedReadSelected(0, 100), "100% must select reads only");
        assertEquals(80, IntStream.range(0, 100).filter(sequence -> Harness.mixedReadSelected(sequence, 80)).count(),
                        "80% selection must retain its configured mix");

        assertTrue(!Harness.requiresIndexedRead("mixed-read-write-annotation-values", 0),
                        "a writes-only or short phase with no executed reads must not require indexed-query deltas");
        assertTrue(Harness.requiresIndexedRead("mixed-read-write-annotation-values", 1),
                        "a mixed phase with an executed read must require its own indexed-query evidence");
        assertTrue(Harness.requiresIndexedRead("annotation-value-query-exact-pair", 0),
                        "dedicated value-query phases require indexed evidence independent of the mixed workload");
        assertEquals("not-applicable", Harness.queryScope("mixed-read-write-annotation-values", 0));
        assertEquals("not-applicable", Harness.readKind("mixed-read-write-annotation-values", 0));
        assertEquals(-1, Harness.expectedResults("mixed-read-write-annotation-values", 0, 2));
        assertEquals("mixed_exact_pair_values_and_putIfAbsent", Harness.queryScope("mixed-read-write-annotation-values", 1));
        assertEquals("annotation-values", Harness.readKind("mixed-read-write-annotation-values", 1));
        assertEquals(2, Harness.expectedResults("mixed-read-write-annotation-values", 1, 2));
    }

    @Test
    void populatedClearKeepsWarmupAndMeasuredIterationsPopulated() {
        runClearRegression(1);
        runClearRegression(0);
    }

    private void runClearRegression(int warmupIterations) {
        Set<Integer> markers = new HashSet<>();
        AtomicInteger refills = new AtomicInteger();
        List<ClearSample> samples = runClearSeries(3, warmupIterations, 4, () -> markers.size(), () -> {
            refills.incrementAndGet();
            markers.clear();
            markers.addAll(Set.of(1, 2, 3));
        }, markers::clear);
        assertEquals(4 + warmupIterations, refills.get(), "every warmup/measured iteration must refill its own populated fixture");
        assertTrue(samples.stream().allMatch(sample -> sample.preClearCount == 3 && sample.postClearCount == 0));
    }

    @Test
    void populatedClearRejectsWrongPopulationBeforeTimingClear() {
        Set<Integer> markers = new HashSet<>();
        markers.add(1);
        assertThrows(AssertionError.class, () -> runClearSeries(3, 0, 1, () -> markers.size(), () -> {}, markers::clear));
        assertEquals(Set.of(1), markers, "a failed precondition must not time or execute clear");
    }

    @Test
    void zeroPopulationIsExplicitlyNotApplicable() {
        AtomicInteger clearCalls = new AtomicInteger();
        assertEquals("empty-not-applicable", clearPopulationStatus(0));
        List<ClearSample> samples = runClearSeries(0, 0, 3, () -> 0, () -> {
            throw new AssertionError("empty input must not seed a populated fixture");
        }, clearCalls::incrementAndGet);
        assertTrue(samples.isEmpty(), "zero population has no populated-clear samples");
        assertEquals(0, clearCalls.get(), "zero population must not invent an empty-clear measurement");
    }

    private static AnnotationMessage checkpointMessage(String documentId, String annotationId, int payloadBytes) {
        return AnnotationMessage.newBuilder().putParameters("benchmark-payload", "x".repeat(payloadBytes))
                        .addAnnotations(Annotation.newBuilder().setDocumentId(documentId).setAnnotationId(annotationId).build()).build();
    }

    private static void validateDocumentKeys(List<AnnotationKey> documentKeys, String documentId, int annotationsPerIdType) {
        assertEquals(annotationsPerIdType * 2, documentKeys.size(), "document-ID query key count across identifier types at checkpoint");
        assertTrue(documentKeys.stream()
                        .allMatch(key -> documentId.equals(key.getDocumentId()) && (ID_TYPE.equals(key.getIdType()) || OTHER_ID_TYPE.equals(key.getIdType()))),
                        "document-only query returned an unexpected document or identifier type");
        if (annotationsPerIdType > 0) {
            assertTrue(documentKeys.stream().anyMatch(key -> ID_TYPE.equals(key.getIdType()))
                            && documentKeys.stream().anyMatch(key -> OTHER_ID_TYPE.equals(key.getIdType())),
                            "document-only query must include this document under both identifier types");
        }
    }

    private static void validateNegativeMarkers(Collection<FetchRecord> markers, int expectedMarkers) {
        assertEquals(expectedMarkers, markers.size(), "negative-result marker count");
        assertTrue(markers.stream().allMatch(marker -> marker.getAnnotationCountReturned() == 0), "empty source results must retain negative markers");
    }

    private static void validateMessages(Collection<AnnotationMessage> messages, String documentId, Set<String> expectedIds, int expectedCount, String scope,
                    int payloadBytes) {
        assertEquals(expectedCount, messages.size(), scope + " message count");
        Set<String> actualIds = new HashSet<>();
        for (AnnotationMessage message : messages) {
            assertEquals(1, message.getAnnotationsCount(), scope + " must return exactly one annotation per message");
            Annotation annotation = message.getAnnotations(0);
            assertEquals(documentId, annotation.getDocumentId(), scope + " leaked a different document");
            assertTrue(expectedIds.contains(annotation.getAnnotationId()),
                            scope + " returned an unexpected annotation identity " + annotation.getAnnotationId());
            assertTrue(actualIds.add(annotation.getAnnotationId()), scope + " returned a duplicate annotation value");
            String padding = message.getParametersMap().get("benchmark-payload");
            assertTrue(padding != null, scope + " value is missing configured payload parameter");
            assertEquals(payloadBytes, padding.length(), scope + " payload padding size");
        }
        assertEquals(expectedIds, actualIds, scope + " annotation identities");
    }

    private static String clearPopulationStatus(int expectedPopulation) {
        return expectedPopulation == 0 ? "empty-not-applicable" : "populated";
    }

    private static List<ClearSample> runClearSeries(int expectedPopulation, int warmupIterations, int measuredIterations, LongSupplier size, Runnable refill,
                    Runnable clear) {
        if (expectedPopulation < 0 || warmupIterations < 0 || measuredIterations < 1) {
            throw new IllegalArgumentException("clear population must be nonnegative, warmup nonnegative, and measured iterations positive");
        }
        if (expectedPopulation == 0) {
            return Collections.emptyList();
        }
        for (int i = 0; i < warmupIterations; i++) {
            refill.run();
            assertEquals(expectedPopulation, size.getAsLong(), "warmup populated-clear precondition");
            clear.run();
            assertEquals(0, size.getAsLong(), "warmup populated-clear postcondition");
        }
        List<ClearSample> samples = new ArrayList<>();
        for (int i = 0; i < measuredIterations; i++) {
            long cycleStart = System.nanoTime();
            long setupStart = cycleStart;
            refill.run();
            long setupNanos = System.nanoTime() - setupStart;
            long preClearCount = size.getAsLong();
            assertEquals(expectedPopulation, preClearCount, "measured populated-clear precondition at iteration " + i);
            long clearStart = System.nanoTime();
            clear.run();
            long clearNanos = System.nanoTime() - clearStart;
            long postClearCount = size.getAsLong();
            assertEquals(0, postClearCount, "measured populated-clear postcondition at iteration " + i);
            samples.add(new ClearSample(i + 1, preClearCount, postClearCount, setupNanos, clearNanos, System.nanoTime() - cycleStart));
        }
        return samples;
    }

    private static final class Harness {
        private final Settings settings;
        private final List<HazelcastInstance> members = new ArrayList<>();
        private final List<HazelcastInstance> clients = new ArrayList<>();
        private final List<Measurement> measurements = new ArrayList<>();
        private final List<Snapshot> snapshots = new ArrayList<>();
        private final List<String> churnDocuments = new ArrayList<>();
        private final List<String> disabledDocuments = new ArrayList<>();
        private final List<HazelcastInstance> clientPool = new ArrayList<>();
        private final AtomicInteger clientSequence = new AtomicInteger();
        private final AtomicInteger operationSequence = new AtomicInteger();
        private final AtomicInteger clearWorkerSequence = new AtomicInteger();
        private final AtomicInteger invalidationWorkerSequence = new AtomicInteger();
        private final AtomicInteger clearFixtureGeneration = new AtomicInteger();
        private volatile MixOperationCounts activeMixOperationCounts;
        private final ThreadLocal<String> clearDocument;
        private final ThreadLocal<String> invalidationDocument;
        private final ThreadLocal<Long> lastOperationNanos = ThreadLocal.withInitial(() -> 0L);
        private final ThreadLocal<HazelcastInstance> operationClient = ThreadLocal
                        .withInitial(() -> clientPool.get(Math.floorMod(clientSequence.getAndIncrement(), clientPool.size())));
        private IMap<AnnotationKey,AnnotationMessage> annotations;
        private IMap<FetchKey,FetchRecord> fetchRecords;
        private ExecutorService workers;
        private Path runDirectory;
        private final AtomicLong nonce = new AtomicLong();
        private ClearRun clearRun;
        private int verifiedPairValueCountPerDocument = -1;
        private int verifiedDocumentValueCountPerDocument = -1;
        private int verifiedNegativeMarkerCount;

        private Harness(Settings settings) {
            this.settings = settings;
            this.clearDocument = ThreadLocal.withInitial(() -> "clear-target-" + settings.seed + "-" + clearWorkerSequence.getAndIncrement());
            this.invalidationDocument = ThreadLocal
                            .withInitial(() -> "invalidate-target-" + settings.seed + "-" + invalidationWorkerSequence.getAndIncrement());
        }

        private void run() throws Exception {
            try {
                startCluster();
                verifyConfiguredIndexes();
                annotations = clients.get(0).getMap(ANNOTATIONS_MAP);
                fetchRecords = clients.get(0).getMap(FETCH_MAP);
                annotations.size();
                fetchRecords.size();
                assertDataObjects();
                seedDataset();
                validateDataset();
                snapshot("seeded");
                workers = Executors.newFixedThreadPool(settings.concurrency);

                measure("annotation-value-query-exact-pair", this::queryPairValues, settings.warmupMs, settings.measuredMs);
                measure("annotation-value-query-document-id-only", this::queryByDocumentIdValues, settings.warmupMs, settings.measuredMs);
                measure("annotation-write-merge-putIfAbsent", this::mergeAnnotation, settings.warmupMs, settings.measuredMs);
                measure("fetch-marker-document-invalidation", this::invalidateFetchMarkers, settings.warmupMs, settings.measuredMs);
                measure("document-id-only-clear-across-id-types", this::clearDocumentAcrossTypes, settings.warmupMs, settings.measuredMs);
                measurePopulatedFetchClear();
                measure("mixed-read-write-annotation-values", this::mixedReadWrite, settings.warmupMs, settings.measuredMs);
                validateDataset();
                snapshot("after-operation-measurements");

                measureDriftingWorkload();
                writeResults();
                for (Measurement measurement : measurements) {
                    assertEquals(0, measurement.errors, measurement.name + " errors; first error=" + measurement.firstError);
                    assertEquals(0, measurement.timeouts, measurement.name + " timeouts");
                    assertTrue(measurement.sampleCount > 0, measurement.name + " must produce latency samples");
                }
            } finally {
                if (workers != null) {
                    workers.shutdownNow();
                    workers.awaitTermination(10, TimeUnit.SECONDS);
                }
                for (HazelcastInstance client : clients) {
                    client.shutdown();
                }
                for (HazelcastInstance member : members) {
                    member.shutdown();
                }
            }
        }

        private void startCluster() throws IOException, InterruptedException {
            int[] ports = availablePorts(settings.members);
            String clusterName = "annotation-scaling-" + UUID.randomUUID();
            AnnotationCacheProperties properties = new AnnotationCacheProperties();
            properties.setMaxCacheAge(Duration.ofSeconds(settings.annotationTtlSeconds));
            properties.setMaxFetchAge(Duration.ofSeconds(settings.fetchTtlSeconds));
            properties.setTopologyMonitoringEnabled(false);

            for (int i = 0; i < settings.members; i++) {
                Config config = new Config();
                config.setClusterName(clusterName);
                config.setProperty("hazelcast.logging.type", "none");
                config.setProperty("hazelcast.shutdownhook.enabled", "false");
                config.getNetworkConfig().setPort(ports[i]).setPortAutoIncrement(false);
                JoinConfig join = config.getNetworkConfig().getJoin();
                join.getAutoDetectionConfig().setEnabled(false);
                join.getMulticastConfig().setEnabled(false);
                join.getTcpIpConfig().setEnabled(true);
                for (int port : ports) {
                    join.getTcpIpConfig().addMember("127.0.0.1:" + port);
                }
                AnnotationSyncListener listener = new AnnotationSyncListener();
                new AnnotationCacheConfiguration(properties).configureMaps(config, mock(AnnotationMapStore.class), listener);
                members.add(Hazelcast.newHazelcastInstance(config));
                listener.setHazelcastInstance(members.get(members.size() - 1));
            }
            await("members to form one cluster", () -> members.stream().allMatch(member -> member.getCluster().getMembers().size() == settings.members),
                            30_000);

            for (int i = 0; i < settings.clients; i++) {
                ClientConfig config = new ClientConfig();
                config.setClusterName(clusterName);
                for (int port : ports) {
                    config.getNetworkConfig().addAddress("127.0.0.1:" + port);
                }
                clients.add(HazelcastClient.newHazelcastClient(config));
                clientPool.add(clients.get(i));
            }
            assertTrue(clients.stream().allMatch(client -> client.getCluster().getMembers().size() == settings.members),
                            "all configured clients must connect to every isolated member");
        }

        private void verifyConfiguredIndexes() {
            for (HazelcastInstance member : members) {
                assertIndexes(member.getConfig().getMapConfig(ANNOTATIONS_MAP).getIndexConfigs(), ANNOTATIONS_MAP);
                assertIndexes(member.getConfig().getMapConfig(FETCH_MAP).getIndexConfigs(), FETCH_MAP);
            }
        }

        private void assertIndexes(List<IndexConfig> indexes, String mapName) {
            assertEquals(2, indexes.size(), mapName + " must have only the two planned indexes");
            assertTrue(indexes.stream()
                            .anyMatch(index -> index.getType() == IndexType.HASH
                                            && index.getAttributes().equals(Arrays.asList(ID_TYPE_KEY_ATTRIBUTE, DOCUMENT_ID_KEY_ATTRIBUTE))),
                            mapName + " missing pair HASH index");
            assertTrue(indexes.stream().anyMatch(
                            index -> index.getType() == IndexType.HASH && index.getAttributes().equals(Collections.singletonList(DOCUMENT_ID_KEY_ATTRIBUTE))),
                            mapName + " missing document HASH index");
        }

        private void seedDataset() {
            long measuredPhasesMs = 8L * (settings.warmupMs + settings.measuredMs) + settings.warmupMs + settings.churnDurationMs
                            + (settings.annotationTtlSeconds + settings.fetchTtlSeconds) * 1000L + settings.settleMs + 60_000L;
            long retention = Math.max(settings.annotationTtlSeconds, (measuredPhasesMs + 999) / 1000);
            long markerRetention = Math.max(settings.fetchTtlSeconds, retention);
            for (int i = 0; i < settings.uniqueDocs; i++) {
                String documentId = documentId(i);
                for (int a = 0; a < settings.annotationsPerDoc; a++) {
                    String annotationId = "ann-" + a;
                    annotations.put(annotationKey(ID_TYPE, documentId, annotationId), annotationMessage(documentId, annotationId, settings.payloadBytes),
                                    retention, TimeUnit.SECONDS);
                    String otherTypeAnnotationId = "other-ann-" + a;
                    annotations.put(annotationKey(OTHER_ID_TYPE, documentId, otherTypeAnnotationId),
                                    annotationMessage(documentId, otherTypeAnnotationId, settings.payloadBytes), retention, TimeUnit.SECONDS);
                }
                for (int auth = 0; auth < settings.authContextsPerDoc; auth++) {
                    fetchRecords.put(fetchKey(ID_TYPE, documentId, "auth-" + auth), new FetchRecord(System.currentTimeMillis(), settings.annotationsPerDoc),
                                    markerRetention, TimeUnit.SECONDS);
                }
            }
            long expectedAnnotations = (long) settings.uniqueDocs * 2L * settings.annotationsPerDoc;
            long expectedMarkers = (long) settings.uniqueDocs * settings.authContextsPerDoc;
            assertEquals(expectedAnnotations, annotations.size(), "seeded annotation key/value count");
            assertEquals(expectedMarkers, fetchRecords.size(), "seeded fetch marker count");
            if (settings.annotationsPerDoc == 0) {
                validateNegativeMarkers(fetchRecords.values(), Math.toIntExact(expectedMarkers));
                verifiedNegativeMarkerCount = Math.toIntExact(expectedMarkers);
            }
        }

        private void validateDataset() {
            assertDataObjects();
            if (settings.uniqueDocs > 0) {
                for (int i : new int[] {0, settings.uniqueDocs / 2, settings.uniqueDocs - 1}) {
                    String documentId = documentId(i);
                    List<AnnotationKey> pairKeys = queryPairKeysFor(documentId);
                    assertEquals(settings.annotationsPerDoc, pairKeys.size(), "pair-query key count at checkpoint for " + documentId);
                    assertTrue(pairKeys.stream().allMatch(key -> ID_TYPE.equals(key.getIdType()) && documentId.equals(key.getDocumentId())),
                                    "pair query returned a key outside the requested pair");
                    Collection<AnnotationMessage> pairMessages = queryPairValuesFor(documentId);
                    validateMessages(pairMessages, documentId, expectedAnnotationIds(ID_TYPE), settings.annotationsPerDoc, "exact-pair value query");
                    verifiedPairValueCountPerDocument = pairMessages.size();
                    List<AnnotationKey> documentKeys = queryByDocumentIdKeysFor(documentId);
                    assertEquals(settings.annotationsPerDoc * 2, documentKeys.size(),
                                    "document-ID query key count across identifier types at checkpoint for " + documentId);
                    validateDocumentKeys(documentKeys, documentId, settings.annotationsPerDoc);
                    Collection<AnnotationMessage> documentMessages = queryByDocumentIdValuesFor(documentId);
                    validateMessages(documentMessages, documentId, union(expectedAnnotationIds(ID_TYPE), expectedAnnotationIds(OTHER_ID_TYPE)),
                                    settings.annotationsPerDoc * 2, "document-ID-only value query including both identifier types");
                    verifiedDocumentValueCountPerDocument = documentMessages.size();
                }
            }
            long indexedQueries = members.stream()
                            .mapToLong(member -> member.<AnnotationKey,AnnotationMessage> getMap(ANNOTATIONS_MAP).getLocalMapStats().getIndexedQueryCount())
                            .sum();
            assertTrue(indexedQueries > 0 || hasZeroAnnotationCheckpoint(),
                            "real member statistics must confirm indexed query execution unless exact zero-result checkpoint evidence applies");
            if (indexedQueries == 0 && hasZeroAnnotationCheckpoint()) {
                System.out.println(
                                "INDEX_EVIDENCE empty seeded dataset: exact pair/document key and value checkpoint counts are 0; configured HASH indexes verified");
            }
        }

        private boolean hasZeroAnnotationCheckpoint() {
            return settings.annotationsPerDoc == 0 && verifiedPairValueCountPerDocument == 0 && verifiedDocumentValueCountPerDocument == 0;
        }

        private void measurePopulatedFetchClear() {
            int expectedPopulation = settings.effectiveClearPopulation();
            // All worker operations from the preceding phase have joined. Fixture IDs never appear in the annotation map, so delayed annotation-loss
            // callbacks cannot target these markers. Topology monitoring remains disabled as in the raw member/client harness.
            fetchRecords.clear();
            assertEquals(0, fetchRecords.size(), "clear fixture starts with an empty fetch map");
            if (expectedPopulation == 0) {
                clearRun = new ClearRun(0, 0, clearPopulationStatus(0), Collections.emptyList(), 0, 0, 0);
                return;
            }
            int warmupIterations = settings.warmupMs > 0 ? 1 : 0;
            List<ClearSample> samples = runClearSeries(expectedPopulation, warmupIterations, settings.clearIterations, () -> fetchRecords.size(),
                            this::refillClearFixture, fetchRecords::clear);
            long setupNanos = samples.stream().mapToLong(sample -> sample.setupNanos).sum();
            long clearNanos = samples.stream().mapToLong(sample -> sample.clearNanos).sum();
            long cycleNanos = samples.stream().mapToLong(sample -> sample.cycleNanos).sum();
            clearRun = new ClearRun(expectedPopulation, warmupIterations, clearPopulationStatus(expectedPopulation), samples, setupNanos, clearNanos,
                            cycleNanos);
            assertEquals(settings.clearIterations, samples.size(), "every configured populated clear must be sampled");
            assertEquals(0, fetchRecords.size(), "final populated-clear fixture must be empty");
            assertDataObjects();
        }

        private void refillClearFixture() {
            int generation = clearFixtureGeneration.getAndIncrement();
            Map<FetchKey,FetchRecord> markers = new HashMap<>(Math.max(16, settings.effectiveClearPopulation() * 2));
            for (int i = 0; i < settings.effectiveClearPopulation(); i++) {
                String documentId = "clear-fixture-" + settings.seed + '-' + generation + '-' + i;
                markers.put(fetchKey(ID_TYPE, documentId, "clear-auth"), new FetchRecord(System.currentTimeMillis(), 1));
            }
            fetchRecords.putAll(markers);
        }

        private void measure(String name, Operation operation, long warmupMs, long measuredMs) throws Exception {
            boolean mixedPhase = isMixedPhase(name);
            MixOperationCounts warmupCounts = mixedPhase ? new MixOperationCounts() : null;
            MixOperationCounts measuredCounts = mixedPhase ? new MixOperationCounts() : null;
            IndexUse before = indexUse();
            if (warmupMs > 0) {
                activeMixOperationCounts = warmupCounts;
                try {
                    runWorkers(operation, warmupMs, false);
                } finally {
                    activeMixOperationCounts = null;
                }
            }
            Counter counter;
            activeMixOperationCounts = measuredCounts;
            try {
                counter = runWorkers(operation, measuredMs, true);
            } finally {
                activeMixOperationCounts = null;
            }
            IndexUse after = indexUse();
            long indexedQueryDelta = after.indexedQueries - before.indexedQueries;
            long indexQueryDelta = after.indexQueryCount - before.indexQueryCount;
            long warmupReads = mixedPhase ? warmupCounts.readOperations.sum() : -1;
            long warmupWrites = mixedPhase ? warmupCounts.writeOperations.sum() : -1;
            long measuredReads = mixedPhase ? measuredCounts.readOperations.sum() : -1;
            long measuredWrites = mixedPhase ? measuredCounts.writeOperations.sum() : -1;
            long phaseReadOperations = mixedPhase ? warmupReads + measuredReads : isDedicatedValueReadPhase(name) ? counter.operations.sum() : -1;
            String readKind = readKind(name, phaseReadOperations);
            String queryScope = queryScope(name, phaseReadOperations);
            int expectedResults = expectedResults(name, phaseReadOperations, settings.annotationsPerDoc);
            boolean emptyResultIndexStatsException = false;
            if (requiresIndexedRead(name, phaseReadOperations)) {
                boolean indexedEvidence = indexedQueryDelta > 0 || indexQueryDelta > 0;
                emptyResultIndexStatsException = !indexedEvidence && isZeroSeededRead(name, phaseReadOperations, expectedResults);
                assertTrue(indexedEvidence || emptyResultIndexStatsException,
                                name + " must increase member indexed-query statistics unless exact zero-result checkpoint evidence applies");
            }
            long elapsedMs = Math.max(1, counter.elapsedNanos / 1_000_000);
            long[] samples = counter.samples();
            Measurement result = new Measurement(name, readKind, queryScope, expectedResults, settings.payloadBytes, indexedQueryDelta, indexQueryDelta,
                            counter.operations.sum(), counter.errors.sum(), counter.timeouts.sum(), counter.firstError.get(), counter.sampleCount.get(),
                            percentile(samples, 0.50), percentile(samples, 0.95), percentile(samples, 0.99), counter.operations.sum() * 1000.0 / elapsedMs,
                            elapsedMs, emptyResultIndexStatsException, mixedPhase ? settings.readPercent : -1, mixedPhase ? 100 - settings.readPercent : -1,
                            warmupReads, warmupWrites, measuredReads, measuredWrites);
            measurements.add(result);
            System.out.printf(
                            "MEASURE %s operations=%d samples=%d errors=%d timeouts=%d indexedQueryDelta=%d indexQueryDelta=%d p50=%.3fms p95=%.3fms p99=%.3fms throughput=%.2f/s%n",
                            name, result.operations, result.sampleCount, result.errors, result.timeouts, indexedQueryDelta, indexQueryDelta,
                            result.p50Nanos / 1_000_000.0, result.p95Nanos / 1_000_000.0, result.p99Nanos / 1_000_000.0, result.throughputPerSecond);
        }

        private IndexUse indexUse() {
            long indexedQueries = 0;
            long indexQueryCount = 0;
            for (HazelcastInstance member : members) {
                LocalMapStats stats = member.<AnnotationKey,AnnotationMessage> getMap(ANNOTATIONS_MAP).getLocalMapStats();
                indexedQueries += stats.getIndexedQueryCount();
                for (LocalIndexStats indexStats : stats.getIndexStats().values()) {
                    indexQueryCount += indexStats.getQueryCount();
                }
            }
            return new IndexUse(indexedQueries, indexQueryCount);
        }

        private static boolean mixedReadSelected(long sequence, int readPercent) {
            return Math.floorMod(sequence, 100) < readPercent;
        }

        private static boolean isMixedPhase(String name) {
            return "mixed-read-write-annotation-values".equals(name);
        }

        private static boolean isDedicatedValueReadPhase(String name) {
            return name.startsWith("annotation-value-query-") || "expired-document-annotation-value-query".equals(name);
        }

        private static boolean requiresIndexedRead(String name, long actualReadOperations) {
            return isDedicatedValueReadPhase(name) || isMixedPhase(name) && actualReadOperations > 0;
        }

        private boolean isZeroSeededRead(String name, long actualReadOperations, int expectedResults) {
            return hasZeroAnnotationCheckpoint() && actualReadOperations > 0 && expectedResults == 0 && ("annotation-value-query-exact-pair".equals(name)
                            || "annotation-value-query-document-id-only".equals(name) || isMixedPhase(name));
        }

        private static String readKind(String name, long actualReadOperations) {
            return requiresIndexedRead(name, actualReadOperations) ? "annotation-values" : "not-applicable";
        }

        private static String queryScope(String name, long actualReadOperations) {
            if ("annotation-value-query-exact-pair".equals(name)) {
                return "exact_pair_idType_documentId";
            }
            if ("annotation-value-query-document-id-only".equals(name)) {
                return "documentId_only_across_idTypes";
            }
            if ("expired-document-annotation-value-query".equals(name)) {
                return "documentId_only_expired";
            }
            if (isMixedPhase(name) && actualReadOperations > 0) {
                return "mixed_exact_pair_values_and_putIfAbsent";
            }
            return "not-applicable";
        }

        private static int expectedResults(String name, long actualReadOperations, int annotationsPerDoc) {
            if (isMixedPhase(name) && actualReadOperations == 0) {
                return -1;
            }
            if ("expired-document-annotation-value-query".equals(name)) {
                return 0;
            }
            if ("annotation-value-query-exact-pair".equals(name) || isMixedPhase(name)) {
                return annotationsPerDoc;
            }
            if ("annotation-value-query-document-id-only".equals(name)) {
                return annotationsPerDoc * 2;
            }
            return -1;
        }

        private Counter runWorkers(Operation operation, long durationMs, boolean record) throws Exception {
            Counter counter = new Counter(record ? MAX_LATENCY_SAMPLES : 0);
            CountDownLatch ready = new CountDownLatch(settings.concurrency);
            CountDownLatch start = new CountDownLatch(1);
            long stopAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(durationMs);
            List<Future<?>> futures = new ArrayList<>();
            for (int worker = 0; worker < settings.concurrency; worker++) {
                futures.add(workers.submit(() -> {
                    ready.countDown();
                    start.await();
                    while (System.nanoTime() < stopAt) {
                        long begin = System.nanoTime();
                        try {
                            lastOperationNanos.set(0L);
                            operation.execute();
                            if (record) {
                                long operationNanos = lastOperationNanos.get();
                                counter.record(operationNanos > 0 ? operationNanos : System.nanoTime() - begin);
                            }
                        } catch (Throwable error) {
                            counter.firstError.compareAndSet(null, error.getClass().getName() + ": " + error.getMessage());
                            counter.errors.increment();
                            if (error instanceof TimeoutException || error.getClass().getSimpleName().contains("Timeout")) {
                                counter.timeouts.increment();
                            }
                            if (record) {
                                counter.record(System.nanoTime() - begin);
                            }
                        }
                    }
                    return null;
                }));
            }
            assertTrue(ready.await(10, TimeUnit.SECONDS), "workers did not become ready");
            long actualStart = System.nanoTime();
            start.countDown();
            for (Future<?> future : futures) {
                try {
                    future.get(durationMs + 60_000, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    counter.timeouts.increment();
                    future.cancel(true);
                }
            }
            counter.elapsedNanos = System.nanoTime() - actualStart;
            return counter;
        }

        private void measureDriftingWorkload() throws Exception {
            snapshot("before-drift");
            if (settings.warmupMs > 0) {
                runChurn(settings.warmupMs, false);
            }
            long churnStart = System.nanoTime();
            Counter churnCounter = runChurn(settings.churnDurationMs, true);
            long churnElapsedMs = Math.max(1, (System.nanoTime() - churnStart) / 1_000_000);
            Measurement churn = new Measurement("drifting-fresh-doc-churn", "not-applicable", "not-applicable", -1, settings.payloadBytes, 0, 0,
                            churnCounter.operations.sum(), churnCounter.errors.sum(), churnCounter.timeouts.sum(), churnCounter.firstError.get(),
                            churnCounter.samples().length, percentile(churnCounter.samples(), .50), percentile(churnCounter.samples(), .95),
                            percentile(churnCounter.samples(), .99), churnCounter.operations.sum() * 1000.0 / churnElapsedMs, churnElapsedMs, false, -1, -1, -1,
                            -1, -1, -1);
            measurements.add(churn);
            snapshot("after-drift-before-expiry");

            long settleMs = settings.annotationTtlSeconds * 1000L + settings.settleMs;
            Thread.sleep(settleMs);
            for (String documentId : churnDocuments) {
                assertTrue(queryPairValuesFor(documentId).isEmpty(), "expired pair value query must be empty for " + documentId);
                assertTrue(queryByDocumentIdValuesFor(documentId).isEmpty(), "expired document-only value query must be empty for " + documentId);
                for (int auth = 0; auth < settings.authContextsPerDoc; auth++) {
                    assertTrue(!fetchRecords.containsKey(fetchKey(ID_TYPE, documentId, "drift-auth-" + auth)),
                                    "expired fetch marker remains for " + documentId);
                }
            }
            for (String documentId : disabledDocuments) {
                assertTrue(queryByDocumentIdValuesFor(documentId).isEmpty(),
                                "disabled-cache equivalent unexpectedly populated an annotation for " + documentId);
            }
            assertDataObjects();
            snapshot("after-expiry-and-settle");

            int reinserts = Math.min(10, Math.max(1, settings.authContextsPerDoc));
            for (int i = 0; i < reinserts; i++) {
                String id = "drift-reinsert-" + settings.seed + "-" + i;
                annotations.put(annotationKey(ID_TYPE, id, "reinsert"), annotationMessage(id, settings.payloadBytes), settings.annotationTtlSeconds,
                                TimeUnit.SECONDS);
                fetchRecords.put(fetchKey(ID_TYPE, id, "drift-auth-0"), new FetchRecord(System.currentTimeMillis(), 1), settings.fetchTtlSeconds,
                                TimeUnit.SECONDS);
            }
            assertDataObjects();
            measure("expired-document-annotation-value-query", this::queryExpiredDocument, 0, Math.max(250, settings.measuredMs / 2));
            snapshot("after-expiry-reinsert");
            assertTrue(members.stream().flatMap(
                            member -> member.<AnnotationKey,AnnotationMessage> getMap(ANNOTATIONS_MAP).getLocalMapStats().getIndexStats().values().stream())
                            .anyMatch(stats -> stats.getMemoryCost() >= 0), "Hazelcast index statistics should be exposed (zero is valid)");
        }

        private Counter runChurn(long durationMs, boolean record) throws InterruptedException {
            Counter counter = new Counter(record ? MAX_LATENCY_SAMPLES : 0);
            long start = System.nanoTime();
            long deadline = start + TimeUnit.MILLISECONDS.toNanos(durationMs);
            long intervalNanos = Math.max(1, TimeUnit.SECONDS.toNanos(1) / settings.churnBatchRate);
            long nextTarget = start;
            while (System.nanoTime() < deadline) {
                for (int inBatch = 0; inBatch < settings.churnBatchSize && System.nanoTime() < deadline; inBatch++) {
                    long begin = System.nanoTime();
                    String id = "drift-" + settings.seed + "-" + nonce.getAndIncrement();
                    int mode = (int) (nonce.get() % 3);
                    try {
                        if (mode == 0) {
                            // Source-empty/negative-cache equivalent: freshness marker, intentionally no annotation values.
                            for (int auth = 0; auth < settings.authContextsPerDoc; auth++) {
                                fetchRecords.put(fetchKey(ID_TYPE, id, "drift-auth-" + auth), new FetchRecord(System.currentTimeMillis(), 0),
                                                settings.fetchTtlSeconds, TimeUnit.SECONDS);
                            }
                        } else if (mode == 1) {
                            // Disabled-cache equivalent: issue a unique logical document identity and make no map writes.
                            disabledDocuments.add(id);
                        } else {
                            for (int auth = 0; auth < settings.authContextsPerDoc; auth++) {
                                fetchRecords.put(fetchKey(ID_TYPE, id, "drift-auth-" + auth), new FetchRecord(System.currentTimeMillis(), 1),
                                                settings.fetchTtlSeconds, TimeUnit.SECONDS);
                            }
                            annotations.put(annotationKey(ID_TYPE, id, "drift-ann"), annotationMessage(id, settings.payloadBytes),
                                            settings.annotationTtlSeconds, TimeUnit.SECONDS);
                        }
                        if (mode != 1) {
                            churnDocuments.add(id);
                        }
                        if (record) {
                            counter.record(System.nanoTime() - begin);
                        } else {
                            counter.operations.increment();
                        }
                    } catch (Throwable e) {
                        counter.firstError.compareAndSet(null, e.getClass().getName() + ": " + e.getMessage());
                        counter.errors.increment();
                        if (e instanceof TimeoutException || e.getClass().getSimpleName().contains("Timeout")) {
                            counter.timeouts.increment();
                        }
                    }
                }
                nextTarget += intervalNanos;
                long wait = nextTarget - System.nanoTime();
                if (wait > 0) {
                    TimeUnit.NANOSECONDS.sleep(wait);
                } else {
                    nextTarget = System.nanoTime(); // no catch-up burst when a batch takes longer than its pacing interval
                }
            }
            counter.elapsedNanos = System.nanoTime() - start;
            return counter;
        }

        private void snapshot(String phase) {
            long annotationCount = annotations.size();
            long fetchCount = fetchRecords.size();
            long heapCost = 0;
            long ownedEntries = 0;
            long indexMemory = 0;
            long indexQueries = 0;
            long indexQueryCount = 0;
            for (HazelcastInstance member : members) {
                for (String mapName : Arrays.asList(ANNOTATIONS_MAP, FETCH_MAP)) {
                    LocalMapStats stats = member.<Object,Object> getMap(mapName).getLocalMapStats();
                    heapCost += stats.getHeapCost();
                    ownedEntries += stats.getOwnedEntryCount();
                    indexQueries += stats.getIndexedQueryCount();
                    for (LocalIndexStats indexStats : stats.getIndexStats().values()) {
                        indexMemory += indexStats.getMemoryCost();
                        indexQueryCount += indexStats.getQueryCount();
                    }
                }
            }
            MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
            MemoryUsage heap = memoryBean.getHeapMemoryUsage();
            Set<String> mapObjects = members.get(0).getDistributedObjects().stream().map(object -> object.getName())
                            .filter(name -> ANNOTATIONS_MAP.equals(name) || FETCH_MAP.equals(name)).collect(Collectors.toSet());
            Snapshot snapshot = new Snapshot(phase, annotationCount, fetchCount, mapObjects.size(), ownedEntries, heapCost, indexMemory, indexQueries,
                            indexQueryCount, heap.getUsed(), heap.getCommitted(), heap.getMax());
            snapshots.add(snapshot);
            System.out.printf("SNAPSHOT %s annotations=%d fetch=%d dataMaps=%d ownedEntries=%d heapCostEstimate=%d indexMemoryEstimate=%d jvmUsedHeap=%d%n",
                            phase, annotationCount, fetchCount, mapObjects.size(), ownedEntries, heapCost, indexMemory, heap.getUsed());
        }

        private void assertDataObjects() {
            Set<String> names = members.stream().flatMap(member -> member.getDistributedObjects().stream()).map(object -> object.getName())
                            .collect(Collectors.toSet());
            assertEquals(Set.of(ANNOTATIONS_MAP, FETCH_MAP), names, "exactly two data map objects must exist; no per-document maps/directory");
        }

        private void queryPairValues() {
            long start = System.nanoTime();
            Collection<AnnotationMessage> result = queryPairValuesFor(documentId(nextIndex(settings.uniqueDocs)));
            result.size(); // consume the fully materialized client result within the timed operation
            lastOperationNanos.set(System.nanoTime() - start);
        }

        private Collection<AnnotationMessage> queryPairValuesFor(String documentId) {
            return new ArrayList<>(annotationMap()
                            .values(Predicates.and(Predicates.equal("__key.idType", ID_TYPE), Predicates.equal("__key.documentId", documentId))));
        }

        private void queryByDocumentIdValues() {
            long start = System.nanoTime();
            Collection<AnnotationMessage> result = queryByDocumentIdValuesFor(documentId(nextIndex(settings.uniqueDocs)));
            result.size(); // consume the fully materialized client result within the timed operation
            lastOperationNanos.set(System.nanoTime() - start);
        }

        private Collection<AnnotationMessage> queryByDocumentIdValuesFor(String documentId) {
            return new ArrayList<>(annotationMap().values(Predicates.equal("__key.documentId", documentId)));
        }

        private List<AnnotationKey> queryPairKeysFor(String documentId) {
            return new ArrayList<>(annotationMap()
                            .keySet(Predicates.and(Predicates.equal("__key.idType", ID_TYPE), Predicates.equal("__key.documentId", documentId))));
        }

        private List<AnnotationKey> queryByDocumentIdKeysFor(String documentId) {
            return new ArrayList<>(annotationMap().keySet(Predicates.equal("__key.documentId", documentId)));
        }

        private Set<String> expectedAnnotationIds(String idType) {
            return IntStream.range(0, settings.annotationsPerDoc).mapToObj(index -> ID_TYPE.equals(idType) ? "ann-" + index : "other-ann-" + index)
                            .collect(Collectors.toSet());
        }

        private Set<String> union(Set<String> first, Set<String> second) {
            Set<String> combined = new HashSet<>(first);
            combined.addAll(second);
            return combined;
        }

        private void validateMessages(Collection<AnnotationMessage> messages, String documentId, Set<String> expectedIds, int expectedCount, String scope) {
            AnnotationCacheScalingHarnessTest.validateMessages(messages, documentId, expectedIds, expectedCount, scope, settings.payloadBytes);
        }

        private String mergeDocumentId(int docIndex) {
            return mergeDocumentId(settings.annotationsPerDoc, settings.seed, docIndex);
        }

        private static String mergeDocumentId(int annotationsPerDoc, long seed, int docIndex) {
            return annotationsPerDoc == 0 ? "merge-empty-" + seed : "doc-" + seed + '-' + docIndex;
        }

        private void mergeAnnotation() {
            int docIndex = nextIndex(settings.uniqueDocs);
            String documentId = mergeDocumentId(docIndex);
            int annotationIndex = Math.max(0, settings.annotationsPerDoc - 1);
            AnnotationKey key = annotationKey(ID_TYPE, documentId, "ann-" + annotationIndex);
            AnnotationMessage value = annotationMessage(documentId, settings.payloadBytes);
            // Existing IDs win, matching Sonicweb's putIfAbsent merge semantics. For zero-annotation datasets this inserts one stable merge key.
            long mergeStart = System.nanoTime();
            annotationMap().putIfAbsent(key, value, Math.max(settings.annotationTtlSeconds, 60), TimeUnit.SECONDS);
            lastOperationNanos.set(System.nanoTime() - mergeStart);
            assertTrue(annotationMap().containsKey(key), "putIfAbsent merge key must be present");
        }

        private void invalidateFetchMarkers() {
            String documentId = invalidationDocument.get();
            for (int auth = 0; auth < settings.authContextsPerDoc; auth++) {
                fetchMap().put(fetchKey(ID_TYPE, documentId, "invalidate-" + auth), new FetchRecord(System.currentTimeMillis(), 1),
                                Math.max(settings.fetchTtlSeconds, 60), TimeUnit.SECONDS);
            }
            long start = System.nanoTime();
            fetchMap().removeAll(Predicates.and(Predicates.equal("__key.idType", ID_TYPE), Predicates.equal("__key.documentId", documentId)));
            lastOperationNanos.set(System.nanoTime() - start);
            for (int auth = 0; auth < settings.authContextsPerDoc; auth++) {
                assertTrue(!fetchMap().containsKey(fetchKey(ID_TYPE, documentId, "invalidate-" + auth)), "document fetch invalidation left a marker");
            }
        }

        private void clearDocumentAcrossTypes() {
            String documentId = clearDocument.get();
            annotationMap().put(annotationKey(ID_TYPE, documentId, "first"), annotationMessage(documentId, settings.payloadBytes), 60, TimeUnit.SECONDS);
            annotationMap().put(annotationKey(OTHER_ID_TYPE, documentId, "second"), annotationMessage(documentId, settings.payloadBytes), 60, TimeUnit.SECONDS);
            long clearStart = System.nanoTime();
            annotationMap().removeAll(Predicates.equal("__key.documentId", documentId));
            lastOperationNanos.set(System.nanoTime() - clearStart);
            assertTrue(queryByDocumentIdKeysFor(documentId).isEmpty(), "document-only clear must remove matching keys across idTypes");
            annotationMap().put(annotationKey(ID_TYPE, documentId, "first"), annotationMessage(documentId, settings.payloadBytes), 60, TimeUnit.SECONDS);
            annotationMap().put(annotationKey(OTHER_ID_TYPE, documentId, "second"), annotationMessage(documentId, settings.payloadBytes), 60, TimeUnit.SECONDS);
        }

        private void mixedReadWrite() {
            if (mixedReadSelected(operationSequence.getAndIncrement(), settings.readPercent)) {
                activeMixOperationCounts.readOperations.increment();
                queryPairValues();
            } else {
                activeMixOperationCounts.writeOperations.increment();
                mergeAnnotation();
            }
        }

        private void queryExpiredDocument() {
            if (churnDocuments.isEmpty()) {
                Collection<AnnotationMessage> results = queryByDocumentIdValuesFor("no-such-expired-document");
                results.size();
            } else {
                String expiredDocumentId = churnDocuments.get(nextIndex(churnDocuments.size()));
                Collection<AnnotationMessage> results = queryByDocumentIdValuesFor(expiredDocumentId);
                results.size();
            }
        }

        private void await(String description, BooleanSupplier condition, long timeoutMs) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            while (System.nanoTime() < deadline) {
                if (condition.getAsBoolean()) {
                    return;
                }
                Thread.sleep(25);
            }
            throw new AssertionError("Timed out waiting for " + description);
        }

        private void writeResults() throws IOException {
            String configuredOutput = System.getProperty("annotation.cache.scaling.output", "target/annotation-cache-scaling");
            Path root = Path.of(configuredOutput).toAbsolutePath();
            Files.createDirectories(root);
            runDirectory = root.resolve("run-" + Instant.now().toString().replace(':', '-') + "-seed-" + settings.seed);
            Files.createDirectories(runDirectory);
            writeCsv(runDirectory.resolve("measurements.csv"));
            writeClearCsv(runDirectory.resolve("populated-fetch-clear.csv"));
            writeJson(runDirectory.resolve("summary.json"));
            writeMarkdown(runDirectory.resolve("summary.md"));
            System.out.println("SCALING_OUTPUT=" + runDirectory);
        }

        private void writeCsv(Path path) throws IOException {
            StringBuilder csv = new StringBuilder(
                            "phase,query_scope,read_kind,configured_read_percent,configured_write_percent,configured_payload_parameter_bytes,expected_values_per_read,warmup_read_operations,warmup_write_operations,measured_read_operations,measured_write_operations,verified_pair_value_count_per_document,verified_document_id_only_value_count_per_document,verified_payload_parameter_bytes,operations,samples,errors,timeouts,indexed_query_delta,index_query_delta,empty_result_index_stats_exception,p50_ms,p95_ms,p99_ms,throughput_per_second,elapsed_ms\n");
            for (Measurement result : measurements) {
                csv.append(result.name).append(',').append(result.queryScope).append(',').append(result.readKind).append(',')
                                .append(result.configuredReadPercent).append(',').append(result.configuredWritePercent).append(',').append(result.payloadBytes)
                                .append(',').append(result.expectedValuesPerRead).append(',').append(result.warmupReadOperations).append(',')
                                .append(result.warmupWriteOperations).append(',').append(result.measuredReadOperations).append(',')
                                .append(result.measuredWriteOperations).append(',').append(verifiedPairValueCountPerDocument).append(',')
                                .append(verifiedDocumentValueCountPerDocument).append(',').append(settings.annotationsPerDoc == 0 ? 0 : settings.payloadBytes)
                                .append(',').append(result.operations).append(',').append(result.sampleCount).append(',').append(result.errors).append(',')
                                .append(result.timeouts).append(',').append(result.indexedQueryDelta).append(',').append(result.indexQueryDelta).append(',')
                                .append(result.emptyResultIndexStatsException).append(',').append(ms(result.p50Nanos)).append(',').append(ms(result.p95Nanos))
                                .append(',').append(ms(result.p99Nanos)).append(',').append(String.format(Locale.ROOT, "%.3f", result.throughputPerSecond))
                                .append(',').append(result.elapsedMs).append('\n');
            }
            Files.writeString(path, csv, StandardCharsets.UTF_8);
        }

        private void writeClearCsv(Path path) throws IOException {
            StringBuilder csv = new StringBuilder(
                            "population_status,expected_population,warmup_iterations,measured_iteration,pre_clear_count,post_clear_count,setup_ms,clear_ms,cycle_ms,cycle_throughput_per_second,errors\n");
            if (clearRun.samples.isEmpty()) {
                csv.append(clearRun.populationStatus).append(',').append(clearRun.expectedPopulation).append(',').append(clearRun.warmupIterations)
                                .append(",0,0,0,0,0,0,0,0\n");
            } else {
                for (ClearSample sample : clearRun.samples) {
                    csv.append(clearRun.populationStatus).append(',').append(clearRun.expectedPopulation).append(',').append(clearRun.warmupIterations)
                                    .append(',').append(sample.iteration).append(',').append(sample.preClearCount).append(',').append(sample.postClearCount)
                                    .append(',').append(ms(sample.setupNanos)).append(',').append(ms(sample.clearNanos)).append(',')
                                    .append(ms(sample.cycleNanos)).append(',')
                                    .append(String.format(Locale.ROOT, "%.3f", 1_000_000_000.0 / Math.max(1, sample.cycleNanos))).append(",0\n");
                }
            }
            Files.writeString(path, csv, StandardCharsets.UTF_8);
        }

        private void writeJson(Path path) throws IOException {
            StringBuilder json = new StringBuilder();
            json.append("{\n  \"measurementScope\": \"raw indexed Hazelcast member/client operations; not Sonicweb repository behavior\",\n");
            json.append("  \"parameters\": ").append(settings.toJson()).append(",\n");
            json.append("  \"correctnessCheckpoints\": {\"pairValueCountPerDocument\":").append(verifiedPairValueCountPerDocument)
                            .append(",\"documentIdOnlyValueCountPerDocumentAcrossTypes\":").append(verifiedDocumentValueCountPerDocument)
                            .append(",\"verifiedNegativeMarkerCount\":").append(verifiedNegativeMarkerCount).append(",\"verifiedPayloadParameterBytes\":")
                            .append(settings.annotationsPerDoc == 0 ? 0 : settings.payloadBytes).append(",\"returnedValuePayloadVerified\":")
                            .append(settings.annotationsPerDoc > 0).append(",\"valueIdentityChecksApplied\":").append(settings.annotationsPerDoc > 0)
                            .append(",\"zeroResultsVerified\":").append(settings.annotationsPerDoc == 0).append("},\n");
            json.append("  \"emptyResultIndexStatsException\": ").append(measurements.stream().anyMatch(m -> m.emptyResultIndexStatsException)).append(",\n");
            json.append("  \"environment\": {");
            json.append("\"javaVersion\":\"").append(escape(System.getProperty("java.version"))).append("\",");
            json.append("\"hazelcastVersion\":\"").append(escape(HazelcastInstance.class.getPackage().getImplementationVersion())).append("\",");
            json.append("\"availableProcessors\":").append(Runtime.getRuntime().availableProcessors()).append(',');
            json.append("\"jvmInputArguments\":\"").append(escape(ManagementFactory.getRuntimeMXBean().getInputArguments().toString())).append("\"},\n");
            json.append("  \"populatedFetchClear\": ").append(clearRun.toJson()).append(",\n");
            json.append("  \"measurements\": [\n");
            for (int i = 0; i < measurements.size(); i++) {
                Measurement m = measurements.get(i);
                json.append("    {\"phase\":\"").append(m.name).append("\",\"queryScope\":\"").append(m.queryScope).append("\",\"readKind\":\"")
                                .append(m.readKind).append("\",\"configuredPayloadParameterBytes\":").append(m.payloadBytes)
                                .append(",\"expectedValuesPerRead\":").append(m.expectedValuesPerRead).append(",\"configuredReadPercent\":")
                                .append(m.configuredReadPercent).append(",\"configuredWritePercent\":").append(m.configuredWritePercent)
                                .append(",\"warmupReadOperations\":").append(m.warmupReadOperations).append(",\"warmupWriteOperations\":")
                                .append(m.warmupWriteOperations).append(",\"measuredReadOperations\":").append(m.measuredReadOperations)
                                .append(",\"measuredWriteOperations\":").append(m.measuredWriteOperations).append(",\"indexedQueryDelta\":")
                                .append(m.indexedQueryDelta).append(",\"indexQueryDelta\":").append(m.indexQueryDelta)
                                .append(",\"emptyResultIndexStatsException\":").append(m.emptyResultIndexStatsException).append(",\"operations\":")
                                .append(m.operations).append(",\"samples\":").append(m.sampleCount).append(",\"errors\":").append(m.errors)
                                .append(",\"timeouts\":").append(m.timeouts).append(",\"firstError\":")
                                .append(m.firstError == null ? "null" : "\"" + escape(m.firstError) + "\"").append(",\"p50Nanos\":").append(m.p50Nanos)
                                .append(",\"p95Nanos\":").append(m.p95Nanos).append(",\"p99Nanos\":").append(m.p99Nanos).append(",\"throughputPerSecond\":")
                                .append(String.format(Locale.ROOT, "%.3f", m.throughputPerSecond)).append(",\"elapsedMs\":").append(m.elapsedMs).append('}')
                                .append(i + 1 == measurements.size() ? "\n" : ",\n");
            }
            json.append("  ],\n  \"snapshots\": [\n");
            for (int i = 0; i < snapshots.size(); i++) {
                Snapshot s = snapshots.get(i);
                json.append("    {\"phase\":\"").append(s.phase).append("\",\"annotationEntries\":").append(s.annotationEntries).append(",\"fetchEntries\":")
                                .append(s.fetchEntries).append(",\"dataMapObjects\":").append(s.dataMapObjects).append(",\"ownedEntries\":")
                                .append(s.ownedEntries).append(",\"hazelcastHeapCostEstimateBytes\":").append(s.heapCostEstimate)
                                .append(",\"indexMemoryEstimateBytes\":").append(s.indexMemoryEstimate).append(",\"indexedQueryCount\":")
                                .append(s.indexedQueryCount).append(",\"indexQueryCount\":").append(s.indexQueryCount).append(",\"jvmUsedHeapBytes\":")
                                .append(s.jvmUsedHeap).append(",\"jvmCommittedHeapBytes\":").append(s.jvmCommittedHeap).append(",\"jvmMaxHeapBytes\":")
                                .append(s.jvmMaxHeap).append('}').append(i + 1 == snapshots.size() ? "\n" : ",\n");
            }
            json.append("  ],\n  \"interpretation\": \"HeapCost and index memory are Hazelcast estimates, not retained heap. JVM heap-used is process-wide. Expiry waits include configured TTL plus settle time; no fixed latency or memory budget is asserted.\"\n}\n");
            Files.writeString(path, json, StandardCharsets.UTF_8);
        }

        private void writeMarkdown(Path path) throws IOException {
            StringBuilder md = new StringBuilder("# Annotation-cache scaling run\n\n");
            md.append("- Raw Hazelcast measurement scope: not Sonicweb repository throughput. Dedicated pair and document-ID-only benchmark phases time `values(predicate)` and materialize returned `AnnotationMessage` values; expected cardinality and configured padding are reported for value-read phases. A writes-only mixed phase reports no query scope or expected values/read. For a zero-annotation seeded corpus the reads return no values and transfer no annotation payload; mixed phase writes use a synthetic document ID outside the seeded corpus. Key queries are limited to untimed correctness checkpoints.\n");
            md.append("- The seeded corpus stores the same document ID under both `UUID` and `PAGE_ID`, with distinct annotation IDs, so document-ID-only reads validate cross-type aggregation while exact-pair reads stay type-scoped. For nonempty datasets, each value contains one annotation and a `benchmark-payload` parameter with the configured number of ASCII bytes.\n");
            md.append("- Untimed correctness checkpoint: exact-pair returned messages/document = ").append(verifiedPairValueCountPerDocument)
                            .append("; document-ID-only returned messages/document across both types = ").append(verifiedDocumentValueCountPerDocument)
                            .append("; configured padding parameter bytes/value = ").append(settings.payloadBytes)
                            .append(settings.annotationsPerDoc == 0
                                            ? ". No seeded values were returned, so no padding/value transfer is claimed; verified payload bytes are 0. Seeded negative-result markers verified = "
                                                            + verifiedNegativeMarkerCount + ".\n"
                                            : ". Returned values were checked for one annotation, expected identities, document isolation, and exact padding length.\n");
            if (measurements.stream().anyMatch(m -> m.emptyResultIndexStatsException)) {
                md.append("- Empty-result index-stat exception: zero pair/document result counts were checkpoint-verified; configured HASH indexes remain proven by member map configuration, but empty-map queries did not increase indexed-query stats. This exception applies only to zero-result seeded read phases.\n");
            }
            md.append("- Populated fetch-map clear: status `").append(clearRun.populationStatus).append("`; expected markers/clear `")
                            .append(clearRun.expectedPopulation).append("`; warmup clears `").append(clearRun.warmupIterations)
                            .append("`; measured clear samples `").append(clearRun.samples.size())
                            .append("`. Cycle throughput including refill and count validation (not clear-only) is `")
                            .append(String.format(Locale.ROOT, "%.3f", clearRun.cycleThroughputPerSecond()))
                            .append("`/s. Pre/post counts, refill setup, clear-only, and end-to-end cycle timings are in `populated-fetch-clear.csv`; the RAW `IMap.clear()` interval excludes refill and count validation and is not a topology event measurement.\n");
            md.append("- Previous global-marker-clear figures were uncontrolled observations and are treated as empty-clear observations where the map had already been emptied; they are not relabeled as populated clears.\n");
            md.append("- Seed: `").append(settings.seed).append("`; Java `").append(System.getProperty("java.version")).append("`; processors `")
                            .append(Runtime.getRuntime().availableProcessors()).append("`; JVM args `")
                            .append(ManagementFactory.getRuntimeMXBean().getInputArguments()).append("`.\n");
            md.append("- Members/clients: ").append(settings.members).append('/').append(settings.clients).append("; docs: ").append(settings.uniqueDocs)
                            .append("; annotations/document/idType: ").append(settings.annotationsPerDoc).append("; auth contexts/document: ")
                            .append(settings.authContextsPerDoc).append("; payload bytes (padding parameter): ").append(settings.payloadBytes).append(".\n");
            md.append("- Clear population: `").append(settings.clearPopulation).append("` (`-1` derives `uniqueDocs × authContextsPerDoc` = ")
                            .append(settings.effectiveClearPopulation()).append("); measured populated clear iterations: ").append(settings.clearIterations)
                            .append("; clear warmup: one fixture clear iff `warmupMs > 0`. Refill uses unique marker-only document IDs, no annotation entries, and the fixture is run serially after worker phases; no unrelated annotation-removal callback can match those IDs.\n");
            md.append("- Read/write mix: ").append(settings.readPercent).append('/').append(100 - settings.readPercent).append(
                            "% configured for the mixed phase. Its measured latency aggregates whichever reads and `putIfAbsent` writes actually ran; at 0% reads this is a writes-only mixed-phase observation, not a mixed read/write result. Warmup and measured actual read/write operation counts are reported separately; `-1` means not applicable to a phase. A mixed phase with no executed reads has no query scope/read kind (`not-applicable`) and expected values/read `-1`; configured percentages remain reported.\n");

            md.append("- Configured annotation/fetch TTL: ").append(settings.annotationTtlSeconds).append('/').append(settings.fetchTtlSeconds)
                            .append(" s; expiry settle: ").append(settings.settleMs)
                            .append(" ms. The seeded benchmark data uses longer per-entry TTL overrides so it remains available while measurements run; drift entries use configured TTL overrides.\n");
            md.append("- Statistics are observations, not acceptance thresholds. Indexed-query deltas are phase-local and include that phase's warmup and measurement; in a mixed phase, warmup and measured branch counts are also reported separately. Branch-count bookkeeping is inside the timed worker loop; per-operation latency samples use each selected read/write operation's own timer, while elapsed throughput includes loop/bookkeeping overhead. HeapCost/index memory are Hazelcast estimates; JVM used heap is process-wide, and no retained-heap claim is made.\n\n");
            md.append("| Phase | Query scope | Read kind | Configured read/write % | Configured padding parameter B | Expected values/read | Warmup R/W | Measured R/W | Index query delta | Empty-result stats exception | Operations | Samples | Errors | p50 ms | p95 ms | p99 ms | Throughput/s | Elapsed ms |\n|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
            for (Measurement m : measurements) {
                md.append('|').append(m.name).append('|').append(m.queryScope).append('|').append(m.readKind).append('|').append(m.configuredReadPercent)
                                .append('/').append(m.configuredWritePercent).append('|').append(m.payloadBytes).append('|').append(m.expectedValuesPerRead)
                                .append('|').append(m.warmupReadOperations).append('/').append(m.warmupWriteOperations).append('|')
                                .append(m.measuredReadOperations).append('/').append(m.measuredWriteOperations).append('|').append(m.indexedQueryDelta)
                                .append('|').append(m.emptyResultIndexStatsException).append('|').append(m.operations).append('|').append(m.sampleCount)
                                .append('|').append(m.errors).append('|').append(String.format(Locale.ROOT, "%.3f", ms(m.p50Nanos))).append('|')
                                .append(String.format(Locale.ROOT, "%.3f", ms(m.p95Nanos))).append('|')
                                .append(String.format(Locale.ROOT, "%.3f", ms(m.p99Nanos))).append('|')
                                .append(String.format(Locale.ROOT, "%.2f", m.throughputPerSecond)).append('|').append(m.elapsedMs).append("|\n");
            }
            md.append("\n## Populated global fetch-map clear (raw clear only)\n\n| Iteration | Expected population | Pre-clear count | Post-clear count | Refill/setup ms | Clear-only ms | Refill+validation+clear cycle ms | Errors |\n|---:|---:|---:|---:|---:|---:|---:|---:|\n");
            for (ClearSample sample : clearRun.samples) {
                md.append('|').append(sample.iteration).append('|').append(clearRun.expectedPopulation).append('|').append(sample.preClearCount).append('|')
                                .append(sample.postClearCount).append('|').append(String.format(Locale.ROOT, "%.3f", ms(sample.setupNanos))).append('|')
                                .append(String.format(Locale.ROOT, "%.3f", ms(sample.clearNanos))).append('|')
                                .append(String.format(Locale.ROOT, "%.3f", ms(sample.cycleNanos))).append("|0|\n");
            }
            if (clearRun.samples.isEmpty()) {
                md.append("Population is zero; status is `empty-not-applicable`, with no clear samples, no errors, and no populated-clear claim.\n");
            }
            md.append("\nThe actual topology listener also clears the global fetch map after its settle/cluster-safety checks, but this raw `IMap.clear()` series does not include membership events, listener scheduling, settle/recovery, or topology timing. Earlier annotation removals do not compete with this fixture: callbacks are not disabled, but fixture documents have no annotation entries and unique IDs. Throughput for refill cycles is not reported as clear throughput; setup and full-cycle timings are provided separately. Percentiles from this bounded sample count, if computed externally, are descriptive only.\n");
            md.append("\n## Memory / expiry snapshots\n\n| Phase | Annotation entries | Fetch entries | Data map objects | Owned entries | Hazelcast heap-cost estimate B | Index memory estimate B | Indexed queries | JVM used heap B |\n")
                            .append("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
            for (Snapshot s : snapshots) {
                md.append('|').append(s.phase).append('|').append(s.annotationEntries).append('|').append(s.fetchEntries).append('|').append(s.dataMapObjects)
                                .append('|').append(s.ownedEntries).append('|').append(s.heapCostEstimate).append('|').append(s.indexMemoryEstimate).append('|')
                                .append(s.indexedQueryCount).append('|').append(s.jvmUsedHeap).append("|\n");
            }
            md.append("\nSmoke-run percentiles are non-representative; additionally, any phase with fewer than 1,000 latency samples (typically the paced drift phase in a smoke run) is descriptive and does not support a stable tail-percentile estimate. Percentile quality depends on the reported sample count. Query cost still includes member fanout and result transfer. Expiration does not promise immediate index/heap shrinkage, and TTL does not bound memory independently of input rate. The populated fetch clear is a raw map operation, not a membership event or Sonicweb behavior.\n");
            Files.writeString(path, md, StandardCharsets.UTF_8);
        }

        private String documentId(int index) {
            return "doc-" + settings.seed + '-' + index;
        }

        private int nextIndex(int bound) {
            long value = settings.seed + operationSequence.getAndIncrement() * 0x9E3779B97F4A7C15L;
            value = (value ^ (value >>> 30)) * 0xBF58476D1CE4E5B9L;
            value = (value ^ (value >>> 27)) * 0x94D049BB133111EBL;
            value ^= value >>> 31;
            return (int) Math.floorMod(value, bound);
        }

        private IMap<AnnotationKey,AnnotationMessage> annotationMap() {
            return operationClient.get().getMap(ANNOTATIONS_MAP);
        }

        private IMap<FetchKey,FetchRecord> fetchMap() {
            return operationClient.get().getMap(FETCH_MAP);
        }

        private AnnotationKey annotationKey(String idType, String documentId, String annotationId) {
            return new AnnotationKey(idType, documentId, annotationId);
        }

        private FetchKey fetchKey(String idType, String documentId, String auth) {
            return new FetchKey(idType, documentId, auth);
        }

        private AnnotationMessage annotationMessage(String documentId, int payloadBytes) {
            return annotationMessage(documentId, "payload-" + nonce.getAndIncrement(), payloadBytes);
        }

        private AnnotationMessage annotationMessage(String documentId, String annotationId, int payloadBytes) {
            String padding = "x".repeat(payloadBytes);
            // Padding lives in a protobuf parameter so the serialized annotation value changes with configured payload size.
            return AnnotationMessage.newBuilder().putParameters("benchmark-payload", padding)
                            .putParameters(datawave.microservice.annotationCache.api.Constants.PERSISTENCE_MODE_PARAMETER, PersistenceMode.CACHE_ONLY.value())
                            .addAnnotations(annotation(documentId, annotationId)).build();
        }

        private Annotation annotation(String documentId, String annotationId) {
            return Annotation.newBuilder().setDocumentId(documentId).setAnnotationId(annotationId).setAnnotationType("benchmark").build();
        }

        private static double percentile(long[] values, double percentile) {
            if (values.length == 0) {
                return 0;
            }
            Arrays.sort(values);
            int index = Math.min(values.length - 1, (int) Math.ceil(percentile * values.length) - 1);
            return values[index];
        }

        private static double ms(double nanos) {
            return nanos / 1_000_000.0;
        }

        private static String escape(String text) {
            return String.valueOf(text).replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
        }

        private static int[] availablePorts(int count) throws IOException {
            List<ServerSocket> sockets = new ArrayList<>();
            try {
                int[] ports = new int[count];
                for (int i = 0; i < count; i++) {
                    ServerSocket socket = new ServerSocket(0);
                    sockets.add(socket);
                    ports[i] = socket.getLocalPort();
                }
                return ports;
            } finally {
                for (ServerSocket socket : sockets) {
                    socket.close();
                }
            }
        }
    }

    @FunctionalInterface
    private interface Operation {
        void execute() throws Exception;
    }

    private static final class MixOperationCounts {
        private final LongAdder readOperations = new LongAdder();
        private final LongAdder writeOperations = new LongAdder();
    }

    private static final class Counter {
        private final LongAdder operations = new LongAdder();
        private final LongAdder errors = new LongAdder();
        private final LongAdder timeouts = new LongAdder();
        private final AtomicInteger sampleCount = new AtomicInteger();
        private final AtomicReference<String> firstError = new AtomicReference<>();
        private final long[] latencySamples;
        private volatile long elapsedNanos;

        private Counter(int maxSamples) {
            latencySamples = new long[maxSamples];
        }

        private void record(long latency) {
            operations.increment();
            int slot = sampleCount.getAndIncrement();
            if (slot < latencySamples.length) {
                latencySamples[slot] = latency;
            }
        }

        private long[] samples() {
            return Arrays.copyOf(latencySamples, Math.min(sampleCount.get(), latencySamples.length));
        }
    }

    private static final class ClearSample {
        private final int iteration;
        private final long preClearCount;
        private final long postClearCount;
        private final long setupNanos;
        private final long clearNanos;
        private final long cycleNanos;

        private ClearSample(int iteration, long preClearCount, long postClearCount, long setupNanos, long clearNanos, long cycleNanos) {
            this.iteration = iteration;
            this.preClearCount = preClearCount;
            this.postClearCount = postClearCount;
            this.setupNanos = setupNanos;
            this.clearNanos = clearNanos;
            this.cycleNanos = cycleNanos;
        }
    }

    private static final class ClearRun {
        private final int expectedPopulation;
        private final int warmupIterations;
        private final String populationStatus;
        private final List<ClearSample> samples;
        private final long setupNanos;
        private final long clearNanos;
        private final long cycleNanos;

        private ClearRun(int expectedPopulation, int warmupIterations, String populationStatus, List<ClearSample> samples, long setupNanos, long clearNanos,
                        long cycleNanos) {
            this.expectedPopulation = expectedPopulation;
            this.warmupIterations = warmupIterations;
            this.populationStatus = populationStatus;
            this.samples = samples;
            this.setupNanos = setupNanos;
            this.clearNanos = clearNanos;
            this.cycleNanos = cycleNanos;
        }

        private String toJson() {
            StringBuilder json = new StringBuilder("{\"status\":\"").append(populationStatus).append("\",\"expectedPopulation\":").append(expectedPopulation)
                            .append(",\"warmupIterations\":").append(warmupIterations).append(",\"measuredIterations\":").append(samples.size())
                            .append(",\"setupNanosTotal\":").append(setupNanos).append(",\"clearNanosTotal\":").append(clearNanos)
                            .append(",\"cycleNanosTotal\":").append(cycleNanos).append(",\"errors\":0,\"cycleThroughputPerSecond\":")
                            .append(String.format(Locale.ROOT, "%.3f", cycleNanos == 0 ? 0.0 : samples.size() * 1_000_000_000.0 / cycleNanos))
                            .append(",\"throughputScope\":\"refill+pre/post-validation+clear cycle, not clear-only\",\"samples\":[");
            for (int i = 0; i < samples.size(); i++) {
                ClearSample sample = samples.get(i);
                json.append("{\"iteration\":").append(sample.iteration).append(",\"preClearCount\":").append(sample.preClearCount)
                                .append(",\"postClearCount\":").append(sample.postClearCount).append(",\"setupNanos\":").append(sample.setupNanos)
                                .append(",\"clearNanos\":").append(sample.clearNanos).append(",\"cycleNanos\":").append(sample.cycleNanos).append('}')
                                .append(i + 1 == samples.size() ? "" : ",");
            }
            return json.append("]}").toString();
        }

        private double cycleThroughputPerSecond() {
            return cycleNanos == 0 ? 0.0 : samples.size() * 1_000_000_000.0 / cycleNanos;
        }
    }

    private static final class IndexUse {
        private final long indexedQueries;
        private final long indexQueryCount;

        private IndexUse(long indexedQueries, long indexQueryCount) {
            this.indexedQueries = indexedQueries;
            this.indexQueryCount = indexQueryCount;
        }
    }

    private static final class Measurement {
        private final String name;
        private final String readKind;
        private final String queryScope;
        private final int expectedValuesPerRead;
        private final int payloadBytes;
        private final long indexedQueryDelta;
        private final long indexQueryDelta;
        private final long operations;
        private final long errors;
        private final long timeouts;
        private final String firstError;
        private final int sampleCount;
        private final double p50Nanos;
        private final double p95Nanos;
        private final double p99Nanos;
        private final double throughputPerSecond;
        private final long elapsedMs;
        private final boolean emptyResultIndexStatsException;
        private final int configuredReadPercent;
        private final int configuredWritePercent;
        private final long warmupReadOperations;
        private final long warmupWriteOperations;
        private final long measuredReadOperations;
        private final long measuredWriteOperations;

        private Measurement(String name, String readKind, String queryScope, int expectedValuesPerRead, int payloadBytes, long indexedQueryDelta,
                        long indexQueryDelta, long operations, long errors, long timeouts, String firstError, int sampleCount, double p50Nanos, double p95Nanos,
                        double p99Nanos, double throughputPerSecond, long elapsedMs, boolean emptyResultIndexStatsException, int configuredReadPercent,
                        int configuredWritePercent, long warmupReadOperations, long warmupWriteOperations, long measuredReadOperations,
                        long measuredWriteOperations) {
            this.name = name;
            this.readKind = readKind;
            this.queryScope = queryScope;
            this.expectedValuesPerRead = expectedValuesPerRead;
            this.payloadBytes = payloadBytes;
            this.indexedQueryDelta = indexedQueryDelta;
            this.indexQueryDelta = indexQueryDelta;
            this.operations = operations;
            this.errors = errors;
            this.timeouts = timeouts;
            this.firstError = firstError;
            this.sampleCount = sampleCount;
            this.p50Nanos = p50Nanos;
            this.p95Nanos = p95Nanos;
            this.p99Nanos = p99Nanos;
            this.throughputPerSecond = throughputPerSecond;
            this.elapsedMs = elapsedMs;
            this.emptyResultIndexStatsException = emptyResultIndexStatsException;
            this.configuredReadPercent = configuredReadPercent;
            this.configuredWritePercent = configuredWritePercent;
            this.warmupReadOperations = warmupReadOperations;
            this.warmupWriteOperations = warmupWriteOperations;
            this.measuredReadOperations = measuredReadOperations;
            this.measuredWriteOperations = measuredWriteOperations;
        }
    }

    private static final class Snapshot {
        private final String phase;
        private final long annotationEntries;
        private final long fetchEntries;
        private final int dataMapObjects;
        private final long ownedEntries;
        private final long heapCostEstimate;
        private final long indexMemoryEstimate;
        private final long indexedQueryCount;
        private final long indexQueryCount;
        private final long jvmUsedHeap;
        private final long jvmCommittedHeap;
        private final long jvmMaxHeap;

        private Snapshot(String phase, long annotationEntries, long fetchEntries, int dataMapObjects, long ownedEntries, long heapCostEstimate,
                        long indexMemoryEstimate, long indexedQueryCount, long indexQueryCount, long jvmUsedHeap, long jvmCommittedHeap, long jvmMaxHeap) {
            this.phase = phase;
            this.annotationEntries = annotationEntries;
            this.fetchEntries = fetchEntries;
            this.dataMapObjects = dataMapObjects;
            this.ownedEntries = ownedEntries;
            this.heapCostEstimate = heapCostEstimate;
            this.indexMemoryEstimate = indexMemoryEstimate;
            this.indexedQueryCount = indexedQueryCount;
            this.indexQueryCount = indexQueryCount;
            this.jvmUsedHeap = jvmUsedHeap;
            this.jvmCommittedHeap = jvmCommittedHeap;
            this.jvmMaxHeap = jvmMaxHeap;
        }
    }

    private static final class Settings {
        private final int members;
        private final int clients;
        private final int uniqueDocs;
        private final int annotationsPerDoc;
        private final int authContextsPerDoc;
        private final int payloadBytes;
        private final int concurrency;
        private final int readPercent;
        private final long warmupMs;
        private final long measuredMs;
        private final int clearPopulation;
        private final int clearIterations;
        private final int churnBatchRate;
        private final int churnBatchSize;
        private final long churnDurationMs;
        private final int annotationTtlSeconds;
        private final int fetchTtlSeconds;
        private final long settleMs;
        private final long seed;

        private Settings() {
            members = integer("members", 2, 1, 8);
            clients = integer("clients", 1, 1, 8);
            uniqueDocs = integer("uniqueDocs", 100, 1, 100_000);
            annotationsPerDoc = integer("annotationsPerDoc", 2, 0, 10_000);
            authContextsPerDoc = integer("authContextsPerDoc", 2, 1, 1_000);
            payloadBytes = integer("payloadBytes", 128, 0, 1_000_000);
            concurrency = integer("concurrency", 2, 1, 64);
            readPercent = integer("readPercent", 80, 0, 100);
            warmupMs = longValue("warmupMs", 500, 0, 60_000);
            measuredMs = longValue("measuredMs", 1_000, 100, 300_000);
            clearPopulation = integer("clearPopulation", -1, -1, 250_000);
            clearIterations = integer("clearIterations", 5, 1, 50);
            churnBatchRate = integer("churnBatchRate", 2, 1, 1_000);
            churnBatchSize = integer("churnBatchSize", 5, 1, 1_000);
            churnDurationMs = longValue("churnDurationMs", 2_000, 100, 300_000);
            annotationTtlSeconds = integer("annotationTtlSeconds", 6, 1, 86_400);
            fetchTtlSeconds = integer("fetchTtlSeconds", 3, 1, 86_400);
            settleMs = longValue("settleMs", 1_000, 0, 300_000);
            seed = Long.parseLong(System.getProperty("annotation.cache.scaling.seed", "20251023"));
            if (fetchTtlSeconds > annotationTtlSeconds) {
                throw new IllegalArgumentException("fetchTtlSeconds must not exceed annotationTtlSeconds");
            }
            long seededEntries = (long) uniqueDocs * (2L * annotationsPerDoc + authContextsPerDoc);
            long driftDocuments = ((long) churnBatchRate * churnBatchSize * (warmupMs + churnDurationMs) + 999) / 1000;
            long driftEntries = driftDocuments * (authContextsPerDoc + 1L);
            long clearEntries = effectiveClearPopulation();
            if (clearEntries + seededEntries + driftEntries > 250_000) {
                throw new IllegalArgumentException("configured live fixtures including clearPopulation exceed the 250000-entry safety limit");
            }
            if (clearEntries * clearIterations > 500_000L) {
                throw new IllegalArgumentException("clearPopulation × clearIterations exceeds the 500000 total-refill-entry safety limit");
            }
            long payloadEstimate = 2L * uniqueDocs * annotationsPerDoc * payloadBytes + driftDocuments * payloadBytes;
            if (payloadEstimate > 128L * 1024 * 1024) {
                throw new IllegalArgumentException("configured annotation payload estimate exceeds 128 MiB; reduce workload or plan capacity explicitly");
            }
        }

        private static Settings fromSystemProperties() {
            return new Settings();
        }

        private static int integer(String name, int defaultValue, int min, int max) {
            return bounded(name, Integer.parseInt(System.getProperty("annotation.cache.scaling." + name, Integer.toString(defaultValue))), min, max);
        }

        private static long longValue(String name, long defaultValue, long min, long max) {
            long value = Long.parseLong(System.getProperty("annotation.cache.scaling." + name, Long.toString(defaultValue)));
            if (value < min || value > max) {
                throw new IllegalArgumentException(name + " must be in [" + min + ", " + max + "]");
            }
            return value;
        }

        private static int bounded(String name, int value, int min, int max) {
            if (value < min || value > max) {
                throw new IllegalArgumentException(name + " must be in [" + min + ", " + max + "]");
            }
            return value;
        }

        private int effectiveClearPopulation() {
            return clearPopulation < 0 ? Math.multiplyExact(uniqueDocs, authContextsPerDoc) : clearPopulation;
        }

        private String toJson() {
            return "{\"members\":" + members + ",\"clients\":" + clients + ",\"uniqueDocs\":" + uniqueDocs + ",\"annotationsPerDoc\":" + annotationsPerDoc
                            + ",\"authContextsPerDoc\":" + authContextsPerDoc + ",\"clearPopulation\":" + clearPopulation + ",\"effectiveClearPopulation\":"
                            + effectiveClearPopulation() + ",\"clearIterations\":" + clearIterations + ",\"payloadBytes\":" + payloadBytes + ",\"concurrency\":"
                            + concurrency + ",\"readPercent\":" + readPercent + ",\"warmupMs\":" + warmupMs + ",\"measuredMs\":" + measuredMs
                            + ",\"churnBatchRate\":" + churnBatchRate + ",\"churnBatchSize\":" + churnBatchSize + ",\"churnDurationMs\":" + churnDurationMs
                            + ",\"annotationTtlSeconds\":" + annotationTtlSeconds + ",\"fetchTtlSeconds\":" + fetchTtlSeconds + ",\"settleMs\":" + settleMs
                            + ",\"seed\":" + seed + "}";
        }
    }
}
