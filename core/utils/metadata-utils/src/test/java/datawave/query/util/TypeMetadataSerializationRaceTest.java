package datawave.query.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

/**
 * Demonstrates that {@link TypeMetadata#toString()} mutates instance state, which is unsafe because a {@link TypeMetadata} is shared between concurrent queries
 * via the metadata helper cache.
 */
public class TypeMetadataSerializationRaceTest {

    private static final int THREADS = 8;
    private static final int ROUNDS = 500;

    private static TypeMetadata newTypeMetadata() {
        TypeMetadata typeMetadata = new TypeMetadata();
        for (int i = 0; i < 40; i++) {
            typeMetadata.put("FIELD_" + i, "ingest_" + (i % 7), "datawave.data.type.LcNoDiacriticsType");
            typeMetadata.put("FIELD_" + i, "ingest_" + (i % 5), "datawave.data.type.NumberType");
            typeMetadata.put("FIELD_" + i, "ingest_" + (i % 3), "datawave.data.type.DateType");
        }
        return typeMetadata;
    }

    /**
     * Serialization must not write to the instance. A cached {@link TypeMetadata} is handed to every query that shares the same auths and datatype filter, so a
     * write here is a write to state other threads are reading.
     */
    @Test
    public void testToStringDoesNotMutateTheInstance() {
        TypeMetadata typeMetadata = newTypeMetadata();

        typeMetadata.setIngestTypesMiniMap(Collections.unmodifiableMap(new TreeMap<>()));
        typeMetadata.setDataTypesMiniMap(Collections.unmodifiableMap(new TreeMap<>()));

        try {
            typeMetadata.toString();
        } catch (UnsupportedOperationException e) {
            fail("TypeMetadata.toString() wrote to the instance's mini-maps");
        }
    }

    /**
     * Serializing the same instance from several threads must produce the same string every time. The mini-maps are built on the first serialization, so the
     * structural writes only race the first time an instance is serialized, which is why this only bites shortly after a metadata cache refresh.
     */
    @Test(timeout = 20_000)
    public void testConcurrentSerializationIsConsistent() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);
        try {
            for (int round = 0; round < ROUNDS; round++) {
                TypeMetadata shared = newTypeMetadata();
                String expected = newTypeMetadata().toString();

                CyclicBarrier barrier = new CyclicBarrier(THREADS);
                // @formatter:off
                List<Callable<String>> tasks = IntStream.range(0, THREADS)
                                .mapToObj(i -> (Callable<String>) () -> {
                                    barrier.await(30, TimeUnit.SECONDS);
                                    return shared.toString();
                                })
                                .collect(Collectors.toList());
                // @formatter:on

                for (Future<String> future : executor.invokeAll(tasks)) {
                    assertEquals("round " + round + " serialized a shared TypeMetadata inconsistently", expected, future.get());
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * A cache miss against the metadata table yields an empty {@link TypeMetadata}. Serializing it must produce something the tablet servers can parse back.
     */
    @Test
    public void testEmptyTypeMetadataRoundTrips() {
        String serialized = new TypeMetadata().toString();
        assertTrue("empty TypeMetadata serialized to an unterminated string: " + serialized, serialized.endsWith("]") || serialized.endsWith("];"));
        assertTrue(new TypeMetadata(serialized).isEmpty());
    }

    /**
     * A datatype filter naming an ingest type the metadata table has no entry for must fail loudly. Folding it away would normalize that datatype's values
     * against the wrong types and silently drop valid results.
     */
    @Test
    public void testFoldWithUnknownIngestTypeThrows() {
        TypeMetadata typeMetadata = newTypeMetadata();
        Set<String> filter = Collections.singleton("ingest_not_in_the_metadata_table");

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> typeMetadata.fold(filter));
        assertTrue(e.getMessage(), e.getMessage().contains("ingest_not_in_the_metadata_table"));
    }

    /**
     * A malformed mini-map must fail loudly rather than resolve field entries to the wrong types.
     */
    @Test
    public void testMalformedMiniMapThrows() {
        assertThrows(IllegalStateException.class, () -> new TypeMetadata("dts:[0:ingestA];types:[bogus];FIELD:[0:0]"));
    }

    /**
     * An unclosed mini-map bracket hides the delimiters that follow it, so the ingest type entry swallows the normalizer entry. Parsed leniently it yields an
     * ingest type named "ingestB;types" and no normalizer mini-map at all, leaving every field to resolve its normalizer to "".
     */
    @Test
    public void testUnterminatedIngestTypeMiniMapThrows() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                        () -> new TypeMetadata("dts:[0:ingestA,1:ingestB;types:[0:LcNoDiacriticsType];FIELD1:[0:0];FIELD2:[0:0]"));
        assertTrue(e.getMessage(), e.getMessage().contains("closing bracket"));
    }

    /**
     * The same truncation in the normalizer mini-map, which leniently names index 0 "LcNoDiacriticsType;FIELD1" and so resolves every field holding it to a
     * normalizer that does not exist.
     */
    @Test
    public void testUnterminatedNormalizerMiniMapThrows() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                        () -> new TypeMetadata("dts:[0:ingestA];types:[0:LcNoDiacriticsType;FIELD1:[0:0];FIELD2:[0:0]"));
        assertTrue(e.getMessage(), e.getMessage().contains("closing bracket"));
    }

    /**
     * A field entry with no bracketed list at all, which used to fail on an array index rather than say what was wrong.
     */
    @Test
    public void testFieldEntryWithoutListThrows() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> new TypeMetadata("dts:[0:ingestA];types:[0:LcNoDiacriticsType];FIELD"));
        assertTrue(e.getMessage(), e.getMessage().contains("FIELD"));
    }

    /**
     * A field entry whose list is never closed, which used to drop the last index pair and then fail on an array index.
     */
    @Test
    public void testUnterminatedFieldEntryThrows() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> new TypeMetadata("dts:[0:ingestA];types:[0:LcNoDiacriticsType];FIELD:[0:0"));
        assertTrue(e.getMessage(), e.getMessage().contains("bracketed list"));
    }

    /**
     * A field entry holding an index with no normalizer index paired to it.
     */
    @Test
    public void testMalformedIndexPairThrows() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> new TypeMetadata("dts:[0:ingestA];types:[0:LcNoDiacriticsType];FIELD:[0]"));
        assertTrue(e.getMessage(), e.getMessage().contains("index pair"));
    }

    /**
     * The guards above must not reject a trailing delimiter, which is an empty entry rather than a malformed field.
     */
    @Test
    public void testTrailingDelimiterStillParses() {
        TypeMetadata typeMetadata = new TypeMetadata("dts:[0:ingestA];types:[0:LcNoDiacriticsType];FIELD:[0:0];");
        assertEquals(Collections.singleton("LcNoDiacriticsType"), typeMetadata.getNormalizerNamesForField("FIELD"));
    }
}
