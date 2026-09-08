package datawave.query.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Demonstrates the relative cost of the two ways a {@link TypeMetadata} instance can be round-tripped: the native {@link TypeMetadata#toString()} /
 * {@code new TypeMetadata(String)} text format, versus native Kryo serialization via {@link TypeMetadata#write(Kryo, Output)} /
 * {@link TypeMetadata#read(Kryo, Input)}. Runs across a matrix of small/medium/large datatype counts and field counts, since the two serialization approaches
 * scale differently with each dimension. This isn't a strict pass/fail gate on timing (wall-clock timing is noisy in CI); it verifies both round trips produce
 * an equivalent instance, and prints the relative throughput and payload size of each combination.
 * <p>
 * The matrix combinations are shuffled before running so that no single ordering (e.g. always smallest-first) biases JIT warmup across combinations, but
 * results are collected and printed in sorted order afterward so the report is easy to read.
 */
public class TypeMetadataSerializationPerformanceTest {

    private static final int[] DATATYPE_COUNTS = {1, 5, 20};
    private static final int[] FIELD_COUNTS = {25, 250, 2_500};
    // fully qualified, as they are in a TypeMetadata built from the metadata table, since their length is what the
    // mini-map convention saves on and a short name would understate it
    private static final String[] NORMALIZERS = {"datawave.data.type.LcNoDiacriticsType", "datawave.data.type.NumberType", "datawave.data.type.DateType",
            "datawave.data.type.GeometryType"};

    // total field entries processed per timed round trip, held roughly constant across the matrix so the smallest
    // combination doesn't run for a trivial fraction of a millisecond and the largest doesn't dominate the build
    private static final long TIMED_ENTRY_BUDGET = 500_000L;
    private static final int MIN_TIMED_ITERATIONS = 3;
    private static final int MAX_TIMED_ITERATIONS = 2_000;
    private static final int WARMUP_ITERATIONS = 20;

    private static final List<Result> RESULTS = Collections.synchronizedList(new ArrayList<>());

    private static final class Result {
        private final int datatypeCount;
        private final int fieldCount;
        private final int iterations;
        private final double stringMsPerOp;
        private final double kryoMsPerOp;
        private final int stringBytes;
        private final int kryoBytes;

        private Result(int datatypeCount, int fieldCount, int iterations, double stringMsPerOp, double kryoMsPerOp, int stringBytes, int kryoBytes) {
            this.datatypeCount = datatypeCount;
            this.fieldCount = fieldCount;
            this.iterations = iterations;
            this.stringMsPerOp = stringMsPerOp;
            this.kryoMsPerOp = kryoMsPerOp;
            this.stringBytes = stringBytes;
            this.kryoBytes = kryoBytes;
        }

        private double speedup() {
            return stringMsPerOp / kryoMsPerOp;
        }

        private double relativeSize() {
            return (double) kryoBytes / stringBytes;
        }
    }

    private static Stream<Arguments> matrix() {
        List<Arguments> combinations = new ArrayList<>();
        for (int datatypeCount : DATATYPE_COUNTS) {
            for (int fieldCount : FIELD_COUNTS) {
                combinations.add(Arguments.of(datatypeCount, fieldCount));
            }
        }
        // shuffle so the timing runs don't always execute smallest-to-largest, which would let later (larger)
        // combinations benefit from JIT warmup accumulated by earlier (smaller) ones
        Collections.shuffle(combinations);
        return combinations.stream();
    }

    @AfterAll
    public static void printResultsSortedByDatatypeThenFieldCount() {
        List<Result> sorted = new ArrayList<>(RESULTS);
        sorted.sort(Comparator.<Result> comparingInt(r -> r.datatypeCount).thenComparingInt(r -> r.fieldCount));

        System.out.println("TypeMetadata serialization round-trip: toString()/fromString() vs Kryo");
        for (Result result : sorted) {
            System.out.printf(
                            "%2d datatypes x %5d fields (%d iterations): toString/fromString=%.4f ms/op, Kryo=%.4f ms/op, speedup=%.2fx, "
                                            + "toString=%d bytes, Kryo=%d bytes, relative size=%.2fx%n",
                            result.datatypeCount, result.fieldCount, result.iterations, result.stringMsPerOp, result.kryoMsPerOp, result.speedup(),
                            result.stringBytes, result.kryoBytes, result.relativeSize());
        }
    }

    private TypeMetadata buildTypeMetadata(int datatypeCount, int fieldCount) {
        TypeMetadata typeMetadata = new TypeMetadata();
        for (int d = 0; d < datatypeCount; d++) {
            String ingestType = "datatype" + d;
            for (int f = 0; f < fieldCount; f++) {
                String field = "FIELD_" + f;
                String normalizer = NORMALIZERS[f % NORMALIZERS.length];
                typeMetadata.put(field, ingestType, normalizer);
            }
        }
        return typeMetadata;
    }

    private byte[] toKryoBytes(Kryo kryo, TypeMetadata typeMetadata) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (Output output = new Output(baos)) {
            typeMetadata.write(kryo, output);
        }
        return baos.toByteArray();
    }

    private TypeMetadata fromKryoBytes(Kryo kryo, byte[] bytes) {
        TypeMetadata typeMetadata = new TypeMetadata();
        try (Input input = new Input(bytes)) {
            typeMetadata.read(kryo, input);
        }
        return typeMetadata;
    }

    @ParameterizedTest(name = "{0} datatypes x {1} fields")
    @MethodSource("matrix")
    public void kryoRoundTripIsEquivalentToStringRoundTrip(int datatypeCount, int fieldCount) {
        TypeMetadata original = buildTypeMetadata(datatypeCount, fieldCount);
        Kryo kryo = new Kryo();

        TypeMetadata viaString = new TypeMetadata(original.toString());
        TypeMetadata viaKryo = fromKryoBytes(kryo, toKryoBytes(kryo, original));

        assertEquals(original, viaString);
        assertEquals(original, viaKryo);
        assertEquals(original.fold(), viaKryo.fold());
    }

    @ParameterizedTest(name = "{0} datatypes x {1} fields")
    @MethodSource("matrix")
    public void compareToStringVersusKryoRoundTripSpeed(int datatypeCount, int fieldCount) {
        TypeMetadata original = buildTypeMetadata(datatypeCount, fieldCount);
        Kryo kryo = new Kryo();

        long totalEntries = (long) datatypeCount * fieldCount;
        int iterations = (int) Math.max(MIN_TIMED_ITERATIONS, Math.min(MAX_TIMED_ITERATIONS, TIMED_ENTRY_BUDGET / totalEntries));

        // warm up the JIT for both code paths before timing
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            new TypeMetadata(original.toString());
            fromKryoBytes(kryo, toKryoBytes(kryo, original));
        }

        long stringStart = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            new TypeMetadata(original.toString());
        }
        long stringElapsedNanos = System.nanoTime() - stringStart;

        long kryoStart = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            fromKryoBytes(kryo, toKryoBytes(kryo, original));
        }
        long kryoElapsedNanos = System.nanoTime() - kryoStart;

        double stringMsPerOp = (stringElapsedNanos / 1_000_000.0) / iterations;
        double kryoMsPerOp = (kryoElapsedNanos / 1_000_000.0) / iterations;

        int stringBytes = original.toString().getBytes(StandardCharsets.UTF_8).length;
        int kryoBytes = toKryoBytes(kryo, original).length;

        RESULTS.add(new Result(datatypeCount, fieldCount, iterations, stringMsPerOp, kryoMsPerOp, stringBytes, kryoBytes));
    }
}
