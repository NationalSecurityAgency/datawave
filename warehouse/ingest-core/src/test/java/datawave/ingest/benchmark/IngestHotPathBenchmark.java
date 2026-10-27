package datawave.ingest.benchmark;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.normalizer.Normalizer;
import datawave.data.type.BaseType;
import datawave.data.type.DateType;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.data.config.ingest.AbstractContentIngestHelper;
import datawave.ingest.data.config.ingest.ContentBaseIngestHelper;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.handler.tokenize.ContentIndexingColumnBasedHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.policy.IngestPolicyEnforcer;
import datawave.table.constants.TableName;

/**
 * Benchmark and structural-instrumentation harness for the ingest hot path, {@link ShardedDataTypeHandler#processBulk}.
 * <p>
 * This is a command line utility, not a test. It carries no test annotations and declares no test methods, so no build or CI configuration has to know it
 * exists; it lives under the test sources only because it needs the test-scoped dependencies and {@link BenchmarkEventGenerator}. Run it with
 * {@link #main(String[])}.
 * <p>
 * Measurements come in two kinds:
 * <ul>
 * <li><b>Structural</b> — deterministic call counts collected by {@link InstrumentedHandler} and the counting types, aggregated over a full pass of the event
 * pool. These are exact and independent of machine speed, so a violation throws.</li>
 * <li><b>Timing</b> — wall clock and per-thread allocation, printed as a table and never checked, since it depends on the machine.</li>
 * </ul>
 * Test data comes from {@link BenchmarkEventGenerator}: pools of at least {@link BenchmarkEventGenerator#DEFAULT_EVENT_COUNT} events sized between
 * {@link BenchmarkEventGenerator#MIN_FIELDS} and {@link BenchmarkEventGenerator#MAX_FIELDS} fields, covering every concrete normalizer type and rotating event
 * and field visibilities.
 * <p>
 * Iteration counts are overridable with {@code -Ddatawave.benchmark.iterations=N} and {@code -Ddatawave.benchmark.warmup=N}.
 */
public class IngestHotPathBenchmark {

    private static final int NUM_SHARDS = 241;

    private static final int WARMUP_ITERATIONS = Integer.getInteger("datawave.benchmark.warmup", 1500);
    private static final int MEASURED_ITERATIONS = Integer.getInteger("datawave.benchmark.iterations", 5000);

    /** Events per pool. Never below the generator's floor. */
    private static final int EVENT_COUNT = Math.max(BenchmarkEventGenerator.DEFAULT_EVENT_COUNT, Integer.getInteger("datawave.benchmark.events", 250));

    /**
     * Fixed event sizes for the scaling sweep, spanning the generator's size range. The sweep needs one size at a time to separate linear growth from a
     * quadratic term; the mixed-size arm is measured separately by {@link #benchmarkVariableEventSizes()}.
     */
    private static final int[] SWEEP_FIELD_COUNTS = {15, 25, 30, 35, 45};

    /** Mark every Nth field with its own visibility in the diverse arms. */
    private static final int MARKED_FIELD_STRIDE = 4;

    /** Value used by the uniform arm when no type-specific one is needed. */
    private static final String UNIFORM_VALUE = "SomeMixedCase Value-payload";

    private static final com.sun.management.ThreadMXBean THREAD_BEAN = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();

    /**
     * Each scenario gets its own datatype name. {@link Type#getIngestHelper} caches helpers in a static map keyed by {@link Type}, whose equality is based on
     * the type name and helper class but not on the {@link Configuration}; {@code TypeRegistry.reset()} does not clear that map. Reusing one name across
     * scenarios therefore silently hands back the first scenario's helper, with the first scenario's indexed-field set.
     */
    private static final AtomicInteger SCENARIO_SEQ = new AtomicInteger();

    /** Counts datawave Type normalizer invocations. Reset per scenario. */
    static final AtomicLong NORMALIZE_CALLS = new AtomicLong();

    /** Whether the generated fields are configured as indexed or as normalized-but-not-indexed. */
    enum FieldMode {
        INDEXED, NORMALIZED_ONLY
    }

    /** How much variety the test data carries. */
    enum DataVariety {
        /** One cheap type, one value, no field markings — the original shape. */
        UNIFORM,
        /** Every concrete normalizer, rotating event visibilities, strided field markings. */
        DIVERSE
    }

    // ---------------------------------------------------------------- instrumentation

    /**
     * A cheap Type that counts {@code normalize} invocations, so the once-per-value check does not depend on machine speed.
     */
    public static class CountingType extends BaseType<String> {
        private static final long serialVersionUID = 1L;

        public CountingType() {
            super(Normalizer.LC_NO_DIACRITICS_NORMALIZER);
        }

        @Override
        public String normalize(String in) {
            NORMALIZE_CALLS.incrementAndGet();
            return super.normalize(in);
        }
    }

    /**
     * The same counter over an expensive normalizer. {@code DateNormalizer} walks its format list until one parses, so an extra invocation shows up in the
     * timings, not only the counts.
     */
    public static class CountingDateType extends DateType {
        private static final long serialVersionUID = 1L;

        @Override
        public String normalize(String in) {
            NORMALIZE_CALLS.incrementAndGet();
            return super.normalize(in);
        }
    }

    /**
     * Handler that counts the protected extension points named in the audit, so per-pass call counts can be compared directly against the predicted ones.
     * Extends the content-indexing handler so tokenized fields run the Lucene analyzer and emit term frequency keys, both of which happen inside the measured
     * {@code processBulk}.
     */
    public static class InstrumentedHandler extends ContentIndexingColumnBasedHandler<Text> {
        final AtomicLong flattenCalls = new AtomicLong();

        void resetCounts() {
            flattenCalls.set(0);
        }

        @Override
        protected byte[] flatten(ColumnVisibility vis) {
            flattenCalls.incrementAndGet();
            return super.flatten(vis);
        }

        @Override
        public AbstractContentIngestHelper getContentIndexingDataTypeHelper() {
            return (BenchIngestHelper) helper;
        }
    }

    /** Ingest helper that parses the synthetic {@code NAME=value;NAME=value} payload. */
    public static class BenchIngestHelper extends ContentBaseIngestHelper {
        @Override
        public Multimap<String,NormalizedContentInterface> getEventFields(RawRecordContainer record) {
            Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
            String raw = new String(record.getRawData(), StandardCharsets.UTF_8);
            for (String pair : raw.split(";")) {
                int eq = pair.indexOf('=');
                String name = pair.substring(0, eq);
                eventFields.put(name, new NormalizedFieldAndValue(name, pair.substring(eq + 1)));
            }
            return normalizeMap(eventFields);
        }

        @Override
        public String getNormalizedMaskedValue(final String key) {
            return "MASKED_VALUE";
        }
    }

    // ---------------------------------------------------------------- harness

    /** One measured scenario's results, aggregated over a full pass of the event pool. */
    static final class Result {
        final String name;
        final int events;
        final double meanFields;
        final int minFields;
        final int maxFields;
        final long medianNanos;
        final long meanNanos;
        final long p90Nanos;
        final long bytesPerEvent;
        final long keysPerPass;
        final long flattenPerPass;
        final long totalFieldsPerPass;
        /** keys/pass split by kind: event, field index, global index, term frequency. */
        final long[] keyKinds;

        Result(String name, int events, double meanFields, int minFields, int maxFields, long medianNanos, long meanNanos, long p90Nanos, long bytesPerEvent,
                        long keysPerPass, long flattenPerPass, long totalFieldsPerPass, long[] keyKinds) {
            this.name = name;
            this.events = events;
            this.meanFields = meanFields;
            this.minFields = minFields;
            this.maxFields = maxFields;
            this.medianNanos = medianNanos;
            this.meanNanos = meanNanos;
            this.p90Nanos = p90Nanos;
            this.bytesPerEvent = bytesPerEvent;
            this.keysPerPass = keysPerPass;
            this.flattenPerPass = flattenPerPass;
            this.totalFieldsPerPass = totalFieldsPerPass;
            this.keyKinds = keyKinds;
        }

        /**
         * Size-normalized cost, the figure to compare across scenarios of differing event size. Based on the mean rather than the median: in a pool of mixed
         * event sizes the median sample is a median-sized event, not an average one, so a median-based rate understates a heterogeneous pool against a uniform
         * one. For uniform pools the two agree.
         */
        double nanosPerKey() {
            return (meanNanos * (double) events) / Math.max(1, keysPerPass);
        }
    }

    private final List<Result> results = new ArrayList<>();

    private Configuration baseConfiguration(String dataTypeName, FieldMode mode, DataVariety variety, String uniformType) {
        Configuration conf = new Configuration();
        conf.set(dataTypeName + DataTypeHelper.Properties.INGEST_POLICY_ENFORCER_CLASS, IngestPolicyEnforcer.NoOpIngestPolicyEnforcer.class.getName());
        conf.set(DataTypeHelper.Properties.DATA_NAME, dataTypeName);
        conf.set(TypeRegistry.INGEST_DATA_TYPES, dataTypeName);
        conf.set(dataTypeName + TypeRegistry.INGEST_HELPER, BenchIngestHelper.class.getName());

        conf.set(ShardedDataTypeHandler.METADATA_TABLE_NAME, TableName.METADATA);
        conf.set(ShardedDataTypeHandler.NUM_SHARDS, Integer.toString(NUM_SHARDS));
        conf.set(ShardedDataTypeHandler.SHARDED_TNAMES, TableName.SHARD + "," + TableName.ERROR_SHARD);
        conf.set(ShardedDataTypeHandler.SHARD_TNAME, TableName.SHARD);
        conf.set(ShardedDataTypeHandler.SHARD_LPRIORITY, "30");
        conf.set(TableName.SHARD + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX, ShardTableConfigHelper.class.getName());
        conf.set(ShardedDataTypeHandler.SHARD_GIDX_TNAME, TableName.SHARD_INDEX);
        conf.set(ShardedDataTypeHandler.SHARD_GIDX_LPRIORITY, "30");
        conf.set(TableName.SHARD_INDEX + TableConfigHelper.TABLE_CONFIG_CLASS_SUFFIX, ShardTableConfigHelper.class.getName());
        conf.set(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, TableName.SHARD_RINDEX);
        conf.set(ShardedDataTypeHandler.SHARD_GRIDX_LPRIORITY, "30");

        // declare the full field-name space, so a short event's fields are configured just like a long one's
        String allFields = String.join(",", BenchmarkEventGenerator.configuredFieldNames());
        if (mode == FieldMode.NORMALIZED_ONLY) {
            // normalized but NOT indexed: this is the branch that reaches
            // BaseIngestHelper.normalizeFieldValue(NormalizedContentInterface, Type)
            conf.set(dataTypeName + ".data.category.normalized", allFields);
        } else if (variety == DataVariety.UNIFORM) {
            // single category, used by the normalization comparisons
            conf.set(dataTypeName + ".data.category.index", allFields);
        } else {
            // four categories: event-only fields are simply absent from every index list
            List<String> indexed = BenchmarkEventGenerator.fieldNamesFor(FieldCategory.INDEXED);
            List<String> indexOnly = BenchmarkEventGenerator.fieldNamesFor(FieldCategory.INDEX_ONLY);
            List<String> tokenized = BenchmarkEventGenerator.fieldNamesFor(FieldCategory.TOKENIZED_INDEX_ONLY);

            List<String> allIndexed = new ArrayList<>(indexed);
            allIndexed.addAll(indexOnly);
            allIndexed.addAll(tokenized);
            conf.set(dataTypeName + ".data.category.index", String.join(",", allIndexed));

            List<String> notStored = new ArrayList<>(indexOnly);
            notStored.addAll(tokenized);
            conf.set(dataTypeName + ".data.category.index.only", String.join(",", notStored));

            conf.set(dataTypeName + ".data.category.index.tokenize.allowlist", String.join(",", tokenized));
            // keep the token field name equal to the base field name, matching the deployed no-designator mode
            conf.setBoolean(dataTypeName + ".data.category.token.fieldname.designator.enabled", false);
        }

        if (variety == DataVariety.DIVERSE) {
            // one Type per field, round robin over every concrete normalizer except on tokenized fields
            for (int i = 0; i < BenchmarkEventGenerator.MAX_FIELDS; i++) {
                conf.set(dataTypeName + ".FIELD_" + i + ".data.field.type.class", BenchmarkEventGenerator.typeClassForCategory(i));
            }
            conf.set(dataTypeName + ".data.default.type.class", "datawave.data.type.NoOpType");
        } else {
            conf.set(dataTypeName + ".data.default.type.class", uniformType);
        }

        return conf;
    }

    /**
     * Set once the JVM has executed the tokenized key-generation path enough for C2 to compile it. Without this the first scenario measured in a JVM pays
     * compilation and, more visibly, allocates around 30% more per event because escape analysis has not yet kicked in — which silently made whichever scenario
     * ran first look slower than the identical workload measured later.
     */
    private static boolean jvmWarmed = false;

    public void setUp() throws Exception {
        TypeRegistry.reset();
        if (!jvmWarmed) {
            jvmWarmed = true;
            measure("jvm-warmup", BenchmarkEventGenerator.fixedFieldCounts(20, 30), FieldMode.INDEXED, DataVariety.DIVERSE, CountingType.class.getName(),
                            UNIFORM_VALUE, MARKED_FIELD_STRIDE, 3000, 3000);
            results.clear();
            TypeRegistry.reset();
        }
    }

    private Result measure(String name, int[] fieldCounts) throws Exception {
        return measure(name, fieldCounts, FieldMode.INDEXED, DataVariety.DIVERSE, CountingType.class.getName(), UNIFORM_VALUE, MARKED_FIELD_STRIDE);
    }

    private Result measure(String name, int[] fieldCounts, FieldMode mode, DataVariety variety, String uniformType, String uniformValue, int markedStride)
                    throws Exception {
        return measure(name, fieldCounts, mode, variety, uniformType, uniformValue, markedStride, WARMUP_ITERATIONS, MEASURED_ITERATIONS);
    }

    private Result measure(String name, int[] fieldCounts, FieldMode mode, DataVariety variety, String uniformType, String uniformValue, int markedStride,
                    int warmupIterations, int measuredIterations) throws Exception {
        return measure(name, fieldCounts, mode, variety, uniformType, uniformValue, markedStride, warmupIterations, measuredIterations, null);
    }

    private Result measure(String name, int[] fieldCounts, FieldMode mode, DataVariety variety, String uniformType, String uniformValue, int markedStride,
                    int warmupIterations, int measuredIterations, String contentOverride) throws Exception {
        String dataTypeName = "bench" + SCENARIO_SEQ.incrementAndGet();
        Configuration conf = baseConfiguration(dataTypeName, mode, variety, uniformType);
        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);

        InstrumentedHandler handler = new InstrumentedHandler();
        handler.setup(new TaskAttemptContextImpl(conf, new TaskAttemptID()));

        BenchIngestHelper helper = new BenchIngestHelper();
        helper.setup(conf);

        Type dataType = TypeRegistry.getType(dataTypeName);
        List<RawRecordContainer> pool = BenchmarkEventGenerator.eventPool(fieldCounts, dataType, variety == DataVariety.DIVERSE, uniformValue, contentOverride);

        // pre-normalize each event's fields once, then annotate with per-field markings
        List<Multimap<String,NormalizedContentInterface>> fieldSets = new ArrayList<>(pool.size());
        for (RawRecordContainer record : pool) {
            Multimap<String,NormalizedContentInterface> f = helper.getEventFields(record);
            BenchmarkEventGenerator.applyFieldMarkings(f, variety == DataVariety.DIVERSE ? markedStride : 0);
            fieldSets.add(f);
        }

        int poolSize = pool.size();
        for (int i = 0; i < warmupIterations; i++) {
            int p = i % poolSize;
            handler.processBulk(null, pool.get(p), fieldSets.get(p), null);
        }

        // one clean instrumented pass over the whole pool for the structural counts
        handler.resetCounts();
        long keysPerPass = 0;
        long[] keyKinds = new long[KEY_KIND_COUNT];
        int structuralEvents = poolSize;
        for (int p = 0; p < structuralEvents; p++) {
            Multimap<BulkIngestKey,Value> out = handler.processBulk(null, pool.get(p), fieldSets.get(p), null);
            keysPerPass += out.size();
            for (BulkIngestKey bik : out.keySet()) {
                keyKinds[classify(bik)] += out.get(bik).size();
            }
        }
        long flattenPerPass = handler.flattenCalls.get();

        long[] samples = new long[measuredIterations];
        long allocStart = getCurrentThreadAllocatedBytes();
        for (int i = 0; i < measuredIterations; i++) {
            int p = i % poolSize;
            long t0 = System.nanoTime();
            handler.processBulk(null, pool.get(p), fieldSets.get(p), null);
            samples[i] = System.nanoTime() - t0;
        }
        long allocEnd = getCurrentThreadAllocatedBytes();

        long sum = 0;
        for (long sample : samples) {
            sum += sample;
        }
        long mean = sum / samples.length;
        Arrays.sort(samples);
        long median = samples[samples.length / 2];
        long p90 = samples[(int) (samples.length * 0.9)];
        long bytesPerEvent = (allocEnd - allocStart) / measuredIterations;

        long totalFields = 0;
        int min = Integer.MAX_VALUE;
        int max = 0;
        for (int c : fieldCounts) {
            totalFields += c;
            min = Math.min(min, c);
            max = Math.max(max, c);
        }
        // structural counters cover structuralEvents, so scale the field total to match
        long fieldsCounted = 0;
        for (int p = 0; p < structuralEvents; p++) {
            fieldsCounted += fieldCounts[p];
        }

        Result r = new Result(name, structuralEvents, totalFields / (double) poolSize, min, max, median, mean, p90, bytesPerEvent, keysPerPass, flattenPerPass,
                        fieldsCounted, keyKinds);
        results.add(r);
        return r;
    }

    static final int KEY_EVENT = 0;
    static final int KEY_FIELD_INDEX = 1;
    static final int KEY_GLOBAL_INDEX = 2;
    static final int KEY_TERM_FREQUENCY = 3;
    static final int KEY_KIND_COUNT = 4;
    private static final String[] KEY_KIND_NAMES = {"event", "fi", "index", "tf"};

    /**
     * Buckets a generated key by the structure it belongs to. Shard-table keys carry either the {@code tf} column family, an {@code fi\0FIELD} column family,
     * or the datatype/uid column family of an event key; anything on another table is a global index key.
     *
     * @param bik
     *            the generated key
     * @return an index into the key-kind array
     */
    private static int classify(BulkIngestKey bik) {
        if (!TableName.SHARD.equals(bik.getTableName().toString())) {
            return KEY_GLOBAL_INDEX;
        }
        String colf = bik.getKey().getColumnFamily().toString();
        if ("tf".equals(colf)) {
            return KEY_TERM_FREQUENCY;
        }
        if (colf.startsWith("fi\u0000")) {
            return KEY_FIELD_INDEX;
        }
        return KEY_EVENT;
    }

    // ---------------------------------------------------------------- scenarios

    /**
     * Sweeps a fixed event size across the generator's range. Per-key cost that stays flat as event size grows indicates linear scaling; per-key cost that
     * itself grows indicates a quadratic term.
     */
    public void benchmarkCreateColumnsScaling() throws Exception {
        for (int fields : SWEEP_FIELD_COUNTS) {
            measure("fields/" + fields, BenchmarkEventGenerator.fixedFieldCounts(EVENT_COUNT, fields));
        }
        printTable();
    }

    /**
     * The realistic workload: a pool of mixed-size events rather than one shape repeated. Compared against a fixed-size pool at the same mean, which isolates
     * whether size variation itself costs anything.
     */
    public void benchmarkVariableEventSizes() throws Exception {
        int[] variable = BenchmarkEventGenerator.variableFieldCounts(EVENT_COUNT);
        int mean = (int) Math.round(Arrays.stream(variable).average().orElse(30));

        Result fixed = measure("fixed at mean", BenchmarkEventGenerator.fixedFieldCounts(EVENT_COUNT, mean));
        Result mixed = measure("mixed 15-45", variable);

        printTable();
        System.out.printf("mixed-size pool: %d events, %d..%d fields, mean %.1f%n", mixed.events, mixed.minFields, mixed.maxFields, mixed.meanFields);
        System.out.printf("ns/key mixed %.1f vs fixed-at-mean %.1f (%.1f%%)%n", mixed.nanosPerKey(), fixed.nanosPerKey(),
                        100.0 * (mixed.nanosPerKey() - fixed.nanosPerKey()) / fixed.nanosPerKey());
    }

    /**
     * The audit predicted the event ColumnVisibility is re-flattened once per field, once per forward term and once per reverse term, rather than once per
     * event.
     */
    public void visibilityIsFlattenedPerFieldNotPerEvent() throws Exception {
        Result r = measure("flatten-probe", BenchmarkEventGenerator.variableFieldCounts(EVENT_COUNT));
        check(r.flattenPerPass > r.events, "expected far more flatten calls than events, got " + r.flattenPerPass + " over " + r.events);
        check(r.flattenPerPass >= 2 * r.totalFieldsPerPass, "expected at least two flatten calls per field");
        System.out.printf("flatten calls over %d events (%d fields): %d (%.2f per field)%n", r.events, r.totalFieldsPerPass, r.flattenPerPass,
                        r.flattenPerPass / (double) r.totalFieldsPerPass);
    }

    /**
     * Both BaseIngestHelper branches normalize each value exactly once, the indexed path through {@code normalize} and the normalized-only path through
     * {@code normalizeFieldValue}. Counted over a whole pass so the ratio holds across event sizes.
     */
    public void normalizerInvocationsPerField() throws Exception {
        String cheap = CountingType.class.getName();
        String pricey = CountingDateType.class.getName();
        String dateValue = "2024-06-15T10:30:00.000Z";
        int[] variable = BenchmarkEventGenerator.variableFieldCounts(EVENT_COUNT);
        long totalFields = Arrays.stream(variable).sum();

        long indexedCheap = countNormalizations(variable, FieldMode.INDEXED, cheap, UNIFORM_VALUE);
        long onlyCheap = countNormalizations(variable, FieldMode.NORMALIZED_ONLY, cheap, UNIFORM_VALUE);
        long indexedDate = countNormalizations(variable, FieldMode.INDEXED, pricey, dateValue);
        long onlyDate = countNormalizations(variable, FieldMode.NORMALIZED_ONLY, pricey, dateValue);

        System.out.printf("over %d events (%d fields total)%n", variable.length, totalFields);
        System.out.printf("  INDEXED cheap:          %d (%.2f per field)%n", indexedCheap, indexedCheap / (double) totalFields);
        System.out.printf("  NORMALIZED-ONLY cheap:  %d (%.2f per field)%n", onlyCheap, onlyCheap / (double) totalFields);
        System.out.printf("  INDEXED DateType:       %d (%.2f per field)%n", indexedDate, indexedDate / (double) totalFields);
        System.out.printf("  NORMALIZED-ONLY Date:   %d (%.2f per field)%n", onlyDate, onlyDate / (double) totalFields);

        checkEquals(totalFields, indexedCheap, "indexed path should normalize each value once");
        checkEquals(totalFields, onlyCheap, "normalized-but-not-indexed path should normalize each value once");
        checkEquals(totalFields, indexedDate, "expensive type, indexed path");
        checkEquals(totalFields, onlyDate, "expensive type, normalized-only path");
    }

    /** Normalizes every event in the pool once and returns the total normalizer invocations. */
    private long countNormalizations(int[] fieldCounts, FieldMode mode, String uniformType, String uniformValue) throws Exception {
        String dataTypeName = "norm" + SCENARIO_SEQ.incrementAndGet();
        Configuration conf = baseConfiguration(dataTypeName, mode, DataVariety.UNIFORM, uniformType);
        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);

        BenchIngestHelper helper = new BenchIngestHelper();
        helper.setup(conf);
        List<RawRecordContainer> pool = BenchmarkEventGenerator.eventPool(fieldCounts, TypeRegistry.getType(dataTypeName), false, uniformValue);

        NORMALIZE_CALLS.set(0);
        for (RawRecordContainer record : pool) {
            helper.getEventFields(record);
        }
        return NORMALIZE_CALLS.get();
    }

    /**
     * Times the normalization half of the hot path, {@code getEventFields} into {@code BaseIngestHelper.normalizeMap}. The scaling sweep deliberately hoists
     * this out of the measured loop so it times key generation alone, which means normalization cost is invisible there. This measures it directly, cheap type
     * against expensive type, indexed branch against normalized-only branch.
     */
    public void benchmarkNormalizationPhase() throws Exception {
        String cheap = CountingType.class.getName();
        String pricey = CountingDateType.class.getName();
        String dateValue = "2024-06-15T10:30:00.000Z";
        int[] variable = BenchmarkEventGenerator.variableFieldCounts(EVENT_COUNT);

        System.out.println();
        System.out.printf("=== normalization phase: getEventFields (%d events, %d..%d fields, %d iterations) ===%n", variable.length,
                        BenchmarkEventGenerator.MIN_FIELDS, BenchmarkEventGenerator.MAX_FIELDS, MEASURED_ITERATIONS);
        System.out.printf("%-26s %12s %12s %10s %12s%n", "scenario", "median(ns)", "p90(ns)", "norm/field", "bytes/event");

        timeNormalization("indexed, cheap type", variable, FieldMode.INDEXED, cheap, UNIFORM_VALUE);
        timeNormalization("normalized-only, cheap", variable, FieldMode.NORMALIZED_ONLY, cheap, UNIFORM_VALUE);
        timeNormalization("indexed, DateType", variable, FieldMode.INDEXED, pricey, dateValue);
        timeNormalization("normalized-only, DateType", variable, FieldMode.NORMALIZED_ONLY, pricey, dateValue);
        timeNormalization("indexed, all 20 types", variable, FieldMode.INDEXED, cheap, null);
        timeNormalization("normalized-only, 20 types", variable, FieldMode.NORMALIZED_ONLY, cheap, null);
        System.out.println();
    }

    /** A {@code uniformValue} of null selects the diverse all-types pool. */
    private void timeNormalization(String label, int[] fieldCounts, FieldMode mode, String uniformType, String uniformValue) throws Exception {
        DataVariety variety = uniformValue == null ? DataVariety.DIVERSE : DataVariety.UNIFORM;
        String dataTypeName = "normt" + SCENARIO_SEQ.incrementAndGet();
        Configuration conf = baseConfiguration(dataTypeName, mode, variety, uniformType);
        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);

        BenchIngestHelper helper = new BenchIngestHelper();
        helper.setup(conf);
        Type dataType = TypeRegistry.getType(dataTypeName);
        List<RawRecordContainer> pool = BenchmarkEventGenerator.eventPool(fieldCounts, dataType, variety == DataVariety.DIVERSE,
                        uniformValue == null ? UNIFORM_VALUE : uniformValue);

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            helper.getEventFields(pool.get(i % pool.size()));
        }

        NORMALIZE_CALLS.set(0);
        long fieldsInPass = 0;
        for (RawRecordContainer record : pool) {
            helper.getEventFields(record);
        }
        for (int c : fieldCounts) {
            fieldsInPass += c;
        }
        long normalizes = NORMALIZE_CALLS.get();

        long[] samples = new long[MEASURED_ITERATIONS];
        long allocStart = getCurrentThreadAllocatedBytes();
        for (int i = 0; i < MEASURED_ITERATIONS; i++) {
            long t0 = System.nanoTime();
            helper.getEventFields(pool.get(i % pool.size()));
            samples[i] = System.nanoTime() - t0;
        }
        long allocEnd = getCurrentThreadAllocatedBytes();

        Arrays.sort(samples);
        // the counter only fires for the counting types; the all-types arm uses the real ones
        String normPerField = normalizes == 0 ? "n/a" : String.format("%.2f", normalizes / (double) fieldsInPass);
        System.out.printf("%-26s %12d %12d %10s %12d%n", label, samples[samples.length / 2], samples[(int) (samples.length * 0.9)], normPerField,
                        (allocEnd - allocStart) / MEASURED_ITERATIONS);
    }

    /** Every concrete normalizer type should be represented across the pool. */
    public void generatorCoversEveryNormalizerType() throws Exception {
        int[] variable = BenchmarkEventGenerator.variableFieldCounts(EVENT_COUNT);
        int maxFields = Arrays.stream(variable).max().orElse(0);
        check(maxFields >= BenchmarkEventGenerator.TYPE_COUNT, "largest event must cover every type, was " + maxFields + " fields");
        check(variable.length >= BenchmarkEventGenerator.DEFAULT_EVENT_COUNT,
                        "pool must hold at least " + BenchmarkEventGenerator.DEFAULT_EVENT_COUNT + " events");
        checkEquals(BenchmarkEventGenerator.MIN_FIELDS, Arrays.stream(variable).min().orElse(0), "smallest event size");
        checkEquals(BenchmarkEventGenerator.MAX_FIELDS, maxFields, "largest event size");

        Result r = measure("type-coverage", variable);
        System.out.printf("pool: %d events, %d..%d fields (mean %.1f), %d types, %d keys/pass%n", r.events, r.minFields, r.maxFields, r.meanFields,
                        BenchmarkEventGenerator.TYPE_COUNT, r.keysPerPass);
    }

    /** Scenarios run when no arguments are given. {@code profile} is deliberately excluded: it duplicates work the others already do. */
    private static final String[] DEFAULT_SCENARIOS = {"scaling", "variable", "normalization", "structural"};

    /**
     * Entry point. With no arguments every scenario in {@link #DEFAULT_SCENARIOS} runs in order; otherwise each argument names one:
     * <ul>
     * <li>{@code scaling} — key generation over a fixed-size field sweep</li>
     * <li>{@code variable} — a variable-size pool against a fixed pool at the same mean</li>
     * <li>{@code normalization} — the {@code getEventFields} phase</li>
     * <li>{@code structural} — the deterministic call counts; throws {@link IllegalStateException} if one does not hold</li>
     * <li>{@code profile} — the mixed-size workload alone, so a JFR recording covers only that</li>
     * </ul>
     * Exits non-zero on an unknown scenario or a failed structural check.
     */
    public static void main(String[] args) throws Exception {
        for (String scenario : (args.length > 0 ? args : DEFAULT_SCENARIOS)) {
            run(scenario);
        }
    }

    private static void run(String scenario) throws Exception {
        if ("structural".equals(scenario)) {
            run("flatten");
            run("normalizer-calls");
            run("generator-coverage");
            return;
        }

        // A fresh instance per scenario, matching what the harness relied on when these were test methods: a printed
        // table then holds only its own rows, and every scenario starts from a reset TypeRegistry.
        IngestHotPathBenchmark benchmark = new IngestHotPathBenchmark();
        benchmark.setUp();

        if ("scaling".equals(scenario)) {
            benchmark.benchmarkCreateColumnsScaling();
        } else if ("variable".equals(scenario)) {
            benchmark.benchmarkVariableEventSizes();
        } else if ("normalization".equals(scenario)) {
            benchmark.benchmarkNormalizationPhase();
        } else if ("flatten".equals(scenario)) {
            benchmark.visibilityIsFlattenedPerFieldNotPerEvent();
        } else if ("normalizer-calls".equals(scenario)) {
            benchmark.normalizerInvocationsPerField();
        } else if ("generator-coverage".equals(scenario)) {
            benchmark.generatorCoversEveryNormalizerType();
        } else if ("profile".equals(scenario)) {
            benchmark.profileMixedWorkload();
        } else {
            System.err.println("unknown scenario '" + scenario + "'; expected one of scaling, variable, normalization, structural, profile");
            System.exit(2);
        }
    }

    /** The mixed-size workload on its own, so a profiler recording covers only it. */
    private void profileMixedWorkload() throws Exception {
        measure("mixed", BenchmarkEventGenerator.variableFieldCounts(EVENT_COUNT));
        printTable();
    }

    /** Structural counts are exact, so a violation is a defect in the harness or in the code under it, not a slow machine. */
    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static void checkEquals(long expected, long actual, String message) {
        if (expected != actual) {
            throw new IllegalStateException(message + ": expected " + expected + ", got " + actual);
        }
    }

    private static long getCurrentThreadAllocatedBytes() {
        return THREAD_BEAN.getThreadAllocatedBytes(Thread.currentThread().getId());
    }

    private void printTable() {
        System.out.println();
        System.out.println("=== ingest hot path (" + EVENT_COUNT + " events; " + MEASURED_ITERATIONS + " iterations) ===");
        System.out.printf("%-20s %7s %12s %12s %12s %12s %10s %10s %9s %12s%n", "scenario", "events", "fields", "median(ns)", "mean(ns)", "p90(ns)", "ns/key",
                        "keys/pass", "flatten", "bytes/event");
        for (Result r : results) {
            String fieldDesc = r.minFields == r.maxFields ? Integer.toString(r.minFields)
                            : String.format("%d-%d(%.1f)", r.minFields, r.maxFields, r.meanFields);
            StringBuilder kinds = new StringBuilder();
            for (int k = 0; k < KEY_KIND_COUNT; k++) {
                kinds.append(k == 0 ? "" : "/").append(KEY_KIND_NAMES[k]).append(':').append(r.keyKinds[k]);
            }
            System.out.printf("%-20s %7d %12s %12d %12d %12d %10.1f %10d %9d %12d  %s%n", r.name, r.events, fieldDesc, r.medianNanos, r.meanNanos, r.p90Nanos,
                            r.nanosPerKey(), r.keysPerPass, r.flattenPerPass, r.bytesPerEvent, kinds);
        }
        System.out.println();
        results.clear();
    }
}
