package datawave.ingest.benchmark;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.accumulo.core.security.ColumnVisibility;

import com.google.common.collect.Multimap;

import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.marking.AccessExpressionMarkings;

/**
 * Generates the synthetic event pools the ingest benchmarks measure.
 * <p>
 * Three axes matter for the measurements they feed:
 * <ul>
 * <li><b>Event size.</b> Events carry between {@link #MIN_FIELDS} and {@link #MAX_FIELDS} fields, drawn from a seeded generator so a run is reproducible. A
 * pool of at least {@link #DEFAULT_EVENT_COUNT} events means no single event shape dominates the measurement, and inline caches and branch predictors see the
 * variation a real mapper sees rather than one shape repeated.</li>
 * <li><b>Normalizer coverage.</b> {@link #TYPE_SAMPLES} pairs every concrete {@code datawave.data.type.Type} with a value it normalizes cleanly, verified
 * rather than assumed. Types are assigned to fields round robin by field index, so every event of at least {@link #TYPE_COUNT} fields covers each normalizer
 * and the pool covers all of them many times over. Normalizer cost varies by more than an order of magnitude across these — {@code DateType} walks a list of
 * {@code SimpleDateFormat} patterns, {@code NoOpType} returns its input — so generating a single type misrepresents where time goes.</li>
 * <li><b>Visibility diversity.</b> Events rotate through {@link #EVENT_VISIBILITIES} and, when a marked stride is requested, individual fields carry their own
 * {@link AccessExpressionMarkings} drawn from {@link #FIELD_VISIBILITIES}. This matters because {@code FlattenedVisibilityCache} is keyed on
 * {@code ColumnVisibility}: a pool with one visibility hits that cache on every lookup after the first and makes the visibility path look free.</li>
 * </ul>
 */
final class BenchmarkEventGenerator {

    private BenchmarkEventGenerator() {}

    /** Smallest event, in fields. */
    static final int MIN_FIELDS = 15;

    /** Largest event, in fields. Configuration always declares this many field names. */
    static final int MAX_FIELDS = 45;

    /** Events per pool. Large enough that no single event shape dominates a measurement. */
    static final int DEFAULT_EVENT_COUNT = 250;

    /** Fixed seed so event sizes, and therefore the numbers, are reproducible run to run. */
    static final long FIELD_COUNT_SEED = 20260807L;

    /**
     * Every concrete Type in {@code datawave.data.type} paired with a value it normalizes without error. Verified by round-tripping each pair through
     * {@code Type.normalize} (and {@code normalizeToMany} for the one-to-many types) before being recorded here.
     */
    static final String[][] TYPE_SAMPLES = {
            // {type class, sample value}
            {"datawave.data.type.DateType", "2024-06-15T10:30:00.000Z"}, //
            {"datawave.data.type.RawDateType", "2024-06-15T10:30:00.000Z"}, //
            {"datawave.data.type.GeoLatType", "38.8977"}, //
            {"datawave.data.type.GeoLonType", "-77.0365"}, //
            {"datawave.data.type.GeoType", "38.8977|-77.0365"}, //
            {"datawave.data.type.GeometryType", "POINT(-77.0365 38.8977)"}, // one-to-many
            {"datawave.data.type.PointType", "POINT(-77.0365 38.8977)"}, //
            {"datawave.data.type.HexStringType", "DEADBEEF"}, //
            {"datawave.data.type.HitTermType", "FIELD:value"}, //
            {"datawave.data.type.IpAddressType", "192.168.1.100"}, //
            {"datawave.data.type.IpV4AddressType", "192.168.1.100"}, //
            {"datawave.data.type.LcNoDiacriticsType", "Café Ünicode Text"}, //
            {"datawave.data.type.LcNoDiacriticsListType", "Alpha,Béta,Gamma"}, // one-to-many, 3 elements
            {"datawave.data.type.LcType", "MixedCaseValue"}, //
            {"datawave.data.type.MacAddressType", "00:1A:2B:3C:4D:5E"}, //
            {"datawave.data.type.NumberType", "42.5"}, //
            {"datawave.data.type.NumberListType", "42"}, // one-to-many
            {"datawave.data.type.NoOpType", "raw value"}, //
            {"datawave.data.type.StringType", "plain string"}, //
            {"datawave.data.type.TrimLeadingZerosType", "000123"},};

    static final int TYPE_COUNT = TYPE_SAMPLES.length;

    /**
     * The field index carrying paragraph-length content. Always below {@link #MIN_FIELDS} so every event in the pool has it, and always a
     * {@link FieldCategory#TOKENIZED_INDEX_ONLY} slot.
     */
    static final int LONG_CONTENT_FIELD_INDEX = 3;

    /**
     * Paragraph-scale text for the long tokenized field. Deliberately prose rather than repeated filler: the analyzer's term-length filters, type
     * classification and synonym handling all behave differently on realistic word-length distributions, and a repeated token would collapse the offset cache
     * into one entry and hide the per-token term-frequency work entirely. The subject matter is unrelated to this codebase on purpose, so that nothing here
     * reads as documentation of the system under test.
     */
    static final String LONG_CONTENT = String.join(" ", "A prime number has no divisors beyond itself and one, and Euclid showed more than two thousand",
                    "years ago that the supply of them never runs out. The sieve attributed to Eratosthenes finds them",
                    "by striking out multiples in turn until only the primes remain. Their spacing grows slowly and",
                    "irregularly, yet the average gap near a large number follows its logarithm with surprising fidelity.",
                    "Geometry began as the measurement of land and grew into the study of shape, distance and symmetry.",
                    "The angles of a triangle sum to a straight line on a flat surface, but not on a sphere, where the",
                    "excess is proportional to the area enclosed, and that single observation opens the door to curvature.",
                    "Only five convex solids can be assembled from identical regular faces meeting alike at every vertex:",
                    "the tetrahedron, the cube, the octahedron, the dodecahedron and the icosahedron. In the plane only",
                    "three regular polygons tile without leaving gaps, the triangle, the square and the hexagon, which is",
                    "why honeycomb and paving so often repeat one of those three arrangements. Color admits a comparable",
                    "structure. Light mixes additively, so red, green and blue together approach white, while pigments mix",
                    "subtractively, absorbing wavelengths until what remains approaches black. Arranging hues around a",
                    "circle places complementary pairs opposite one another, and painters use that opposition to make a",
                    "small patch of one hue appear vivid against a wide field of its complement. Saturation describes how",
                    "far a color sits from grey, and value describes how much light it returns to the eye. The golden",
                    "ratio divides a line so that the whole stands to the larger part as the larger stands to the smaller,",
                    "and the same proportion surfaces in the pentagon, in spiral growth, and in a famous integer sequence.");

    /**
     * Content of a requested token count, using distinct words so the analyzer produces that many distinct indexed terms rather than collapsing them in the
     * offset cache.
     *
     * @param tokens
     *            number of tokens to generate
     * @return the generated content
     */
    static String contentWithTokens(int tokens) {
        StringBuilder sb = new StringBuilder(tokens * 8);
        for (int i = 0; i < tokens; i++) {
            if (i > 0) {
                sb.append(' ');
            }
            sb.append("term").append(i).append("word");
        }
        return sb.toString();
    }

    /** Short phrases for the remaining tokenized fields, cycled by field index. */
    static final String[] SHORT_CONTENT = {"quick brown fox jumps over the lazy dog", "distributed systems require careful capacity planning",
            "the analyzer splits values into a stream of tokens", "shard identifiers derive from the event date and a hash",
            "normalized content fans out across several tables"};

    /** Event level visibilities, rotated across the event pool. Mixed arity and nesting depth. */
    static final String[] EVENT_VISIBILITIES = {"PUBLIC", "PUBLIC&A", "(PUBLIC|PRIVATE)&(A|B)", "(PUBLIC|PRIVATE)&(A|B)&(C|D)",
            "((PUBLIC|PRIVATE)&(A|B))|((INTERNAL|RESTRICTED)&(C|D))", "PRIVATE&INTERNAL&(A|B|C)", "RESTRICTED", "(A|B)&(C|D)&(E|F)&(G|H)"};

    /** Field level visibilities, applied to a strided subset of fields when requested. */
    static final String[] FIELD_VISIBILITIES = {"PUBLIC&FIELDMARK1", "PRIVATE&FIELDMARK2", "(PUBLIC|PRIVATE)&FIELDMARK3", "INTERNAL&(FIELDMARK4|FIELDMARK5)",
            "RESTRICTED&FIELDMARK6"};

    /**
     * Field names declared in the configuration. Always {@link #MAX_FIELDS} of them regardless of the sizes actually generated, so a short event's fields are
     * still configured as indexed and typed.
     *
     * @return the configured field names
     */
    static List<String> configuredFieldNames() {
        return fieldNames(MAX_FIELDS);
    }

    static List<String> fieldNames(int n) {
        List<String> names = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            names.add("FIELD_" + i);
        }
        return names;
    }

    /** The Type class configured for a given field index under round robin assignment. */
    static String typeClassFor(int fieldIndex) {
        return TYPE_SAMPLES[fieldIndex % TYPE_COUNT][0];
    }

    /**
     * The sample value for a given field index. Tokenized fields get prose, since a normalizer sample value such as an IP address or a WKT point would tokenize
     * into a handful of meaningless terms and would not exercise the analyzer the way real content does. Everything else gets the value matching its configured
     * Type.
     *
     * @param fieldIndex
     *            zero based field index
     * @return the value to place in the payload
     */
    static String sampleValueFor(int fieldIndex) {
        if (FieldCategory.forIndex(fieldIndex) == FieldCategory.TOKENIZED_INDEX_ONLY) {
            return fieldIndex == LONG_CONTENT_FIELD_INDEX ? LONG_CONTENT : SHORT_CONTENT[(fieldIndex / 4) % SHORT_CONTENT.length];
        }
        return TYPE_SAMPLES[fieldIndex % TYPE_COUNT][1];
    }

    /**
     * The Type class for a field index. Tokenized fields must take a text-friendly normalizer; the round robin type assignment applies only to the other
     * categories.
     *
     * @param fieldIndex
     *            zero based field index
     * @return the fully qualified Type class name
     */
    static String typeClassForCategory(int fieldIndex) {
        if (FieldCategory.forIndex(fieldIndex) == FieldCategory.TOKENIZED_INDEX_ONLY) {
            return "datawave.data.type.LcNoDiacriticsType";
        }
        return TYPE_SAMPLES[fieldIndex % TYPE_COUNT][0];
    }

    /**
     * Field names belonging to a given category, across the whole configured field space.
     *
     * @param category
     *            the category to collect
     * @return the matching field names
     */
    static List<String> fieldNamesFor(FieldCategory category) {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < MAX_FIELDS; i++) {
            if (FieldCategory.forIndex(i) == category) {
                names.add("FIELD_" + i);
            }
        }
        return names;
    }

    /**
     * Field counts for a pool of varying event sizes, uniform over {@code [MIN_FIELDS, MAX_FIELDS]} and reproducible via {@link #FIELD_COUNT_SEED}.
     *
     * @param eventCount
     *            number of events in the pool
     * @return one field count per event
     */
    static int[] variableFieldCounts(int eventCount) {
        Random random = new Random(FIELD_COUNT_SEED);
        int[] counts = new int[eventCount];
        for (int i = 0; i < eventCount; i++) {
            counts[i] = MIN_FIELDS + random.nextInt(MAX_FIELDS - MIN_FIELDS + 1);
        }
        return counts;
    }

    /**
     * Field counts for a pool where every event is the same size. Used by the scaling sweep, which needs one size at a time to tell linear growth from
     * quadratic.
     *
     * @param eventCount
     *            number of events in the pool
     * @param fields
     *            the fixed field count
     * @return one field count per event, all equal
     */
    static int[] fixedFieldCounts(int eventCount, int fields) {
        int[] counts = new int[eventCount];
        Arrays.fill(counts, fields);
        return counts;
    }

    /**
     * Builds a pool of events. Sizes come from {@code fieldCounts}; visibilities rotate through {@link #EVENT_VISIBILITIES} so the flattened-visibility cache
     * sees more than one key; dates advance by a day per event so shard ids differ.
     *
     * @param fieldCounts
     *            field count per event, one entry per event to generate
     * @param dataType
     *            the registered ingest type
     * @param diverseTypes
     *            true to assign the sample value matching each field's configured Type, false to give every field {@code uniformValue}
     * @param uniformValue
     *            the value used for every field when {@code diverseTypes} is false; must be parseable by whatever Type the uniform arm configures, otherwise
     *            normalization throws and the caller measures the error path instead of the one it intended
     * @return the event pool
     */
    static List<RawRecordContainer> eventPool(int[] fieldCounts, Type dataType, boolean diverseTypes, String uniformValue) {
        return eventPool(fieldCounts, dataType, diverseTypes, uniformValue, null);
    }

    /**
     * As {@link #eventPool(int[], Type, boolean, String)}, but replaces the long content field's value.
     *
     * @param fieldCounts
     *            field count per event
     * @param dataType
     *            the registered ingest type
     * @param diverseTypes
     *            true to use per-field sample values
     * @param uniformValue
     *            value for every field when {@code diverseTypes} is false
     * @param contentOverride
     *            replacement value for {@link #LONG_CONTENT_FIELD_INDEX}, or null to keep the default
     * @return the event pool
     */
    static List<RawRecordContainer> eventPool(int[] fieldCounts, Type dataType, boolean diverseTypes, String uniformValue, String contentOverride) {
        List<RawRecordContainer> pool = new ArrayList<>(fieldCounts.length);
        for (int e = 0; e < fieldCounts.length; e++) {
            StringBuilder raw = new StringBuilder();
            for (int i = 0; i < fieldCounts[e]; i++) {
                if (i > 0) {
                    raw.append(';');
                }
                String value = diverseTypes ? sampleValueFor(i) : uniformValue;
                if (contentOverride != null && i == LONG_CONTENT_FIELD_INDEX) {
                    value = contentOverride;
                }
                raw.append("FIELD_").append(i).append('=').append(value);
            }
            RawRecordContainer record = new RawRecordContainerImpl();
            record.setDataType(dataType);
            record.setRawFileName("bench.dat");
            record.setRawRecordNumber(e + 1L);
            record.setRawData(raw.toString().getBytes(StandardCharsets.UTF_8));
            long date = 1749254400000L + (e * 86400000L);
            record.setDate(date);
            record.setTimestamp(date);
            record.setVisibility(new ColumnVisibility(EVENT_VISIBILITIES[e % EVENT_VISIBILITIES.length]));
            record.generateId(null);
            pool.add(record);
        }
        return pool;
    }

    /**
     * Applies per-field markings to every {@code stride}-th field, cycling through {@link #FIELD_VISIBILITIES}. A stride of zero leaves the map untouched,
     * which is the case where every field inherits the event visibility.
     *
     * @param fields
     *            the normalized field map to annotate in place
     * @param stride
     *            mark every stride-th field; zero to mark none
     */
    static void applyFieldMarkings(Multimap<String,NormalizedContentInterface> fields, int stride) {
        if (stride <= 0) {
            return;
        }
        int i = 0;
        int marked = 0;
        for (NormalizedContentInterface nci : fields.values()) {
            if (i % stride == 0) {
                nci.setMarkings(AccessExpressionMarkings.create(FIELD_VISIBILITIES[marked % FIELD_VISIBILITIES.length]));
                marked++;
            }
            i++;
        }
    }
}
