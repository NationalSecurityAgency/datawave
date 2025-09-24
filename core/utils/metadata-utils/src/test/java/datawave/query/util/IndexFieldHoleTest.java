package datawave.query.util;

import static datawave.data.ColumnFamilyConstants.COLF_F;
import static datawave.query.util.TestUtils.createDateFrequencyMap;
import static datawave.query.util.TestUtils.createRangedDateFrequencyMap;
import static org.apache.accumulo.core.iterators.LongCombiner.VAR_LEN_ENCODER;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.LcNoDiacriticsType;
import datawave.iterators.FrequencyMetadataAggregator;
import datawave.query.composite.CompositeMetadataHelper;
import datawave.query.model.DateFrequencyMap;
import datawave.query.model.IndexFieldHole;
import datawave.util.time.DateHelper;

class IndexFieldHoleTest {

    private static final String TABLE_METADATA = "metadata";
    private static final String[] AUTHS = {"FOO"};
    private static final String NULL_BYTE = "\0";
    private AccumuloClient accumuloClient;
    private MetadataHelper helper;

    private final List<Mutation> mutations = new ArrayList<>();

    @BeforeAll
    static void beforeAll() throws URISyntaxException {
        File dir = new File(Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource(".")).toURI());
        File targetDir = dir.getParentFile();
        System.setProperty("hadoop.home.dir", targetDir.getAbsolutePath());
    }

    /**
     * Set up the accumulo client and initialize the helper.
     */
    @BeforeEach
    void setUp() throws AccumuloSecurityException, AccumuloException, TableExistsException {
        accumuloClient = new InMemoryAccumuloClient("root", new InMemoryInstance(AllFieldMetadataHelper.class.toString()));
        if (!accumuloClient.tableOperations().exists(TABLE_METADATA)) {
            accumuloClient.tableOperations().create(TABLE_METADATA);
        }
        final Set<Authorizations> allMetadataAuths = Collections.emptySet();
        final Set<Authorizations> auths = Collections.singleton(new Authorizations(AUTHS));
        TypeMetadataHelper typeMetadataHelper = new TypeMetadataHelper(Maps.newHashMap(), allMetadataAuths, accumuloClient, TABLE_METADATA, auths, false);
        CompositeMetadataHelper compositeMetadataHelper = new CompositeMetadataHelper(accumuloClient, TABLE_METADATA, auths);
        AllFieldMetadataHelper allHelper = new AllFieldMetadataHelper(typeMetadataHelper, compositeMetadataHelper, accumuloClient, TABLE_METADATA, auths,
                        allMetadataAuths);
        helper = new MetadataHelper(allHelper, allMetadataAuths, accumuloClient, TABLE_METADATA, auths, allMetadataAuths);
    }

    /**
     * Clear the metadata table after each test.
     */
    @AfterEach
    void tearDown() throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
        accumuloClient.tableOperations().deleteRows(TABLE_METADATA, null, null);
    }

    /**
     * Write the given mutations to the metadata table.
     */
    private void writeMutations(Collection<Mutation> mutations) {
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(0);
        try (BatchWriter writer = accumuloClient.createBatchWriter(TABLE_METADATA, config)) {
            writer.addMutations(mutations);
            writer.flush();
        } catch (MutationsRejectedException | TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Write the given mutations to the metadata table.
     */
    private void writeMutations() {
        TestUtils.writeMutations(accumuloClient, TABLE_METADATA, mutations);
    }

    private void givenNonAggregatedFrequencyRows(String row, String colf, String datatype, String startDate, String endDate, long count) {
        givenNonAggregatedFrequencyRows(row, new Text(colf), datatype, startDate, endDate, count);
    }

    private void givenNonAggregatedFrequencyRows(String row, Text colf, String datatype, String startDate, String endDate, long count) {
        Mutation mutation = new Mutation(row);
        Value value = new Value(VAR_LEN_ENCODER.encode(count));
        List<String> dates = TestUtils.getDatesInRange(startDate, endDate);
        dates.forEach((date) -> mutation.put(colf, new Text(datatype + NULL_BYTE + date), value));
        givenMutation(mutation);
    }

    private void givenIndexMarkerMutation(String row, String colf, String datatype, String date, boolean indexed) {
        Mutation mutation = new Mutation(row);
        mutation.put(colf, datatype + NULL_BYTE + date + NULL_BYTE + indexed, new Value());
        mutations.add(mutation);
    }

    private void givenIndexMarkerMutation(String row, String colf, String datatype, String date) {
        Mutation mutation = new Mutation(row);
        mutation.put(colf, datatype, DateHelper.parse(date).getTime(), new Value());
        mutations.add(mutation);
    }

    private void givenIndexMarkerMutation(String row, String colf, String datatype, String date, Class<?> typeClass) {
        Mutation mutation = new Mutation(row);
        mutation.put(colf, datatype + NULL_BYTE + typeClass.getName(), DateHelper.parse(date).getTime(), new Value());
        mutations.add(mutation);
    }

    private void givenAggregatedFrequencyRow(String row, String colf, String datatype, DateFrequencyMap map) {
        givenAggregatedFrequencyRow(row, new Text(colf), datatype, map);
    }

    private void givenAggregatedFrequencyRow(String row, Text colf, String datatype, DateFrequencyMap map) {
        Mutation mutation = new Mutation(row);
        Value value = new Value(WritableUtils.toByteArray(map));
        mutation.put(colf, new Text(datatype + NULL_BYTE + FrequencyMetadataAggregator.AGGREGATED), value);
        givenMutation(mutation);
    }

    private void givenMutation(Mutation mutation) {
        this.mutations.add(mutation);
    }

    /**
     * Tests for {@link AllFieldMetadataHelper#getCountsByFieldInDayWithTypes(Map.Entry)}.
     */
    @Nested
    public class CountsByFieldInDayWithTypesTests {

        /**
         * Test against a table that has only non-aggregated entries as matches.
         */
        @Test
        void testNonAggregatedEntriesOnly() throws TableNotFoundException, IOException {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200101", "20200120", 1L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200120", 2L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "maze", "20200101", "20200120", 3L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "data", "20200101", "20200102", 3L); // Does not contain target date.
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "csv", "20200101", "20200120", 4L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "wiki", "20200101", "20200120", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "maze", "20200101", "20200120", 6L);
            writeMutations();

            Map<String,Long> expected = new HashMap<>();
            expected.put("csv", 1L);
            expected.put("wiki", 2L);
            expected.put("maze", 3L);

            HashMap<String,Long> actual = helper.getCountsByFieldInDayWithTypes(new AbstractMap.SimpleEntry<>("NAME", "20200110"));

            Assertions.assertEquals(expected, actual);
        }

        /**
         * Test against a table that has only aggregated entries as matches.
         */
        @Test
        void testAggregatedEntriesOnly() throws TableNotFoundException, IOException {
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createDateFrequencyMap("20200101", 1L, "20200102", 5L, "20200103", 3L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createDateFrequencyMap("20200101", 1L, "20200102", 15L, "20200103", 3L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "maze", createDateFrequencyMap("20200101", 1L, "20200102", 55L, "20200103", 3L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "data", createDateFrequencyMap("20200101", 1L, "20200103", 3L)); // Does not contain target date.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "csv", createDateFrequencyMap("20200101", 2L, "20200102", 3L, "20200103", 4L));
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createDateFrequencyMap("20200101", 2L, "20200102", 3L, "20200103", 4L));
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "maze", createDateFrequencyMap("20200101", 2L, "20200102", 3L, "20200103", 4L));
            writeMutations();

            Map<String,Long> expected = new HashMap<>();
            expected.put("csv", 5L);
            expected.put("wiki", 15L);
            expected.put("maze", 55L);

            HashMap<String,Long> actual = helper.getCountsByFieldInDayWithTypes(new AbstractMap.SimpleEntry<>("NAME", "20200102"));

            Assertions.assertEquals(expected, actual);
        }

        /**
         * Test against a table that has both aggregated and non-aggregated entries as matches.
         */
        @Test
        void testMixedEntryFormats() throws TableNotFoundException, IOException {
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createDateFrequencyMap("20200101", 1L, "20200102", 5L, "20200103", 3L));
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200101", "20200120", 1L); // Should get summed into previous.
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createDateFrequencyMap("20200101", 1L, "20200102", 15L, "20200103", 3L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "maze", createDateFrequencyMap("20200101", 1L, "20200102", 55L, "20200103", 3L));
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "maze", "20200115", "20200120", 3L); // Does not have entry for 20200102, should not be incremented.
            givenAggregatedFrequencyRow("NAME", COLF_F, "data", createDateFrequencyMap("20200101", 1L, "20200103", 3L)); // Does not contain target date.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "data", "20200103", "20200105", 3L); // Does not contain target date.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "csv", createDateFrequencyMap("20200101", 2L, "20200102", 3L, "20200103", 4L));
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createDateFrequencyMap("20200101", 2L, "20200102", 3L, "20200103", 4L));
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "maze", createDateFrequencyMap("20200101", 2L, "20200102", 3L, "20200103", 4L));
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "csv", "20200101", "20200120", 4L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "wiki", "20200101", "20200120", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "maze", "20200101", "20200120", 6L);
            writeMutations();

            Map<String,Long> expected = new HashMap<>();
            expected.put("csv", 6L);
            expected.put("wiki", 15L);
            expected.put("maze", 55L);

            HashMap<String,Long> actual = helper.getCountsByFieldInDayWithTypes(new AbstractMap.SimpleEntry<>("NAME", "20200102"));

            Assertions.assertEquals(expected, actual);
        }
    }

    /**
     * Base class for field index hole tests.
     */
    public abstract class AbstractIndexFieldHoleTests {

        protected Set<String> fields = new HashSet<>();
        protected Set<String> datatypes = new HashSet<>();
        protected double minimumThreshold = 1.0d;

        protected final Supplier<Map<String,Map<String,IndexFieldHole>>> INDEX_FUNCTION = () -> {
            try {
                return helper.getFieldIndexHoles(fields, datatypes, minimumThreshold);
            } catch (TableNotFoundException | IOException e) {
                throw new RuntimeException(e);
            }
        };

        protected final Supplier<Map<String,Map<String,IndexFieldHole>>> REVERSED_INDEX_FUNCTION = () -> {
            try {
                return helper.getReversedFieldIndexHoles(fields, datatypes, minimumThreshold);
            } catch (TableNotFoundException | IOException e) {
                throw new RuntimeException(e);
            }
        };

        protected Supplier<Map<String,Map<String,IndexFieldHole>>> getIndexHoleFunction(String cf) {
            return cf.equals("i") ? INDEX_FUNCTION : REVERSED_INDEX_FUNCTION;
        }

        @AfterEach
        void tearDown() {
            fields.clear();
            datatypes.clear();
            givenMinimumThreshold(1.0d);
        }

        protected void givenFields(String... fields) {
            this.fields = Sets.newHashSet(fields);
        }

        protected void givenDatatypes(String... datatypes) {
            this.datatypes = Sets.newHashSet(datatypes);
        }

        protected void givenMinimumThreshold(double minimumThreshold) {
            this.minimumThreshold = minimumThreshold;
        }

        protected Map<String,Map<String,IndexFieldHole>> createIndexFieldHoleMap(IndexFieldHole... holes) {
            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = new HashMap<>();
            for (IndexFieldHole hole : holes) {
                Map<String,IndexFieldHole> datatypeMap = IndexFieldHoles.computeIfAbsent(hole.getFieldName(), k -> new HashMap<>());
                datatypeMap.put(hole.getDatatype(), hole);
            }
            return IndexFieldHoles;
        }

        @SafeVarargs
        protected final IndexFieldHole createIndexFieldHole(String field, String datatype, Pair<Date,Date>... dateRanges) {
            return new IndexFieldHole(field, datatype, Sets.newHashSet(dateRanges));
        }

        protected Pair<Date,Date> dateRange(String start, String end) {
            return Pair.of(DateHelper.parse(start), DateHelper.parse(end));
        }
    }

    /**
     * Tests for {@link MetadataHelper#getFieldIndexHoles(Set, Set, double)} and {@link MetadataHelper#getReversedFieldIndexHoles(Set, Set, double)}.
     */
    @Nested
    public class IndexFieldHoleTestsForNonAggregatedEntries extends AbstractIndexFieldHoleTests {

        /**
         * Test against data that has no field index holes.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testNoIndexFieldHoles(String cf) {
            // Create a series of frequency rows over date ranges, each with a matching index row for each date.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200101", "20200120", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200101", "20200120", 1L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200120", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200120", 1L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "maze", "20200101", "20200120", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "maze", "20200101", "20200120", 1L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "csv", "20200101", "20200120", 1L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "csv", "20200101", "20200120", 1L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "wiki", "20200101", "20200120", 1L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200101", "20200120", 1L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "maze", "20200101", "20200120", 1L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "maze", "20200101", "20200120", 1L);
            writeMutations();

            // Verify that no index holes were found.
            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            Assertions.assertTrue(IndexFieldHoles.isEmpty());
        }

        /**
         * Test against data that has field index holes for an entire fieldName-datatype combination based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForEntireFrequencyDateRange_dateGaps(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 1L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200101", "20200105", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200101", "20200105", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200105")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has field index holes for an entire fieldName-datatype combination based on the threshold requirement.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForEntireFrequencyDateRange_threshold(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200105", 1L); // Make the index counts a value that will not meet the threshold.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200101", "20200105", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200101", "20200105", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200105")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole at the start of a frequency date range for a given fieldName-dataType combination based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForStartOfFrequencyDateRange_dateGaps(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200104", "20200105", 1L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200101", "20200105", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200101", "20200105", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200103")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole at the start of a frequency date range for a given fieldName-dataType combination based on the
         * threshold requirement.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForStartOfFrequencyDateRange_threshold(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200103", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200104", "20200105", 5L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200101", "20200105", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200101", "20200105", 5L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200103")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole at the end of a frequency date range for a given fieldName-dataType combination based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForEndOfFrequencyDateRange_dateGaps(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200102", 1L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200101", "20200105", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200101", "20200105", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200105")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole at the end of a frequency date range for a given fieldName-dataType combination based on the
         * threshold requirement.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForEndOfFrequencyDateRange_thresholds(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200102", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200103", "20200105", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200101", "20200105", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200101", "20200105", 5L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200105")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on date gaps.
         * This uses a negative index marker.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleWithNotIndexedMarker(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200110", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200103", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200107", "20200110", 1L);
            givenIndexMarkerMutation("NAME", cf, "wiki", "20200109", false);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200101", "20200105", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200101", "20200105", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200109")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on the
         * threshold.
         * This uses a positive index marker.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleWithIndexedMarker(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200110", 1L);
            givenIndexMarkerMutation("NAME", cf, "wiki", "20200103", true);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200107", "20200110", 1L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200101", "20200105", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200101", "20200105", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on the
         * threshold.
         * This uses a positive index marker derived from an older date-less format
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleWithIndexedMarkerSansDate(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200110", 1L);
            givenIndexMarkerMutation("NAME", cf, "wiki", "20200103");
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200107", "20200110", 1L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200101", "20200105", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200101", "20200105", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on the
         * threshold.
         * This uses a positive index marker derived from an older date-less format with type class
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleWithIndexedMarkerOldTypeFormat(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200110", 1L);
            givenIndexMarkerMutation("NAME", cf, "wiki", "20200103", LcNoDiacriticsType.class);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200107", "20200110", 1L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200101", "20200105", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200101", "20200105", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleWithIndexedMarkerAndMissingFrequency(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200102", 1L);
            givenIndexMarkerMutation("NAME", cf, "wiki", "20200103", true);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200104", "20200110", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200107", "20200110", 1L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200101", "20200105", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200101", "20200105", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where we have a number of index holes that span just a day based on both dates and the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMixedDateGapsAndNonIndexedFields(String cf) {
            // Not indexed nor covers full range for NAME
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200102", 5L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200110", "20200115", 5L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "wiki", "20200120", "20200125", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200120", "20200121", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200122", "20200122", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200123", "20200125", 5L);
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenNonAggregatedFrequencyRows("URI", COLF_F, "maze", "20200216", "20200328", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200216", "20200220", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200221", "20200221", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200222", "20200302", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200304", "20200315", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200316", "20200316", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200317", "20200328", 5L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                    createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                    createIndexFieldHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on both date
         * gaps and the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForMiddleOfFrequencyDateRange_mixed(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200110", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200103", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200105", "20200106", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200107", "20200110", 5L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200101", "20200105", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200101", "20200105", 5L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has multiple field index holes for a given fieldName-datatype combination based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMultipleIndexFieldHolesInFrequencyDateRange_dateGap(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200120", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200104", "20200106", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200110", "20200113", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200117", "20200118", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200103"),
                                            dateRange("20200107", "20200109"),
                                            dateRange("20200114", "20200116"),
                                            dateRange("20200119", "20200120")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has multiple field index holes for a given fieldName-datatype combination based on the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMultipleIndexFieldHolesInFrequencyDateRange_threshold(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200120", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200103", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200104", "20200106", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200107", "20200109", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200110", "20200113", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200114", "20200116", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200117", "20200118", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200119", "20200120", 1L); // Will not meet threshold.
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200103"),
                                            dateRange("20200107", "20200109"),
                                            dateRange("20200114", "20200116"),
                                            dateRange("20200119", "20200120")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where the expected index hole occurs for the end of a frequency range right before a new fieldName-datatype combination based on
         * date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleAtEndOfFrequencyDateRangeForNonLastCombo_dateGap(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200102", 1L);
            givenNonAggregatedFrequencyRows("ZETA", COLF_F, "csv", "20200101", "20200105", 1L);
            givenNonAggregatedFrequencyRows("ZETA", cf, "csv", "20200101", "20200105", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200105")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where the expected index hole occurs for the end of a frequency range right before a new fieldName-datatype combination based on
         * the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleAtEndOfFrequencyDateRangeForNonLastCombo_threshold(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200102", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200103", "20200105", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("ZETA", COLF_F, "csv", "20200101", "20200105", 5L);
            givenNonAggregatedFrequencyRows("ZETA", cf, "csv", "20200101", "20200105", 5L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200105")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where the expected index hole spans across multiple frequency ranges based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleSpanningMultipleFrequencyDateRanges_dateGaps(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 1L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200110", "20200115", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200103", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200113", "20200115", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200112")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where the expected index hole spans across multiple frequency ranges based on the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleSpanningMultipleFrequencyDateRanges_threshold(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200103", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200104", "20200105", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200110", "20200115", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200110", "20200112", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200113", "20200115", 5L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200112")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where everything is an index hole based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testAllDatesAreIndexHoles_dateGaps(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 1L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200110", "20200115", 1L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "wiki", "20200120", "20200125", 1L);
            givenNonAggregatedFrequencyRows("URI", COLF_F, "maze", "20200216", "20200328", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200115")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200120", "20200125")),
                            createIndexFieldHole("URI", "maze", dateRange("20200216", "20200328")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where everything is an index hole based on the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testAllDatesAreIndexHoles_threshold(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200105", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200110", "20200115", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200110", "20200115", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "wiki", "20200120", "20200125", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200120", "20200125", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", COLF_F, "maze", "20200216", "20200328", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200216", "20200328", 1L); // Will not meet threshold.
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200115")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200120", "20200125")),
                            createIndexFieldHole("URI", "maze", dateRange("20200216", "20200328")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where we have a number of index holes that span just a day based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testSingularDayIndexHoles_dateGaps(String cf) {
            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200102", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200104", "20200104", 1L);
            // Index holes for NAME-csv on 20200110 and 20200113.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200110", "20200115", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200111", "20200112", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200114", "20200115", 1L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "wiki", "20200120", "20200125", 1L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200120", "20200121", 1L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200123", "20200125", 1L);
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenNonAggregatedFrequencyRows("URI", COLF_F, "maze", "20200216", "20200328", 1L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200216", "20200220", 1L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200222", "20200302", 1L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200304", "20200315", 1L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200317", "20200328", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createIndexFieldHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where we have a number of index holes that span just a day based on the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testSingularDayIndexHoles_threshold(String cf) {
            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200102", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200103", "20200103", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200104", "20200104", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200105", "20200105", 1L); // Will not meet threshold.
            // Index holes for NAME-csv on 20200110 and 20200113.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200110", "20200115", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200110", "20200110", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200111", "20200112", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200113", "20200113", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200114", "20200115", 5L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "wiki", "20200120", "20200125", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200120", "20200121", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200122", "20200122", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200123", "20200125", 5L);
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenNonAggregatedFrequencyRows("URI", COLF_F, "maze", "20200216", "20200328", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200216", "20200220", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200221", "20200221", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200222", "20200302", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200303", "20200303", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200304", "20200315", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200316", "20200316", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200317", "20200328", 5L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createIndexFieldHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where we have a number of index holes that span just a day based on both dates and the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMixedDateGapsAndThresholdIndexHoles(String cf) {
            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200102", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200104", "20200104", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200105", "20200105", 1L); // Will not meet threshold.
            // Index holes for NAME-csv on 20200110 and 20200113.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200110", "20200115", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200110", "20200110", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200111", "20200112", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200114", "20200115", 5L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "wiki", "20200120", "20200125", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200120", "20200121", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200122", "20200122", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200123", "20200125", 5L);
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenNonAggregatedFrequencyRows("URI", COLF_F, "maze", "20200216", "20200328", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200216", "20200220", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200221", "20200221", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200222", "20200302", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200304", "20200315", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200316", "20200316", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200317", "20200328", 5L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createIndexFieldHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test specifying a minimum percentage threshold other than the default of 1.0.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMinimumThresholdPercentageBelow100(String cf) {
            givenMinimumThreshold(0.75); // Index count must meet 75% of frequency count to not be considered field index hole.

            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 100L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200102", 75L); // Meets 75% threshold.
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200104", "20200104", 100L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200105", "20200105", 74L); // Will not meet threshold.
            // Index holes for NAME-csv on 20200110 and 20200113.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200110", "20200115", 100L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200110", "20200110", 74L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200111", "20200112", 75L); // Meets 75% threshold.
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200114", "20200115", 100L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "wiki", "20200120", "20200125", 100L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200120", "20200121", 98L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200122", "20200122", 74L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200123", "20200125", 75L); // Meets 75% threshold.
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenNonAggregatedFrequencyRows("URI", COLF_F, "maze", "20200216", "20200328", 100L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200216", "20200220", 100L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200221", "20200221", 74L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200222", "20200302", 90L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200304", "20200315", 75L); // Meets 75% threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200316", "20200316", 74L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200317", "20200328", 99L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createIndexFieldHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test specifying one field to filter on.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testOneFieldSpecified(String cf) {
            // Retrieve field index holes for field NAME.
            givenFields("NAME");

            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200102", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200104", "20200104", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200105", "20200105", 1L); // Will not meet threshold.
            // Index holes for NAME-csv on 20200110 and 20200113.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200110", "20200115", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200110", "20200110", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200111", "20200112", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200114", "20200115", 5L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "wiki", "20200120", "20200125", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200120", "20200121", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200122", "20200122", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200123", "20200125", 5L);
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenNonAggregatedFrequencyRows("URI", COLF_F, "maze", "20200216", "20200328", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200216", "20200220", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200221", "20200221", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200222", "20200302", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200304", "20200315", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200316", "20200316", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200317", "20200328", 5L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test specifying multiple fields to filter on.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMultipleFieldsSpecified(String cf) {
            // Retrieve field index holes for fields URI and EVENT_DATE.
            givenFields("URI", "EVENT_DATE");

            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200102", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200103", "20200103", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200104", "20200104", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200105", "20200105", 1L); // Will not meet threshold.
            // Index holes for ALPHA-csv on 20200110 and 20200113.
            givenNonAggregatedFrequencyRows("ALPHA", COLF_F, "csv", "20200110", "20200115", 5L);
            givenNonAggregatedFrequencyRows("ALPHA", cf, "csv", "20200110", "20200110", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("ALPHA", cf, "csv", "20200111", "20200112", 5L);
            givenNonAggregatedFrequencyRows("ALPHA", cf, "csv", "20200113", "20200113", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("ALPHA", cf, "csv", "20200114", "20200115", 5L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "wiki", "20200120", "20200125", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200120", "20200121", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200122", "20200122", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200123", "20200125", 5L);
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenNonAggregatedFrequencyRows("URI", COLF_F, "maze", "20200216", "20200328", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200216", "20200220", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200221", "20200221", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200222", "20200302", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200303", "20200303", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200304", "20200315", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200316", "20200316", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200317", "20200328", 5L);
            // Index hole for ZETA-wiki on 20200122.
            givenNonAggregatedFrequencyRows("ZETA", COLF_F, "wiki", "20200120", "20200125", 5L);
            givenNonAggregatedFrequencyRows("ZETA", cf, "wiki", "20200120", "20200121", 5L);
            givenNonAggregatedFrequencyRows("ZETA", cf, "wiki", "20200122", "20200122", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("ZETA", cf, "wiki", "20200123", "20200125", 5L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createIndexFieldHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test specifying datatypes.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testDatatypesSpecified(String cf) {
            // Retrieve field index holes for datatypes wiki and csv.
            givenDatatypes("wiki", "csv");

            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200102", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200103", "20200103", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200104", "20200104", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200105", "20200105", 1L); // Will not meet threshold.
            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "maze", "20200101", "20200105", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "maze", "20200101", "20200102", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "maze", "20200103", "20200103", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", cf, "maze", "20200104", "20200104", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "maze", "20200105", "20200105", 1L); // Will not meet threshold.
            // Index holes for ALPHA-csv on 20200110 and 20200113.
            givenNonAggregatedFrequencyRows("ALPHA", COLF_F, "csv", "20200110", "20200115", 5L);
            givenNonAggregatedFrequencyRows("ALPHA", cf, "csv", "20200110", "20200110", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("ALPHA", cf, "csv", "20200111", "20200112", 5L);
            givenNonAggregatedFrequencyRows("ALPHA", cf, "csv", "20200113", "20200113", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("ALPHA", cf, "csv", "20200114", "20200115", 5L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "wiki", "20200120", "20200125", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200120", "20200121", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200122", "20200122", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200123", "20200125", 5L);
            // Index hole for EVENT_DATE-maze on 20200122.
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "maze", "20200120", "20200125", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "maze", "20200120", "20200121", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "maze", "20200122", "20200122", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "maze", "20200123", "20200125", 5L);
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenNonAggregatedFrequencyRows("URI", COLF_F, "maze", "20200216", "20200328", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200216", "20200220", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200221", "20200221", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200222", "20200302", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200303", "20200303", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200304", "20200315", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200316", "20200316", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200317", "20200328", 5L);
            // Index hole for ZETA-csv on 20200122.
            givenNonAggregatedFrequencyRows("ZETA", COLF_F, "csv", "20200120", "20200125", 5L);
            givenNonAggregatedFrequencyRows("ZETA", cf, "csv", "20200120", "20200121", 5L);
            givenNonAggregatedFrequencyRows("ZETA", cf, "csv", "20200122", "20200122", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("ZETA", cf, "csv", "20200123", "20200125", 5L);
            // Index hole for ZETA-imdb on 20200122.
            givenNonAggregatedFrequencyRows("ZETA", COLF_F, "imdb", "20200120", "20200125", 5L);
            givenNonAggregatedFrequencyRows("ZETA", cf, "imdb", "20200120", "20200121", 5L);
            givenNonAggregatedFrequencyRows("ZETA", cf, "imdb", "20200122", "20200122", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("ZETA", cf, "imdb", "20200123", "20200125", 5L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("ALPHA", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createIndexFieldHole("ZETA", "csv", dateRange("20200122", "20200122")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test specifying fields and datatypes.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldsAndDatatypesSpecified(String cf) {
            // Retrieve field index holes for fields NAME and ZETA.
            givenFields("NAME", "ZETA");
            // Retrieve field index holes for datatypes wiki and csv.
            givenDatatypes("wiki", "csv");

            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200102", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200103", "20200103", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200104", "20200104", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200105", "20200105", 1L); // Will not meet threshold.
            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "maze", "20200101", "20200105", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "maze", "20200101", "20200102", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "maze", "20200103", "20200103", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("NAME", cf, "maze", "20200104", "20200104", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "maze", "20200105", "20200105", 1L); // Will not meet threshold.
            // Index holes for ALPHA-csv on 20200110 and 20200113.
            givenNonAggregatedFrequencyRows("ALPHA", COLF_F, "csv", "20200110", "20200115", 5L);
            givenNonAggregatedFrequencyRows("ALPHA", cf, "csv", "20200110", "20200110", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("ALPHA", cf, "csv", "20200111", "20200112", 5L);
            givenNonAggregatedFrequencyRows("ALPHA", cf, "csv", "20200113", "20200113", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("ALPHA", cf, "csv", "20200114", "20200115", 5L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "wiki", "20200120", "20200125", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200120", "20200121", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200122", "20200122", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200123", "20200125", 5L);
            // Index hole for EVENT_DATE-maze on 20200122.
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "maze", "20200120", "20200125", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "maze", "20200120", "20200121", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "maze", "20200122", "20200122", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "maze", "20200123", "20200125", 5L);
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenNonAggregatedFrequencyRows("URI", COLF_F, "maze", "20200216", "20200328", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200216", "20200220", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200221", "20200221", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200222", "20200302", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200303", "20200303", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200304", "20200315", 5L);
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200316", "20200316", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200317", "20200328", 5L);
            // Index hole for ZETA-csv on 20200122.
            givenNonAggregatedFrequencyRows("ZETA", COLF_F, "csv", "20200120", "20200125", 5L);
            givenNonAggregatedFrequencyRows("ZETA", cf, "csv", "20200120", "20200121", 5L);
            givenNonAggregatedFrequencyRows("ZETA", cf, "csv", "20200122", "20200122", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("ZETA", cf, "csv", "20200123", "20200125", 5L);
            // Index hole for ZETA-imdb on 20200122.
            givenNonAggregatedFrequencyRows("ZETA", COLF_F, "imdb", "20200120", "20200125", 5L);
            givenNonAggregatedFrequencyRows("ZETA", cf, "imdb", "20200120", "20200121", 5L);
            givenNonAggregatedFrequencyRows("ZETA", cf, "imdb", "20200122", "20200122", 1L); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("ZETA", cf, "imdb", "20200123", "20200125", 5L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("ZETA", "csv", dateRange("20200122", "20200122")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }
    }

    /**
     * Tests for {@link AllFieldMetadataHelper#getFieldIndexHoles(Set, Set, double)} and
     * {@link AllFieldMetadataHelper#getReversedFieldIndexHoles(Set, Set, double)} where the metadata table contains aggregated entries only.
     */
    @Nested
    public class IndexFieldHoleTestsForAggregatedEntries extends AbstractIndexFieldHoleTests {

        /**
         * Test against data that has no field index holes.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testNoIndexFieldHoles(String cf) {
            // Create a series of frequency rows over date ranges, each with a matching index row for each date.
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "maze", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "maze", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "csv", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "maze", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "maze", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            writeMutations();

            // Verify that no index holes were found.
            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            Assertions.assertTrue(IndexFieldHoles.isEmpty());
        }

        /**
         * Test against data that has field index holes for an entire fieldName-datatype combination based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForEntireFrequencyDateRange_dateGaps(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200105")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has field index holes for an entire fieldName-datatype combination based on the threshold requirement.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForEntireFrequencyDateRange_threshold(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 1L)); // Make the index counts a value that will not meet the threshold.
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200105")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole at the start of a frequency date range for a given fieldName-dataType combination based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForStartOfFrequencyDateRange_dateGaps(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200104", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200103")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole at the start of a frequency date range for a given fieldName-dataType combination based on the
         * threshold requirement.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForStartOfFrequencyDateRange_threshold(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200103", 1L, "20200104", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200103")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole at the end of a frequency date range for a given fieldName-dataType combination based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForEndOfFrequencyDateRange_dateGaps(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200102", 1L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200105")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole at the end of a frequency date range for a given fieldName-dataType combination based on the
         * threshold requirement.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForEndOfFrequencyDateRange_thresholds(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200102", 5L, "20200103", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200105")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on date gaps.
         * This uses a negative index marker.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleWithNotIndexedMarker(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200110", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200103", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200107", "20200110", 1L));
            givenIndexMarkerMutation("NAME", cf, "wiki", "20200109", false);
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200109")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on the
         * threshold.
         * This uses a positive index marker.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleWithIndexedMarker(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200110", 1L));
            givenIndexMarkerMutation("NAME", cf, "wiki", "20200103", true);
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200107", "20200110", 1L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on the
         * threshold.
         * This uses a positive index marker derived from an older date-less format
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleWithIndexedMarkerSansDate(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200110", 1L));
            givenIndexMarkerMutation("NAME", cf, "wiki", "20200103");
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200107", "20200110", 1L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on the
         * threshold.
         * This uses a positive index marker derived from an older date-less format with type class
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleWithIndexedMarkerOldTypeFormat(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200110", 1L));
            givenIndexMarkerMutation("NAME", cf, "wiki", "20200103", LcNoDiacriticsType.class);
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200107", "20200110", 1L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleWithIndexedMarkerAndMissingFrequency(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200102", 1L));
            givenIndexMarkerMutation("NAME", cf, "wiki", "20200103", true);
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200104", "20200110", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200107", "20200110", 1L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForMiddleOfFrequencyDateRange_dateGaps(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200110", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200103", 1L, "20200107", "20200110", 1L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on the
         * threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForMiddleOfFrequencyDateRange_threshold(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200110", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200103", 5L, "20200104", "20200106", 1L, "20200107", "20200110", 5L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on both date
         * gaps and the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForMiddleOfFrequencyDateRange_mixed(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200110", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200103", 5L, "20200105", "20200106", 1L, "20200107", "20200110", 5L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has multiple field index holes for a given fieldName-datatype combination based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMultipleIndexFieldHolesInFrequencyDateRange_dateGap(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200104", "20200106", 1L, "20200110", "20200113", 1L, "20200117", "20200118", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200103"),
                                            dateRange("20200107", "20200109"),
                                            dateRange("20200114", "20200116"),
                                            dateRange("20200119", "20200120")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has multiple field index holes for a given fieldName-datatype combination based on the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMultipleIndexFieldHolesInFrequencyDateRange_threshold(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200120", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200103", 1L, "20200104", "20200106", 5L, "20200107",
                            "20200109", 1L, "20200110", "20200113", 5L, "20200114", "20200116", 1L, "20200117", "20200118", 5L, "20200119", "20200120", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200103"),
                                            dateRange("20200107", "20200109"),
                                            dateRange("20200114", "20200116"),
                                            dateRange("20200119", "20200120")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where the expected index hole occurs for the end of a frequency range right before a new fieldName-datatype combination based on
         * date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleAtEndOfFrequencyDateRangeForNonLastCombo_dateGap(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200102", 1L));
            givenAggregatedFrequencyRow("ZETA", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("ZETA", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200105")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where the expected index hole occurs for the end of a frequency range right before a new fieldName-datatype combination based on
         * the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleAtEndOfFrequencyDateRangeForNonLastCombo_threshold(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200102", 5L, "20200103", "20200105", 1L));
            givenAggregatedFrequencyRow("ZETA", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("ZETA", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200105")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where the expected index hole spans across multiple frequency ranges based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleSpanningMultipleFrequencyDateRanges_dateGaps(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 1L, "20200110", "20200115", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200103", 1L, "20200113", "20200115", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200112")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where the expected index hole spans across multiple frequency ranges based on the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleSpanningMultipleFrequencyDateRanges_threshold(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L, "20200110", "20200115", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200103", 5L, "20200104", "20200105", 1L, "20200110",
                            "20200112", 1L, "20200113", "20200115", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200112")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where everything is an index hole based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testAllDatesAreIndexHoles_dateGaps(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200110", "20200115", 1L));
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 1L));
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200115")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200120", "20200125")),
                            createIndexFieldHole("URI", "maze", dateRange("20200216", "20200328")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where everything is an index hole based on the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testAllDatesAreIndexHoles_threshold(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 1L)); // Will not meet threshold.
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200110", "20200115", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200110", "20200115", 1L)); // Will not meet threshold.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 1L)); // Will not meet threshold.
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 5L));
            givenAggregatedFrequencyRow("URI", cf, "maze", createRangedDateFrequencyMap("20200216", "20200328", 1L)); // Will not meet threshold.
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200115")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200120", "20200125")),
                            createIndexFieldHole("URI", "maze", dateRange("20200216", "20200328")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where we have a number of index holes that span just a day based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testSingularDayIndexHoles_dateGaps(String cf) {
            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200102", 1L, "20200104", "20200104", 1L));
            // Index holes for NAME-csv on 20200110 and 20200113.
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200110", "20200115", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200111", "20200112", 1L, "20200114", "20200115", 1L));
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 1L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki", createRangedDateFrequencyMap("20200120", "20200121", 1L, "20200123", "20200125", 1L));
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 1L));
            givenAggregatedFrequencyRow("URI", cf, "maze", createRangedDateFrequencyMap("20200216", "20200220", 1L, "20200222", "20200302", 1L, "20200304",
                            "20200315", 1L, "20200317", "20200328", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createIndexFieldHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where we have a number of index holes that span just a day based on the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testSingularDayIndexHoles_threshold(String cf) {
            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200102", 5L, "20200103", "20200103", 1L, "20200104",
                            "20200104", 5L, "20200105", "20200105", 1L));
            // Index holes for NAME-csv on 20200110 and 20200113.
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200110", "20200115", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200110", "20200110", 1L, "20200111", "20200112", 5L, "20200113",
                            "20200113", 1L, "20200114", "20200115", 5L));
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 5L));
            givenAggregatedFrequencyRow("URI", cf, "maze", createRangedDateFrequencyMap("20200216", "20200220", 5L, "20200221", "20200221", 1L, "20200222",
                            "20200302", 5L, "20200303", "20200303", 1L, "20200304", "20200315", 5L, "20200316", "20200316", 1L, "20200317", "20200328", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createIndexFieldHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where we have a number of index holes that span just a day based on both dates and the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMixedDateGapsAndThresholdIndexHoles(String cf) {
            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki",
                            createRangedDateFrequencyMap("20200101", "20200102", 5L, "20200104", "20200104", 5L, "20200105", "20200105", 1L));
            // Index holes for NAME-csv on 20200110 and 20200113.
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200110", "20200115", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "csv",
                            createRangedDateFrequencyMap("20200110", "20200110", 1L, "20200111", "20200112", 5L, "20200114", "20200115", 5L));
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 5L));
            givenAggregatedFrequencyRow("URI", cf, "maze", createRangedDateFrequencyMap("20200216", "20200220", 5L, "20200221", "20200221", 1L, "20200222",
                            "20200302", 5L, "20200304", "20200315", 5L, "20200316", "20200316", 1L, "20200317", "20200328", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createIndexFieldHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test specifying a minimum percentage threshold other than the default of 1.0.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMinimumThresholdPercentageBelow100(String cf) {
            givenMinimumThreshold(0.75); // Index count must meet 75% of frequency count to not be considered field index hole.

            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 100L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki",
                            createRangedDateFrequencyMap("20200101", "20200102", 75L, "20200104", "20200104", 100L, "20200105", "20200105", 74L));
            // Index holes for NAME-csv on 20200110 and 20200113.
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200110", "20200115", 100L));
            givenAggregatedFrequencyRow("NAME", cf, "csv",
                            createRangedDateFrequencyMap("20200110", "20200110", 74L, "20200111", "20200112", 75L, "20200114", "20200115", 100L));
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 100L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki",
                            createRangedDateFrequencyMap("20200120", "20200121", 98L, "20200122", "20200122", 74L, "20200123", "20200125", 75L));
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 100L));
            givenAggregatedFrequencyRow("URI", cf, "maze", createRangedDateFrequencyMap("20200216", "20200220", 100L, "20200221", "20200221", 74L, "20200222",
                            "20200302", 90L, "20200304", "20200315", 75L, "20200316", "20200316", 74L, "20200317", "20200328", 99L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createIndexFieldHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test specifying one field to filter on.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testOneFieldSpecified(String cf) {
            // Retrieve field index holes for field NAME.
            givenFields("NAME");

            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki",
                            createRangedDateFrequencyMap("20200101", "20200102", 5L, "20200104", "20200104", 5L, "20200105", "20200105", 1L));
            // Index holes for NAME-csv on 20200110 and 20200113.
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200110", "20200115", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "csv",
                            createRangedDateFrequencyMap("20200110", "20200110", 1L, "20200111", "20200112", 5L, "20200114", "20200115", 5L));
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 5L));
            givenAggregatedFrequencyRow("URI", cf, "maze", createRangedDateFrequencyMap("20200216", "20200220", 5L, "20200221", "20200221", 1L, "20200222",
                            "20200302", 5L, "20200304", "20200315", 5L, "20200316", "20200316", 1L, "20200317", "20200328", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test specifying multiple fields to filter on.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMultipleFieldsSpecified(String cf) {
            // Retrieve field index holes for fields URI and EVENT_DATE.
            givenFields("URI", "EVENT_DATE");

            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200102", 5L, "20200103", "20200103", 1L, "20200104",
                            "20200104", 5L, "20200105", "20200105", 1L));
            // Index holes for ALPHA-csv on 20200110 and 20200113.
            givenAggregatedFrequencyRow("ALPHA", COLF_F, "csv", createRangedDateFrequencyMap("20200110", "20200115", 5L));
            givenAggregatedFrequencyRow("ALPHA", cf, "csv", createRangedDateFrequencyMap("20200110", "20200110", 1L, "20200111", "20200112", 5L, "20200113",
                            "20200113", 1L, "20200114", "20200115", 5L));
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 5L));
            givenAggregatedFrequencyRow("URI", cf, "maze", createRangedDateFrequencyMap("20200216", "20200220", 5L, "20200221", "20200221", 1L, "20200222",
                            "20200302", 5L, "20200303", "20200303", 1L, "20200304", "20200315", 5L, "20200316", "20200316", 1L, "20200317", "20200328", 5L));
            // Index hole for ZETA-wiki on 20200122.
            givenAggregatedFrequencyRow("ZETA", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("ZETA", cf, "wiki",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createIndexFieldHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test specifying datatypes.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testDatatypesSpecified(String cf) {
            // Retrieve field index holes for datatypes wiki and csv.
            givenDatatypes("wiki", "csv");

            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200102", 5L, "20200103", "20200103", 1L, "20200104",
                            "20200104", 5L, "20200105", "20200105", 1L));
            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "maze", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "maze", createRangedDateFrequencyMap("20200101", "20200102", 5L, "20200103", "20200103", 1L, "20200104",
                            "20200104", 5L, "20200105", "20200105", 1L));
            // Index holes for ALPHA-csv on 20200110 and 20200113.
            givenAggregatedFrequencyRow("ALPHA", COLF_F, "csv", createRangedDateFrequencyMap("20200110", "20200115", 5L));
            givenAggregatedFrequencyRow("ALPHA", cf, "csv", createRangedDateFrequencyMap("20200110", "20200110", 1L, "20200111", "20200112", 5L, "20200113",
                            "20200113", 1L, "20200114", "20200115", 5L));
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            // Index hole for EVENT_DATE-maze on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "maze", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "maze",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 5L));
            givenAggregatedFrequencyRow("URI", cf, "maze", createRangedDateFrequencyMap("20200216", "20200220", 5L, "20200221", "20200221", 1L, "20200222",
                            "20200302", 5L, "20200303", "20200303", 1L, "20200304", "20200315", 5L, "20200316", "20200316", 1L, "20200317", "20200328", 5L));
            // Index hole for ZETA-csv on 20200122.
            givenAggregatedFrequencyRow("ZETA", COLF_F, "csv", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("ZETA", cf, "csv",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            // Index hole for ZETA-imdb on 20200122.
            givenAggregatedFrequencyRow("ZETA", COLF_F, "imdb", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("ZETA", cf, "imdb",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("ALPHA", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createIndexFieldHole("ZETA", "csv", dateRange("20200122", "20200122")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test specifying fields and datatypes.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldsAndDatatypesSpecified(String cf) {
            // Retrieve field index holes for fields NAME and ZETA.
            givenFields("NAME", "ZETA");
            // Retrieve field index holes for datatypes wiki and csv.
            givenDatatypes("wiki", "csv");

            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200102", 5L, "20200103", "20200103", 1L, "20200104",
                            "20200104", 5L, "20200105", "20200105", 1L));
            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "maze", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "maze", createRangedDateFrequencyMap("20200101", "20200102", 5L, "20200103", "20200103", 1L, "20200104",
                            "20200104", 5L, "20200105", "20200105", 1L));
            // Index holes for ALPHA-csv on 20200110 and 20200113.
            givenAggregatedFrequencyRow("ALPHA", COLF_F, "csv", createRangedDateFrequencyMap("20200110", "20200115", 5L));
            givenAggregatedFrequencyRow("ALPHA", cf, "csv", createRangedDateFrequencyMap("20200110", "20200110", 1L, "20200111", "20200112", 5L, "20200113",
                            "20200113", 1L, "20200114", "20200115", 5L));
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            // Index hole for EVENT_DATE-maze on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "maze", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "maze",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 5L));
            givenAggregatedFrequencyRow("URI", cf, "maze", createRangedDateFrequencyMap("20200216", "20200220", 5L, "20200221", "20200221", 1L, "20200222",
                            "20200302", 5L, "20200303", "20200303", 1L, "20200304", "20200315", 5L, "20200316", "20200316", 1L, "20200317", "20200328", 5L));
            // Index hole for ZETA-csv on 20200122.
            givenAggregatedFrequencyRow("ZETA", COLF_F, "csv", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("ZETA", cf, "csv",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            // Index hole for ZETA-imdb on 20200122.
            givenAggregatedFrequencyRow("ZETA", COLF_F, "imdb", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("ZETA", cf, "imdb",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("ZETA", "csv", dateRange("20200122", "20200122")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }
    }

    /**
     * Tests for {@link AllFieldMetadataHelper#getFieldIndexHoles(Set, Set, double)} and
     * {@link AllFieldMetadataHelper#getReversedFieldIndexHoles(Set, Set, double)} where the metadata table contains both aggregated and non-aggregated entries.
     */
    @Nested
    public class IndexFieldHoleTestsForMixedAggregatedAndNonAggregatedEntries extends AbstractIndexFieldHoleTests {

        /**
         * Test against data that has no field index holes.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testNoIndexFieldHoles(String cf) {
            // Create a series of frequency rows over date ranges, each with a matching index row for each date.
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200101", "20200120", 1L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200120", 1L);
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "maze", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "maze", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "csv", "20200101", "20200120", 1L);
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200120", 1L));
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "maze", createRangedDateFrequencyMap("20200101", "20200110", 1L));
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "maze", "20200111", "20200120", 1L);
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "maze", createRangedDateFrequencyMap("20200101", "20200114", 1L));
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "maze", "20200115", "20200120", 1L);
            writeMutations();

            // Verify that no index holes were found.
            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            Assertions.assertTrue(IndexFieldHoles.isEmpty());
        }

        /**
         * Test against data that has field index holes for an entire fieldName-datatype combination based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForEntireFrequencyDateRange_dateGaps(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200103", 1L));
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200104", "20200105", 1L);
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200105")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has field index holes for an entire fieldName-datatype combination based on the threshold requirement.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForEntireFrequencyDateRange_threshold(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200102", 1L));
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200103", "20200105", 1L);
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200105")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole at the start of a frequency date range for a given fieldName-dataType combination based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForStartOfFrequencyDateRange_dateGaps(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200103", 1L));
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200104", "20200105", 1L);
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200104", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200103")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole at the start of a frequency date range for a given fieldName-dataType combination based on the
         * threshold requirement.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForStartOfFrequencyDateRange_threshold(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200103", 1L));
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200104", "20200105", 5L);
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200103")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole at the end of a frequency date range for a given fieldName-dataType combination based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForEndOfFrequencyDateRange_dateGaps(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200102", 1L);
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200105")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole at the end of a frequency date range for a given fieldName-dataType combination based on the
         * threshold requirement.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForEndOfFrequencyDateRange_thresholds(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200102", 5L));
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200103", "20200105", 1L);
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200105")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on date gaps.
         * This uses a negative index marker.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleWithNotIndexedMarker(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200110", 1L));
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200103", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200107", "20200110", 1L);
            givenIndexMarkerMutation("NAME", cf, "wiki", "20200109", false);
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200101", "20200105", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200109")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on the
         * threshold.
         * This uses a positive index marker.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleWithIndexedMarker(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200110", 1L);
            givenIndexMarkerMutation("NAME", cf, "wiki", "20200103", true);
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200107", "20200110", 1L));
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200101", "20200105", 1L);
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on the
         * threshold.
         * This uses a positive index marker derived from an older date-less format
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleWithIndexedMarkerSansDate(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200110", 1L));
            givenIndexMarkerMutation("NAME", cf, "wiki", "20200103");
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200107", "20200110", 1L));
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200101", "20200105", 1L);
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on the
         * threshold.
         * This uses a positive index marker derived from an older date-less format with type class
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleWithIndexedMarkerOldTypeFormat(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200110", 1L));
            givenIndexMarkerMutation("NAME", cf, "wiki", "20200103", LcNoDiacriticsType.class);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200107", "20200110", 1L);
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200101", "20200105", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleWithIndexedMarkerAndMissingFrequency(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200102", 1L);
            givenIndexMarkerMutation("NAME", cf, "wiki", "20200103", true);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200104", "20200110", 1L);
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200107", "20200110", 1L));
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200101", "20200105", 1L);
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForMiddleOfFrequencyDateRange_dateGaps(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200110", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200103", 1L));
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200107", "20200110", 1L);
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on the
         * threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForMiddleOfFrequencyDateRange_threshold(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200110", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200103", 5L, "20200104", "20200106", 1L));
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200107", "20200110", 5L);
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200101", "20200105", 5L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on both date
         * gaps and the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleForMiddleOfFrequencyDateRange_mixed(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200107", 5L));
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200108", "20200110", 5L);
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200103", 5L, "20200105", "20200106", 1L));
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200107", "20200110", 5L);
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has multiple field index holes for a given fieldName-datatype combination based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMultipleIndexFieldHolesInFrequencyDateRange_dateGap(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200114", 1L));
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200115", "20200120", 1L);
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200104", "20200106", 1L));
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200110", "20200113", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200117", "20200118", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200103"),
                                            dateRange("20200107", "20200109"),
                                            dateRange("20200114", "20200116"),
                                            dateRange("20200119", "20200120")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data that has multiple field index holes for a given fieldName-datatype combination based on the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMultipleIndexFieldHolesInFrequencyDateRange_threshold(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200120", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200103", 1L, "20200104", "20200106", 5L, "20200107",
                            "20200109", 1L, "20200110", "20200113", 5L));
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200114", "20200116", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200117", "20200118", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200119", "20200120", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200103"),
                                            dateRange("20200107", "20200109"),
                                            dateRange("20200114", "20200116"),
                                            dateRange("20200119", "20200120")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where the expected index hole occurs for the end of a frequency range right before a new fieldName-datatype combination based on
         * date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleAtEndOfFrequencyDateRangeForNonLastCombo_dateGap(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200102", 1L));
            givenNonAggregatedFrequencyRows("ZETA", COLF_F, "csv", "20200101", "20200105", 1L);
            givenNonAggregatedFrequencyRows("ZETA", cf, "csv", "20200101", "20200105", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200105")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where the expected index hole occurs for the end of a frequency range right before a new fieldName-datatype combination based on
         * the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleAtEndOfFrequencyDateRangeForNonLastCombo_threshold(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200102", 5L));
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200103", "20200105", 1L);
            givenAggregatedFrequencyRow("ZETA", COLF_F, "csv", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("ZETA", cf, "csv", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200105")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where the expected index hole spans across multiple frequency ranges based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleSpanningMultipleFrequencyDateRanges_dateGaps(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200110", "20200115", 1L);
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200103", 1L));
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200113", "20200115", 1L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200112")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where the expected index hole spans across multiple frequency ranges based on the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testIndexFieldHoleSpanningMultipleFrequencyDateRanges_threshold(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L, "20200110", "20200115", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200103", 5L, "20200104", "20200105", 1L));
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200110", "20200112", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200113", "20200115", 5L);
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200104", "20200112")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where everything is an index hole based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testAllDatesAreIndexHoles_dateGaps(String cf) {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200105", 1L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200110", "20200115", 1L);
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 1L));
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200115")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200120", "20200125")),
                            createIndexFieldHole("URI", "maze", dateRange("20200216", "20200328")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where everything is an index hole based on the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testAllDatesAreIndexHoles_threshold(String cf) {
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200105", 1L); // Will not meet threshold.
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200110", "20200115", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200110", "20200115", 1L)); // Will not meet threshold.
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "wiki", "20200120", "20200125", 5L);
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 1L)); // Will not meet threshold.
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 5L));
            givenNonAggregatedFrequencyRows("URI", cf, "maze", "20200216", "20200328", 1L); // Will not meet threshold.
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200101", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200115")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200120", "20200125")),
                            createIndexFieldHole("URI", "maze", dateRange("20200216", "20200328")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where we have a number of index holes that span just a day based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testSingularDayIndexHoles_dateGaps(String cf) {
            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200102", 1L));
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200104", "20200104", 1L);
            // Index holes for NAME-csv on 20200110 and 20200113.
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200110", "20200115", 1L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200111", "20200112", 1L));
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200114", "20200115", 1L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 1L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki", createRangedDateFrequencyMap("20200120", "20200121", 1L));
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200123", "20200125", 1L);
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 1L));
            givenAggregatedFrequencyRow("URI", cf, "maze", createRangedDateFrequencyMap("20200216", "20200220", 1L, "20200222", "20200302", 1L, "20200304",
                            "20200315", 1L, "20200317", "20200328", 1L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createIndexFieldHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where we have a number of index holes that span just a day based on the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testSingularDayIndexHoles_threshold(String cf) {
            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200102", 5L, "20200103", "20200103", 1L));
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200104", "20200104", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200105", "20200105", 1L);
            // Index holes for NAME-csv on 20200110 and 20200113.
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200110", "20200115", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "csv", createRangedDateFrequencyMap("20200110", "20200110", 1L, "20200111", "20200112", 5L, "20200113",
                            "20200113", 1L, "20200114", "20200115", 5L));
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki", createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L));
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "wiki", "20200123", "20200125", 5L);
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 5L));
            givenAggregatedFrequencyRow("URI", cf, "maze", createRangedDateFrequencyMap("20200216", "20200220", 5L, "20200221", "20200221", 1L, "20200222",
                            "20200302", 5L, "20200303", "20200303", 1L, "20200304", "20200315", 5L, "20200316", "20200316", 1L, "20200317", "20200328", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createIndexFieldHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test against data where we have a number of index holes that span just a day based on both dates and the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMixedDateGapsAndThresholdIndexHoles(String cf) {
            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200101", "20200102", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200104", "20200104", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200105", "20200105", 1L);
            // Index holes for NAME-csv on 20200110 and 20200113.
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200110", "20200115", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "csv",
                            createRangedDateFrequencyMap("20200110", "20200110", 1L, "20200111", "20200112", 5L, "20200114", "20200115", 5L));
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 5L));
            givenAggregatedFrequencyRow("URI", cf, "maze", createRangedDateFrequencyMap("20200216", "20200220", 5L, "20200221", "20200221", 1L, "20200222",
                            "20200302", 5L, "20200304", "20200315", 5L, "20200316", "20200316", 1L, "20200317", "20200328", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createIndexFieldHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test specifying a minimum percentage threshold other than the default of 1.0.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMinimumThresholdPercentageBelow100(String cf) {
            givenMinimumThreshold(0.75); // Index count must meet 75% of frequency count to not be considered field index hole.

            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 100L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki",
                            createRangedDateFrequencyMap("20200101", "20200102", 75L, "20200104", "20200104", 100L, "20200105", "20200105", 74L));
            // Index holes for NAME-csv on 20200110 and 20200113.
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createRangedDateFrequencyMap("20200110", "20200115", 100L));
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200110", "20200110", 74L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200111", "20200112", 75L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200114", "20200115", 100L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 100L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki",
                            createRangedDateFrequencyMap("20200120", "20200121", 98L, "20200122", "20200122", 74L, "20200123", "20200125", 75L));
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 100L));
            givenAggregatedFrequencyRow("URI", cf, "maze", createRangedDateFrequencyMap("20200216", "20200220", 100L, "20200221", "20200221", 74L, "20200222",
                            "20200302", 90L, "20200304", "20200315", 75L, "20200316", "20200316", 74L, "20200317", "20200328", 99L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createIndexFieldHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test specifying one field to filter on.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testOneFieldSpecified(String cf) {
            // Retrieve field index holes for field NAME.
            givenFields("NAME");

            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki",
                            createRangedDateFrequencyMap("20200101", "20200102", 5L, "20200104", "20200104", 5L, "20200105", "20200105", 1L));
            // Index holes for NAME-csv on 20200110 and 20200113.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200110", "20200115", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200110", "20200110", 1L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200111", "20200112", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "csv", "20200114", "20200115", 5L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 5L));
            givenAggregatedFrequencyRow("URI", cf, "maze", createRangedDateFrequencyMap("20200216", "20200220", 5L, "20200221", "20200221", 1L, "20200222",
                            "20200302", 5L, "20200304", "20200315", 5L, "20200316", "20200316", 1L, "20200317", "20200328", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test specifying multiple fields to filter on.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMultipleFieldsSpecified(String cf) {
            // Retrieve field index holes for fields URI and EVENT_DATE.
            givenFields("URI", "EVENT_DATE");

            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200102", 5L, "20200103", "20200103", 1L, "20200104",
                            "20200104", 5L, "20200105", "20200105", 1L));
            // Index holes for ALPHA-csv on 20200110 and 20200113.
            givenAggregatedFrequencyRow("ALPHA", COLF_F, "csv", createRangedDateFrequencyMap("20200110", "20200115", 5L));
            givenAggregatedFrequencyRow("ALPHA", cf, "csv", createRangedDateFrequencyMap("20200110", "20200110", 1L, "20200111", "20200112", 5L));
            givenNonAggregatedFrequencyRows("ALPHA", cf, "csv", "20200113", "20200113", 1L);
            givenNonAggregatedFrequencyRows("ALPHA", cf, "csv", "20200114", "20200115", 5L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 5L));
            givenAggregatedFrequencyRow("URI", cf, "maze", createRangedDateFrequencyMap("20200216", "20200220", 5L, "20200221", "20200221", 1L, "20200222",
                            "20200302", 5L, "20200303", "20200303", 1L, "20200304", "20200315", 5L, "20200316", "20200316", 1L, "20200317", "20200328", 5L));
            // Index hole for ZETA-wiki on 20200122.
            givenAggregatedFrequencyRow("ZETA", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("ZETA", cf, "wiki", createRangedDateFrequencyMap("20200120", "20200121", 5L));
            givenNonAggregatedFrequencyRows("ZETA", cf, "wiki", "20200122", "20200122", 1L);
            givenNonAggregatedFrequencyRows("ZETA", cf, "wiki", "20200123", "20200125", 5L);

            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createIndexFieldHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test specifying datatypes.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testDatatypesSpecified(String cf) {
            // Retrieve field index holes for datatypes wiki and csv.
            givenDatatypes("wiki", "csv");

            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200102", 5L, "20200103", "20200103", 1L));
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200104", "20200104", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200105", "20200105", 1L);

            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "maze", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "maze", createRangedDateFrequencyMap("20200101", "20200102", 5L, "20200103", "20200103", 1L, "20200104",
                            "20200104", 5L, "20200105", "20200105", 1L));
            // Index holes for ALPHA-csv on 20200110 and 20200113.
            givenNonAggregatedFrequencyRows("ALPHA", COLF_F, "csv", "20200110", "20200115", 5L);
            givenNonAggregatedFrequencyRows("ALPHA", cf, "csv", "20200110", "20200110", 1L);
            givenNonAggregatedFrequencyRows("ALPHA", cf, "csv", "20200111", "20200112", 5L);
            givenNonAggregatedFrequencyRows("ALPHA", cf, "csv", "20200113", "20200113", 1L);
            givenNonAggregatedFrequencyRows("ALPHA", cf, "csv", "20200114", "20200115", 5L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            // Index hole for EVENT_DATE-maze on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "maze", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "maze", createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L));
            givenNonAggregatedFrequencyRows("EVENT_DATE", cf, "maze", "20200123", "20200125", 5L);
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 5L));
            givenAggregatedFrequencyRow("URI", cf, "maze", createRangedDateFrequencyMap("20200216", "20200220", 5L, "20200221", "20200221", 1L, "20200222",
                            "20200302", 5L, "20200303", "20200303", 1L, "20200304", "20200315", 5L, "20200316", "20200316", 1L, "20200317", "20200328", 5L));
            // Index hole for ZETA-csv on 20200122.
            givenAggregatedFrequencyRow("ZETA", COLF_F, "csv", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("ZETA", cf, "csv",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            // Index hole for ZETA-imdb on 20200122.
            givenAggregatedFrequencyRow("ZETA", COLF_F, "imdb", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("ZETA", cf, "imdb",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("ALPHA", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createIndexFieldHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createIndexFieldHole("ZETA", "csv", dateRange("20200122", "20200122")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }

        /**
         * Test specifying fields and datatypes.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldsAndDatatypesSpecified(String cf) {
            // Retrieve field index holes for fields NAME and ZETA.
            givenFields("NAME", "ZETA");
            // Retrieve field index holes for datatypes wiki and csv.
            givenDatatypes("wiki", "csv");

            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "wiki", createRangedDateFrequencyMap("20200101", "20200102", 5L, "20200103", "20200103", 1L));
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200104", "20200104", 5L);
            givenNonAggregatedFrequencyRows("NAME", cf, "wiki", "20200105", "20200105", 1L);
            // Index holes for NAME-wiki on 20200103 and 20200105.
            givenAggregatedFrequencyRow("NAME", COLF_F, "maze", createRangedDateFrequencyMap("20200101", "20200105", 5L));
            givenAggregatedFrequencyRow("NAME", cf, "maze", createRangedDateFrequencyMap("20200101", "20200102", 5L, "20200103", "20200103", 1L, "20200104",
                            "20200104", 5L, "20200105", "20200105", 1L));
            // Index holes for ALPHA-csv on 20200110 and 20200113.
            givenAggregatedFrequencyRow("ALPHA", COLF_F, "csv", createRangedDateFrequencyMap("20200110", "20200115", 5L));
            givenAggregatedFrequencyRow("ALPHA", cf, "csv", createRangedDateFrequencyMap("20200110", "20200110", 1L, "20200111", "20200112", 5L, "20200113",
                            "20200113", 1L, "20200114", "20200115", 5L));
            // Index hole for EVENT_DATE-wiki on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "wiki",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            // Index hole for EVENT_DATE-maze on 20200122.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "maze", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("EVENT_DATE", cf, "maze",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            givenAggregatedFrequencyRow("URI", COLF_F, "maze", createRangedDateFrequencyMap("20200216", "20200328", 5L));
            givenAggregatedFrequencyRow("URI", cf, "maze", createRangedDateFrequencyMap("20200216", "20200220", 5L, "20200221", "20200221", 1L, "20200222",
                            "20200302", 5L, "20200303", "20200303", 1L, "20200304", "20200315", 5L, "20200316", "20200316", 1L, "20200317", "20200328", 5L));
            // Index hole for ZETA-csv on 20200122.
            givenAggregatedFrequencyRow("ZETA", COLF_F, "csv", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("ZETA", cf, "csv",
                            createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L, "20200123", "20200125", 5L));
            // Index hole for ZETA-imdb on 20200122.
            givenAggregatedFrequencyRow("ZETA", COLF_F, "imdb", createRangedDateFrequencyMap("20200120", "20200125", 5L));
            givenAggregatedFrequencyRow("ZETA", cf, "imdb", createRangedDateFrequencyMap("20200120", "20200121", 5L, "20200122", "20200122", 1L));
            givenNonAggregatedFrequencyRows("ZETA", cf, "imdb", "20200123", "20200125", 5L);

            writeMutations();

            Map<String,Map<String,IndexFieldHole>> IndexFieldHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createIndexFieldHoleMap(
                            createIndexFieldHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createIndexFieldHole("ZETA", "csv", dateRange("20200122", "20200122")));
            // @formatter:on
            Assertions.assertEquals(expected, IndexFieldHoles);
        }
    }
}
