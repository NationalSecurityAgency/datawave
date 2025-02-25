package datawave.query.util;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Calendar;
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
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.LcNoDiacriticsType;
import datawave.query.composite.CompositeMetadataHelper;
import datawave.query.model.IndexFieldHole;
import datawave.util.time.DateHelper;

class AllFieldMetadataHelperTest {
    
    private static final String TABLE_METADATA = "metadata";
    private static final String[] AUTHS = {"FOO"};
    private static final String NULL_BYTE = "\0";
    private AccumuloClient accumuloClient;
    private AllFieldMetadataHelper helper;
    
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
        helper = new AllFieldMetadataHelper(typeMetadataHelper, compositeMetadataHelper, accumuloClient, TABLE_METADATA, auths, allMetadataAuths);
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
     * Tests for {@link AllFieldMetadataHelper#getFieldIndexHoles(Set, Set, double)} and
     * {@link AllFieldMetadataHelper#getReversedFieldIndexHoles(Set, Set, double)}.
     */
    @Nested
    public class FieldIndexHoleTests {
        
        private Set<String> fields = new HashSet<>();
        private Set<String> datatypes = new HashSet<>();
        private double minimumThreshold = 1.0d;
        
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
        
        /**
         * Test against data that has no field index holes.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testNoFieldIndexHoles(String cf) {
            // Create a series of frequency rows over date ranges, each with a matching index row for each date.
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200101", "20200120", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200101", "20200120", 1L);
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200120", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200120", 1L);
            mutationCreator.addFrequencyMutations("NAME", "maze", "20200101", "20200120", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "maze", "20200101", "20200120", 1L);
            mutationCreator.addFrequencyMutations("EVENT_DATE", "csv", "20200101", "20200120", 1L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "csv", "20200101", "20200120", 1L);
            mutationCreator.addFrequencyMutations("EVENT_DATE", "wiki", "20200101", "20200120", 1L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200101", "20200120", 1L);
            mutationCreator.addFrequencyMutations("EVENT_DATE", "maze", "20200101", "20200120", 1L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "maze", "20200101", "20200120", 1L);
            writeMutations(mutationCreator.getMutations());
            
            // Verify that no index holes were found.
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            Assertions.assertTrue(fieldIndexHoles.isEmpty());
        }
        
        /**
         * Test against data that has field index holes for an entire fieldName-datatype combination based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleForEntireFrequencyDateRange_dateGaps(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 1L);
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200101", "20200105", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200101", "20200105", 1L);
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(createFieldIndexHole("NAME", "wiki", dateRange("20200101", "20200105")));
            // @formatter:off
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
    
        /**
         * Test against data that has field index holes for an entire fieldName-datatype combination based on the threshold requirement.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleForEntireFrequencyDateRange_threshold(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200105", 1L); // Make the index counts a value that will not meet the threshold.
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200101", "20200105", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200101", "20200105", 1L);
            writeMutations(mutationCreator.getMutations());
        
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(createFieldIndexHole("NAME", "wiki", dateRange("20200101", "20200105")));
            // @formatter:off
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data that has a field index hole at the start of a frequency date range for a given fieldName-dataType combination based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleForStartOfFrequencyDateRange_dateGaps(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200104", "20200105", 1L);
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200101", "20200105", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200101", "20200105", 1L);
            writeMutations(mutationCreator.getMutations());
    
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(createFieldIndexHole("NAME", "wiki", dateRange("20200101", "20200103")));
            // @formatter:off
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
    
        /**
         * Test against data that has a field index hole at the start of a frequency date range for a given fieldName-dataType combination based on the
         * threshold requirement.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleForStartOfFrequencyDateRange_threshold(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200103", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200104", "20200105", 5L);
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200101", "20200105", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200101", "20200105", 5L);
            writeMutations(mutationCreator.getMutations());
        
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(createFieldIndexHole("NAME", "wiki", dateRange("20200101", "20200103")));
            // @formatter:off
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data that has a field index hole at the end of a frequency date range for a given fieldName-dataType combination based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleForEndOfFrequencyDateRange_dateGaps(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200102", 1L);
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200101", "20200105", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200101", "20200105", 1L);
            writeMutations(mutationCreator.getMutations());

            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(createFieldIndexHole("NAME", "wiki", dateRange("20200103", "20200105")));
            // @formatter:off
            Assertions.assertEquals(expected, fieldIndexHoles);
        }

        /**
         * Test against data that has a field index hole at the end of a frequency date range for a given fieldName-dataType combination based on the
         * threshold requirement.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleForEndOfFrequencyDateRange_thresholds(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200102", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200103", "20200105", 1L); // Will not meet threshold.
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200101", "20200105", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200101", "20200105", 5L);
            writeMutations(mutationCreator.getMutations());

            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(createFieldIndexHole("NAME", "wiki", dateRange("20200103", "20200105")));
            // @formatter:off
            Assertions.assertEquals(expected, fieldIndexHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on date gaps.
         * This uses a negative index marker.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleWithNotIndexedMarker(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200110", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200103", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200107", "20200110", 1L);
            mutationCreator.addIndexMarkerMutation(cf, "NAME", "wiki", "20200109", false);
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200101", "20200105", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200101", "20200105", 1L);
            writeMutations(mutationCreator.getMutations());
    
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(createFieldIndexHole("NAME", "wiki", dateRange("20200101", "20200109")));
            // @formatter:off
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
    
        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on the
         * threshold.
         * This uses a positive index marker.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleWithIndexedMarker(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200110", 1L);
            mutationCreator.addIndexMarkerMutation(cf, "NAME", "wiki", "20200103", true);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200107", "20200110", 1L);
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200101", "20200105", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200101", "20200105", 1L);
            writeMutations(mutationCreator.getMutations());

            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(createFieldIndexHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, fieldIndexHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on the
         * threshold.
         * This uses a positive index marker derived from an older date-less format
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleWithIndexedMarkerSansDate(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200110", 1L);
            mutationCreator.addIndexMarkerMutation(cf, "NAME", "wiki", "20200103");
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200107", "20200110", 1L);
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200101", "20200105", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200101", "20200105", 1L);
            writeMutations(mutationCreator.getMutations());

            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(createFieldIndexHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, fieldIndexHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on the
         * threshold.
         * This uses a positive index marker derived from an older date-less format
         * This tests that the having and old style marker and new style index mutations on the same day works correctly
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleWithIndexedMarkerSansDateAndCount(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200110", 2L);
            mutationCreator.addIndexMarkerMutation(cf, "NAME", "wiki", "20200103");
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200103", "20200103", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200104", "20200106", 2L);
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200101", "20200105", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200101", "20200105", 1L);
            writeMutations(mutationCreator.getMutations());

            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(createFieldIndexHole("NAME", "wiki", dateRange("20200107", "20200110")));
            // @formatter:off
            Assertions.assertEquals(expected, fieldIndexHoles);
        }

        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on the
         * threshold.
         * This uses a positive index marker derived from an older date-less format with type class
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleWithIndexedMarkerOldTypeFormat(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200110", 1L);
            mutationCreator.addIndexMarkerMutation(cf, "NAME", "wiki", "20200103", LcNoDiacriticsType.class);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200107", "20200110", 1L);
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200101", "20200105", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200101", "20200105", 1L);
            writeMutations(mutationCreator.getMutations());

            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(createFieldIndexHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, fieldIndexHoles);
        }

        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleWithIndexedMarkerAndMissingFrequency(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200102", 1L);
            mutationCreator.addIndexMarkerMutation(cf, "NAME", "wiki", "20200103", true);
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200104", "20200110", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200107", "20200110", 1L);
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200101", "20200105", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200101", "20200105", 1L);
            writeMutations(mutationCreator.getMutations());

            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(createFieldIndexHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, fieldIndexHoles);
        }

        /**
         * Test against data where we have a number of index holes that span just a day based on both dates and the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMixedDateGapsAndNonIndexedFields(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            // Not indexed nor covers full range for NAME
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200102", 5L);
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200110", "20200115", 5L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            mutationCreator.addFrequencyMutations("EVENT_DATE", "wiki", "20200120", "20200125", 5L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200120", "20200121", 5L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200122", "20200122", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200123", "20200125", 5L);
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            mutationCreator.addFrequencyMutations("URI", "maze", "20200216", "20200328", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200216", "20200220", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200221", "20200221", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200222", "20200302", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200304", "20200315", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200316", "20200316", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200317", "20200328", 5L);
            writeMutations(mutationCreator.getMutations());

            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(
                    createFieldIndexHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                    createFieldIndexHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data that has a field index hole in the middle of a frequency date range for a given fieldName-datatype combination based on both date
         * gaps and the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleForMiddleOfFrequencyDateRange_mixed(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200110", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200103", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200105", "20200106", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200107", "20200110", 5L);
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200101", "20200105", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200101", "20200105", 5L);
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:on
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(createFieldIndexHole("NAME", "wiki", dateRange("20200104", "20200106")));
            // @formatter:off
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data that has multiple field index holes for a given fieldName-datatype combination based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMultipleFieldIndexHolesInFrequencyDateRange_dateGap(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200120", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200104", "20200106", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200110", "20200113", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200117", "20200118", 1L);
            writeMutations(mutationCreator.getMutations());
    
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200101", "20200103"),
                                            dateRange("20200107", "20200109"),
                                            dateRange("20200114", "20200116"),
                                            dateRange("20200119", "20200120")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data that has multiple field index holes for a given fieldName-datatype combination based on the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMultipleFieldIndexHolesInFrequencyDateRange_threshold(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200120", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200103", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200104", "20200106", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200107", "20200109", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200110", "20200113", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200114", "20200116", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200117", "20200118", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200119", "20200120", 1L); // Will not meet threshold.
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200101", "20200103"),
                                            dateRange("20200107", "20200109"),
                                            dateRange("20200114", "20200116"),
                                            dateRange("20200119", "20200120")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data where the expected index hole occurs for the end of a frequency range right before a new fieldName-datatype combination based on
         * date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleAtEndOfFrequencyDateRangeForNonLastCombo_dateGap(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200102", 1L);
            mutationCreator.addFrequencyMutations("ZETA", "csv", "20200101", "20200105", 1L);
            mutationCreator.addIndexMutations(cf, "ZETA", "csv", "20200101", "20200105", 1L);
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200103", "20200105")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data where the expected index hole occurs for the end of a frequency range right before a new fieldName-datatype combination based on
         * the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleAtEndOfFrequencyDateRangeForNonLastCombo_threshold(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200102", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200103", "20200105", 1L); // Will not meet threshold.
            mutationCreator.addFrequencyMutations("ZETA", "csv", "20200101", "20200105", 5L);
            mutationCreator.addIndexMutations(cf, "ZETA", "csv", "20200101", "20200105", 5L);
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200103", "20200105")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data where the expected index hole spans across multiple frequency ranges based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleSpanningMultipleFrequencyDateRanges_dateGaps(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 1L);
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200110", "20200115", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200103", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200113", "20200115", 1L);
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200104", "20200112")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data where the expected index hole spans across multiple frequency ranges based on the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testFieldIndexHoleSpanningMultipleFrequencyDateRanges_threshold(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200103", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200104", "20200105", 1L); // Will not meet threshold.
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200110", "20200115", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200110", "20200112", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200113", "20200115", 5L);
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200104", "20200112")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data where everything is an index hole based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testAllDatesAreIndexHoles_dateGaps(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 1L);
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200110", "20200115", 1L);
            mutationCreator.addFrequencyMutations("EVENT_DATE", "wiki", "20200120", "20200125", 1L);
            mutationCreator.addFrequencyMutations("URI", "maze", "20200216", "20200328", 1L);
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200101", "20200105")),
                            createFieldIndexHole("NAME", "csv", dateRange("20200110", "20200115")),
                            createFieldIndexHole("EVENT_DATE", "wiki", dateRange("20200120", "20200125")),
                            createFieldIndexHole("URI", "maze", dateRange("20200216", "20200328")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data where everything is an index hole based on the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testAllDatesAreIndexHoles_threshold(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200105", 1L); // Will not meet threshold.
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200110", "20200115", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200110", "20200115", 1L); // Will not meet threshold.
            mutationCreator.addFrequencyMutations("EVENT_DATE", "wiki", "20200120", "20200125", 5L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200120", "20200125", 1L); // Will not meet threshold.
            mutationCreator.addFrequencyMutations("URI", "maze", "20200216", "20200328", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200216", "20200328", 1L); // Will not meet threshold.
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200101", "20200105")),
                            createFieldIndexHole("NAME", "csv", dateRange("20200110", "20200115")),
                            createFieldIndexHole("EVENT_DATE", "wiki", dateRange("20200120", "20200125")),
                            createFieldIndexHole("URI", "maze", dateRange("20200216", "20200328")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data where we have a number of index holes that span just a day based on date gaps.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testSingularDayIndexHoles_dateGaps(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            // Index holes for NAME-wiki on 20200103 and 20200105.
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200102", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200104", "20200104", 1L);
            // Index holes for NAME-csv on 20200110 and 20200113.
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200110", "20200115", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200111", "20200112", 1L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200114", "20200115", 1L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            mutationCreator.addFrequencyMutations("EVENT_DATE", "wiki", "20200120", "20200125", 1L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200120", "20200121", 1L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200123", "20200125", 1L);
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            mutationCreator.addFrequencyMutations("URI", "maze", "20200216", "20200328", 1L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200216", "20200220", 1L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200222", "20200302", 1L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200304", "20200315", 1L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200317", "20200328", 1L);
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createFieldIndexHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createFieldIndexHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createFieldIndexHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data where we have a number of index holes that span just a day based on the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testSingularDayIndexHoles_threshold(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            // Index holes for NAME-wiki on 20200103 and 20200105.
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200102", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200103", "20200103", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200104", "20200104", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200105", "20200105", 1L); // Will not meet threshold.
            // Index holes for NAME-csv on 20200110 and 20200113.
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200110", "20200115", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200110", "20200110", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200111", "20200112", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200113", "20200113", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200114", "20200115", 5L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            mutationCreator.addFrequencyMutations("EVENT_DATE", "wiki", "20200120", "20200125", 5L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200120", "20200121", 5L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200122", "20200122", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200123", "20200125", 5L);
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            mutationCreator.addFrequencyMutations("URI", "maze", "20200216", "20200328", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200216", "20200220", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200221", "20200221", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200222", "20200302", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200303", "20200303", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200304", "20200315", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200316", "20200316", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200317", "20200328", 5L);
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createFieldIndexHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createFieldIndexHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createFieldIndexHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test against data where we have a number of index holes that span just a day based on both dates and the threshold.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMixedDateGapsAndThresholdIndexHoles(String cf) {
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            // Index holes for NAME-wiki on 20200103 and 20200105.
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200102", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200104", "20200104", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200105", "20200105", 1L); // Will not meet threshold.
            // Index holes for NAME-csv on 20200110 and 20200113.
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200110", "20200115", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200110", "20200110", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200111", "20200112", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200114", "20200115", 5L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            mutationCreator.addFrequencyMutations("EVENT_DATE", "wiki", "20200120", "20200125", 5L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200120", "20200121", 5L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200122", "20200122", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200123", "20200125", 5L);
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            mutationCreator.addFrequencyMutations("URI", "maze", "20200216", "20200328", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200216", "20200220", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200221", "20200221", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200222", "20200302", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200304", "20200315", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200316", "20200316", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200317", "20200328", 5L);
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createFieldIndexHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createFieldIndexHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createFieldIndexHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test specifying a minimum percentage threshold other than the default of 1.0.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMinimumThresholdPercentageBelow100(String cf) {
            givenMinimumThreshold(0.75); // Index count must meet 75% of frequency count to not be considered field index hole.
            
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            // Index holes for NAME-wiki on 20200103 and 20200105.
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 100L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200102", 75L); // Meets 75% threshold.
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200104", "20200104", 100L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200105", "20200105", 74L); // Will not meet threshold.
            // Index holes for NAME-csv on 20200110 and 20200113.
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200110", "20200115", 100L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200110", "20200110", 74L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200111", "20200112", 75L); // Meets 75% threshold.
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200114", "20200115", 100L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            mutationCreator.addFrequencyMutations("EVENT_DATE", "wiki", "20200120", "20200125", 100L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200120", "20200121", 98L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200122", "20200122", 74L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200123", "20200125", 75L); // Meets 75% threshold.
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            mutationCreator.addFrequencyMutations("URI", "maze", "20200216", "20200328", 100L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200216", "20200220", 100L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200221", "20200221", 74L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200222", "20200302", 90L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200304", "20200315", 75L); // Meets 75% threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200316", "20200316", 74L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200317", "20200328", 99L);
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createFieldIndexHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createFieldIndexHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createFieldIndexHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test specifying one field to filter on.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testOneFieldSpecified(String cf) {
            // Retrieve field index holes for field NAME.
            givenFields("NAME");
            
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            // Index holes for NAME-wiki on 20200103 and 20200105.
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200102", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200104", "20200104", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200105", "20200105", 1L); // Will not meet threshold.
            // Index holes for NAME-csv on 20200110 and 20200113.
            mutationCreator.addFrequencyMutations("NAME", "csv", "20200110", "20200115", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200110", "20200110", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200111", "20200112", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "csv", "20200114", "20200115", 5L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            mutationCreator.addFrequencyMutations("EVENT_DATE", "wiki", "20200120", "20200125", 5L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200120", "20200121", 5L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200122", "20200122", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200123", "20200125", 5L);
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            mutationCreator.addFrequencyMutations("URI", "maze", "20200216", "20200328", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200216", "20200220", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200221", "20200221", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200222", "20200302", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200304", "20200315", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200316", "20200316", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200317", "20200328", 5L);
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createFieldIndexHole("NAME", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test specifying multiple fields to filter on.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testMultipleFieldsSpecified(String cf) {
            // Retrieve field index holes for fields URI and EVENT_DATE.
            givenFields("URI", "EVENT_DATE");
            
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            // Index holes for NAME-wiki on 20200103 and 20200105.
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200102", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200103", "20200103", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200104", "20200104", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200105", "20200105", 1L); // Will not meet threshold.
            // Index holes for ALPHA-csv on 20200110 and 20200113.
            mutationCreator.addFrequencyMutations("ALPHA", "csv", "20200110", "20200115", 5L);
            mutationCreator.addIndexMutations(cf, "ALPHA", "csv", "20200110", "20200110", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "ALPHA", "csv", "20200111", "20200112", 5L);
            mutationCreator.addIndexMutations(cf, "ALPHA", "csv", "20200113", "20200113", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "ALPHA", "csv", "20200114", "20200115", 5L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            mutationCreator.addFrequencyMutations("EVENT_DATE", "wiki", "20200120", "20200125", 5L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200120", "20200121", 5L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200122", "20200122", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200123", "20200125", 5L);
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            mutationCreator.addFrequencyMutations("URI", "maze", "20200216", "20200328", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200216", "20200220", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200221", "20200221", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200222", "20200302", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200303", "20200303", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200304", "20200315", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200316", "20200316", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200317", "20200328", 5L);
            // Index hole for ZETA-wiki on 20200122.
            mutationCreator.addFrequencyMutations("ZETA", "wiki", "20200120", "20200125", 5L);
            mutationCreator.addIndexMutations(cf, "ZETA", "wiki", "20200120", "20200121", 5L);
            mutationCreator.addIndexMutations(cf, "ZETA", "wiki", "20200122", "20200122", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "ZETA", "wiki", "20200123", "20200125", 5L);
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createFieldIndexHole("URI", "maze", dateRange("20200221", "20200221"), dateRange("20200303", "20200303"),
                                            dateRange("20200316", "20200316")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        /**
         * Test specifying datatypes.
         */
        @ParameterizedTest
        @ValueSource(strings = {"i", "ri"})
        void testDatatypesSpecified(String cf) {
            // Retrieve field index holes for datatypes wiki and csv.
            givenDatatypes("wiki", "csv");
            
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            // Index holes for NAME-wiki on 20200103 and 20200105.
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200102", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200103", "20200103", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200104", "20200104", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200105", "20200105", 1L); // Will not meet threshold.
            // Index holes for NAME-wiki on 20200103 and 20200105.
            mutationCreator.addFrequencyMutations("NAME", "maze", "20200101", "20200105", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "maze", "20200101", "20200102", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "maze", "20200103", "20200103", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "NAME", "maze", "20200104", "20200104", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "maze", "20200105", "20200105", 1L); // Will not meet threshold.
            // Index holes for ALPHA-csv on 20200110 and 20200113.
            mutationCreator.addFrequencyMutations("ALPHA", "csv", "20200110", "20200115", 5L);
            mutationCreator.addIndexMutations(cf, "ALPHA", "csv", "20200110", "20200110", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "ALPHA", "csv", "20200111", "20200112", 5L);
            mutationCreator.addIndexMutations(cf, "ALPHA", "csv", "20200113", "20200113", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "ALPHA", "csv", "20200114", "20200115", 5L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            mutationCreator.addFrequencyMutations("EVENT_DATE", "wiki", "20200120", "20200125", 5L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200120", "20200121", 5L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200122", "20200122", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200123", "20200125", 5L);
            // Index hole for EVENT_DATE-maze on 20200122.
            mutationCreator.addFrequencyMutations("EVENT_DATE", "maze", "20200120", "20200125", 5L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "maze", "20200120", "20200121", 5L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "maze", "20200122", "20200122", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "maze", "20200123", "20200125", 5L);
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            mutationCreator.addFrequencyMutations("URI", "maze", "20200216", "20200328", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200216", "20200220", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200221", "20200221", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200222", "20200302", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200303", "20200303", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200304", "20200315", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200316", "20200316", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200317", "20200328", 5L);
            // Index hole for ZETA-csv on 20200122.
            mutationCreator.addFrequencyMutations("ZETA", "csv", "20200120", "20200125", 5L);
            mutationCreator.addIndexMutations(cf, "ZETA", "csv", "20200120", "20200121", 5L);
            mutationCreator.addIndexMutations(cf, "ZETA", "csv", "20200122", "20200122", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "ZETA", "csv", "20200123", "20200125", 5L);
            // Index hole for ZETA-imdb on 20200122.
            mutationCreator.addFrequencyMutations("ZETA", "imdb", "20200120", "20200125", 5L);
            mutationCreator.addIndexMutations(cf, "ZETA", "imdb", "20200120", "20200121", 5L);
            mutationCreator.addIndexMutations(cf, "ZETA", "imdb", "20200122", "20200122", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "ZETA", "imdb", "20200123", "20200125", 5L);
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createFieldIndexHole("ALPHA", "csv", dateRange("20200110", "20200110"), dateRange("20200113", "20200113")),
                            createFieldIndexHole("EVENT_DATE", "wiki", dateRange("20200122", "20200122")),
                            createFieldIndexHole("ZETA", "csv", dateRange("20200122", "20200122")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
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
            
            FieldIndexHoleMutationCreator mutationCreator = new FieldIndexHoleMutationCreator();
            // Index holes for NAME-wiki on 20200103 and 20200105.
            mutationCreator.addFrequencyMutations("NAME", "wiki", "20200101", "20200105", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200101", "20200102", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200103", "20200103", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200104", "20200104", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "wiki", "20200105", "20200105", 1L); // Will not meet threshold.
            // Index holes for NAME-wiki on 20200103 and 20200105.
            mutationCreator.addFrequencyMutations("NAME", "maze", "20200101", "20200105", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "maze", "20200101", "20200102", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "maze", "20200103", "20200103", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "NAME", "maze", "20200104", "20200104", 5L);
            mutationCreator.addIndexMutations(cf, "NAME", "maze", "20200105", "20200105", 1L); // Will not meet threshold.
            // Index holes for ALPHA-csv on 20200110 and 20200113.
            mutationCreator.addFrequencyMutations("ALPHA", "csv", "20200110", "20200115", 5L);
            mutationCreator.addIndexMutations(cf, "ALPHA", "csv", "20200110", "20200110", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "ALPHA", "csv", "20200111", "20200112", 5L);
            mutationCreator.addIndexMutations(cf, "ALPHA", "csv", "20200113", "20200113", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "ALPHA", "csv", "20200114", "20200115", 5L);
            // Index hole for EVENT_DATE-wiki on 20200122.
            mutationCreator.addFrequencyMutations("EVENT_DATE", "wiki", "20200120", "20200125", 5L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200120", "20200121", 5L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200122", "20200122", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "wiki", "20200123", "20200125", 5L);
            // Index hole for EVENT_DATE-maze on 20200122.
            mutationCreator.addFrequencyMutations("EVENT_DATE", "maze", "20200120", "20200125", 5L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "maze", "20200120", "20200121", 5L);
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "maze", "20200122", "20200122", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "EVENT_DATE", "maze", "20200123", "20200125", 5L);
            // Index holes for URI-maze on 20200221, 20200303, and 20200316.
            mutationCreator.addFrequencyMutations("URI", "maze", "20200216", "20200328", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200216", "20200220", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200221", "20200221", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200222", "20200302", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200303", "20200303", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200304", "20200315", 5L);
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200316", "20200316", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "URI", "maze", "20200317", "20200328", 5L);
            // Index hole for ZETA-csv on 20200122.
            mutationCreator.addFrequencyMutations("ZETA", "csv", "20200120", "20200125", 5L);
            mutationCreator.addIndexMutations(cf, "ZETA", "csv", "20200120", "20200121", 5L);
            mutationCreator.addIndexMutations(cf, "ZETA", "csv", "20200122", "20200122", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "ZETA", "csv", "20200123", "20200125", 5L);
            // Index hole for ZETA-imdb on 20200122.
            mutationCreator.addFrequencyMutations("ZETA", "imdb", "20200120", "20200125", 5L);
            mutationCreator.addIndexMutations(cf, "ZETA", "imdb", "20200120", "20200121", 5L);
            mutationCreator.addIndexMutations(cf, "ZETA", "imdb", "20200122", "20200122", 1L); // Will not meet threshold.
            mutationCreator.addIndexMutations(cf, "ZETA", "imdb", "20200123", "20200125", 5L);
            writeMutations(mutationCreator.getMutations());
            
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = getIndexHoleFunction(cf).get();
            // @formatter:off
            Map<String,Map<String,IndexFieldHole>> expected = createFieldIndexHoleMap(
                            createFieldIndexHole("NAME", "wiki", dateRange("20200103", "20200103"), dateRange("20200105", "20200105")),
                            createFieldIndexHole("ZETA", "csv", dateRange("20200122", "20200122")));
            // @formatter:on
            Assertions.assertEquals(expected, fieldIndexHoles);
        }
        
        private void givenFields(String... fields) {
            this.fields = Sets.newHashSet(fields);
        }
        
        private void givenDatatypes(String... datatypes) {
            this.datatypes = Sets.newHashSet(datatypes);
        }
        
        private void givenMinimumThreshold(double minimumThreshold) {
            this.minimumThreshold = minimumThreshold;
        }
        
        protected Map<String,Map<String,IndexFieldHole>> createFieldIndexHoleMap(IndexFieldHole... holes) {
            Map<String,Map<String,IndexFieldHole>> fieldIndexHoles = new HashMap<>();
            for (IndexFieldHole hole : holes) {
                Map<String,IndexFieldHole> datatypeMap = fieldIndexHoles.computeIfAbsent(hole.getFieldName(), k -> new HashMap<>());
                datatypeMap.put(hole.getDatatype(), hole);
            }
            return fieldIndexHoles;
        }
        
        @SafeVarargs
        protected final IndexFieldHole createFieldIndexHole(String field, String datatype, Pair<Date,Date>... dateRanges) {
            return new IndexFieldHole(field, datatype, Sets.newHashSet(dateRanges));
        }
        
        protected Pair<Date,Date> dateRange(String start, String end) {
            return Pair.of(DateHelper.parse(start), DateHelper.parse(end));
        }
    }
    
    /**
     * Helper class for creating mutations in bulk for field index hole tests.
     */
    private static class FieldIndexHoleMutationCreator {
        
        private final List<Mutation> mutations = new ArrayList<>();
        
        private void addFrequencyMutations(String fieldName, String datatype, String startDate, String endDate, long count) {
            List<String> dates = getDatesInRange(startDate, endDate);
            dates.forEach(date -> addMutation(fieldName, "f", datatype, date, count));
        }
        
        private void addIndexMutations(String cf, String fieldName, String datatype, String startDate, String endDate, long count) {
            List<String> dates = getDatesInRange(startDate, endDate);
            dates.forEach(date -> addMutation(fieldName, cf, datatype, date, count));
        }
        
        private void addIndexMarkerMutation(String cf, String fieldName, String datatype, String endDate, boolean indexed) {
            addMutation(fieldName, cf, datatype, endDate, indexed);
        }
        
        private void addIndexMarkerMutation(String cf, String fieldName, String datatype, String endDate) {
            addMutation(fieldName, cf, datatype, endDate);
        }
        
        private void addIndexMarkerMutation(String cf, String fieldName, String datatype, String endDate, Class typeClass) {
            addMutation(fieldName, cf, datatype, endDate, typeClass);
        }
        
        private List<String> getDatesInRange(String startDateStr, String endDateStr) {
            Date startDate = DateHelper.parse(startDateStr);
            Date endDate = DateHelper.parse(endDateStr);
            
            List<String> dates = new ArrayList<>();
            dates.add(startDateStr);
            
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(startDate);
            while (true) {
                calendar.add(Calendar.DAY_OF_MONTH, 1);
                Date date = calendar.getTime();
                if (date.before(endDate) || date.equals(endDate)) {
                    dates.add(DateHelper.format(date));
                } else {
                    break;
                }
            }
            
            return dates;
        }
        
        private void addMutation(String row, String columnFamily, String datatype, String date, long count) {
            Mutation mutation = new Mutation(row);
            mutation.put(columnFamily, datatype + NULL_BYTE + date, new Value(SummingCombiner.VAR_LEN_ENCODER.encode(count)));
            mutations.add(mutation);
        }
        
        private void addMutation(String row, String columnFamily, String datatype, String date, boolean indexed) {
            Mutation mutation = new Mutation(row);
            mutation.put(columnFamily, datatype + NULL_BYTE + date + NULL_BYTE + indexed, new Value());
            mutations.add(mutation);
        }
        
        private void addMutation(String row, String columnFamily, String datatype, String date) {
            Mutation mutation = new Mutation(row);
            mutation.put(columnFamily, datatype, getTimestamp(date), new Value());
            mutations.add(mutation);
        }
        
        private void addMutation(String row, String columnFamily, String datatype, String date, Class type) {
            Mutation mutation = new Mutation(row);
            mutation.put(columnFamily, datatype + NULL_BYTE + type.getName(), getTimestamp(date), new Value());
            mutations.add(mutation);
        }
        
        private long getTimestamp(String date) {
            return DateHelper.parse(date).getTime();
        }
        
        private List<Mutation> getMutations() {
            return mutations;
        }
    }
    
}
