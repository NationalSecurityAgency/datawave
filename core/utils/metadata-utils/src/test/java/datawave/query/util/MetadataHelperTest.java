package datawave.query.util;

import static datawave.data.ColumnFamilyConstants.COLF_F;
import static datawave.query.util.TestUtils.createDateFrequencyMap;
import static org.apache.accumulo.core.iterators.LongCombiner.VAR_LEN_ENCODER;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.iterators.FrequencyMetadataAggregator;
import datawave.query.composite.CompositeMetadataHelper;
import datawave.query.model.DateFrequencyMap;
import datawave.util.time.DateHelper;
import datawave.webservice.common.connection.WrappedAccumuloClient;

public class MetadataHelperTest {
    
    private static final String TABLE_METADATA = "metadata";
    private static final String[] AUTHS = {"FOO"};
    private static final Set<Authorizations> AUTHORIZATIONS = Collections.singleton(new Authorizations(AUTHS));
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
    
    @BeforeEach
    public void setup() throws AccumuloException, TableExistsException, AccumuloSecurityException {
        accumuloClient = new InMemoryAccumuloClient("root", new InMemoryInstance(MetadataHelperTest.class.toString()));
        if (!accumuloClient.tableOperations().exists(TABLE_METADATA)) {
            accumuloClient.tableOperations().create(TABLE_METADATA);
        }
        
        helper = new MetadataHelper(createAllFieldMetadataHelper(), Collections.emptySet(), accumuloClient, TABLE_METADATA, AUTHORIZATIONS,
                        Collections.emptySet());
    }
    
    private AllFieldMetadataHelper createAllFieldMetadataHelper() {
        final Set<Authorizations> allMetadataAuths = Collections.emptySet();
        TypeMetadataHelper tmh = new TypeMetadataHelper(Maps.newHashMap(), allMetadataAuths, accumuloClient, TABLE_METADATA, AUTHORIZATIONS, false);
        CompositeMetadataHelper cmh = new CompositeMetadataHelper(accumuloClient, TABLE_METADATA, AUTHORIZATIONS);
        return new AllFieldMetadataHelper(tmh, cmh, accumuloClient, TABLE_METADATA, AUTHORIZATIONS, allMetadataAuths);
    }
    
    @AfterEach
    void tearDown() throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
        accumuloClient.tableOperations().delete(TABLE_METADATA);
        this.mutations.clear();
    }
    
    /**
     * Write the given mutations to the metadata table.
     */
    private void writeMutations() {
        TestUtils.writeMutations(accumuloClient, TABLE_METADATA, mutations);
    }
    
    private void givenMutation(Mutation mutation) {
        this.mutations.add(mutation);
    }
    
    private void givenMutation(String row, String columnFamily, String columnQualifier, Value value) {
        Mutation mutation = new Mutation(row);
        mutation.put(columnFamily, columnQualifier, value);
        givenMutation(mutation);
    }
    
    private void givenNonAggregatedFrequencyRows(String row, Text colf, String datatype, String startDate, String endDate, long count) {
        Mutation mutation = new Mutation(row);
        Value value = new Value(VAR_LEN_ENCODER.encode(count));
        List<String> dates = TestUtils.getDatesInRange(startDate, endDate);
        dates.forEach((date) -> mutation.put(colf, new Text(datatype + NULL_BYTE + date), value));
        givenMutation(mutation);
    }
    
    private void givenAggregatedFrequencyRow(String row, Text colf, String datatype, DateFrequencyMap map) {
        Mutation mutation = new Mutation(row);
        Value value = new Value(WritableUtils.toByteArray(map));
        mutation.put(colf, new Text(datatype + NULL_BYTE + FrequencyMetadataAggregator.AGGREGATED), value);
        givenMutation(mutation);
    }
    
    /**
     * Tests for {@link MetadataHelper#getAllFields(Set)}.
     */
    @Nested
    public class GetAllFieldsTest {
        @Test
        public void testSingleFieldFilter() throws TableNotFoundException {
            givenMutation("rowA", "t", "dataTypeA", new Value("value"));
            
            writeMutations();
            
            Assertions.assertEquals(Collections.singleton("rowA"), helper.getAllFields(Collections.singleton("dataTypeA")));
            Assertions.assertEquals(Collections.singleton("rowA"), helper.getAllFields(null));
            Assertions.assertEquals(Collections.singleton("rowA"), helper.getAllFields(Collections.emptySet()));
        }
        
        @Test
        public void testMultipleFieldFilter() throws TableNotFoundException {
            givenMutation("rowA", "t", "dataTypeA", new Value("value"));
            givenMutation("rowB", "t", "dataTypeB", new Value("value"));
            
            writeMutations();
            
            Assertions.assertEquals(Collections.singleton("rowB"), helper.getAllFields(Collections.singleton("dataTypeB")));
            Assertions.assertEquals(Sets.newHashSet("rowA", "rowB"), helper.getAllFields(null));
            Assertions.assertEquals(Sets.newHashSet("rowA", "rowB"), helper.getAllFields(Collections.emptySet()));
        }
        
        @Test
        public void testMultipleFieldFilter2() throws TableNotFoundException {
            givenMutation("rowA", "t", "dataTypeA", new Value("value"));
            givenMutation("rowB", "t", "dataTypeB", new Value("value"));
            givenMutation("rowC", "t", "dataTypeC", new Value("value"));
            
            writeMutations();
            
            Assertions.assertEquals(Collections.singleton("rowB"), helper.getAllFields(Collections.singleton("dataTypeB")));
            Assertions.assertEquals(Sets.newHashSet("rowA", "rowB", "rowC"), helper.getAllFields(null));
            Assertions.assertEquals(Sets.newHashSet("rowA", "rowB", "rowC"), helper.getAllFields(Collections.emptySet()));
        }
    }
    
    /**
     * Tests for {@link MetadataHelper#getCardinalityForField(String, Date, Date)} and
     * {@link MetadataHelper#getCardinalityForField(String, String, Date, Date)}.
     */
    @Nested
    public class GetCardinalityForFieldTests {
        
        /**
         * Test against a table that has only non-aggregated entries as matches.
         */
        @Test
        void testNonAggregatedEntriesOnly() throws TableNotFoundException {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200111", "20200120", 1L); // 5 entries within date range.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200120", 1L); // 12 entries within date range.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "maze", "20200101", "20200110", 1L); // 7 entries within date range.
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "data", "20200101", "20200102", 1L); // No entries within date range.
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "csv", "20200101", "20200120", 1L); // Field does not match.
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "wiki", "20200101", "20200120", 1L); // Field does not match.
            
            writeMutations();
            
            Assertions.assertEquals(24L, helper.getCardinalityForField("NAME", DateHelper.parse("20200104"), DateHelper.parse("20200115")));
            Assertions.assertEquals(12L, helper.getCardinalityForField("NAME", "wiki", DateHelper.parse("20200104"), DateHelper.parse("20200115")));
        }
        
        /**
         * Test against a table that has only aggregated entries as matches.
         */
        @Test
        void testAggregatedEntriesOnly() throws TableNotFoundException {
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createDateFrequencyMap("20200101", 1L, "20200102", 5L, "20200103", 3L, "20200104", 3L,
                            "20200105", 3L, "20200106", 3L, "20200107", 3L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createDateFrequencyMap("20200101", 1L, "20200102", 15L, "20200103", 3L, "20200107", 3L,
                            "20200108", 3L, "20200111", 3L, "20200113", 3L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "maze", createDateFrequencyMap("20200101", 1L, "20200102", 55L, "20200103", 3L, "20200111", 3L,
                            "20200114", 3L, "20200115", 3L, "20200116", 3L, "20200120", 3L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "data", createDateFrequencyMap("20200101", 1L, "20200102", 55L, "20200103", 3L, "20200120", 3L)); // Does
                                                                                                                                                          // not
                                                                                                                                                          // contain
                                                                                                                                                          // target
                                                                                                                                                          // date.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "csv", createDateFrequencyMap("20200101", 2L, "20200102", 3L, "20200103", 4L)); // Field does not
                                                                                                                                              // match.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "maze", createDateFrequencyMap("20200101", 2L, "20200102", 3L, "20200103", 4L)); // Field does not
                                                                                                                                               // match.
            writeMutations();
            
            Assertions.assertEquals(33L, helper.getCardinalityForField("NAME", DateHelper.parse("20200104"), DateHelper.parse("20200115")));
            Assertions.assertEquals(12L, helper.getCardinalityForField("NAME", "wiki", DateHelper.parse("20200104"), DateHelper.parse("20200115")));
        }
        
        /**
         * Test against a table that has both aggregated and non-aggregated entries as matches.
         */
        @Test
        void testMixedEntryFormats() throws TableNotFoundException {
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createDateFrequencyMap("20200101", 1L, "20200102", 5L, "20200103", 3L, "20200104", 3L,
                            "20200105", 3L, "20200106", 3L, "20200107", 3L));
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200101", "20200120", 1L);
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createDateFrequencyMap("20200101", 1L, "20200102", 15L, "20200103", 3L, "20200107", 3L,
                            "20200108", 3L, "20200111", 3L, "20200113", 3L));
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200113", "20200120", 3L);
            givenAggregatedFrequencyRow("NAME", COLF_F, "maze", createDateFrequencyMap("20200101", 1L, "20200102", 55L, "20200103", 3L, "20200120", 3L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "data", createDateFrequencyMap("20200101", 1L, "20200103", 3L));
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "data", "20200103", "20200105", 3L);
            // Following does not match field.
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "csv", createDateFrequencyMap("20200101", 2L, "20200102", 3L, "20200103", 4L));
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createDateFrequencyMap("20200101", 2L, "20200102", 3L, "20200103", 4L));
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "maze", createDateFrequencyMap("20200101", 2L, "20200102", 3L, "20200103", 4L));
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "csv", "20200101", "20200120", 4L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "wiki", "20200101", "20200120", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "maze", "20200101", "20200120", 6L);
            writeMutations();
            
            Assertions.assertEquals(51L, helper.getCardinalityForField("NAME", DateHelper.parse("20200104"), DateHelper.parse("20200115")));
            Assertions.assertEquals(21L, helper.getCardinalityForField("NAME", "wiki", DateHelper.parse("20200104"), DateHelper.parse("20200115")));
        }
    }
    
    /**
     * Tests for {@link MetadataHelper#getCountsByFieldInDayWithTypes(String, String, AccumuloClient, WrappedAccumuloClient)} (Map.Entry)}.
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
            
            HashMap<String,Long> actual = helper.getCountsByFieldInDayWithTypes("NAME", "20200110", accumuloClient, null);
            
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
            
            HashMap<String,Long> actual = helper.getCountsByFieldInDayWithTypes("NAME", "20200102", accumuloClient, null);
            
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
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "maze", "20200115", "20200120", 3L); // Does not have entry for 20200102, should not get
                                                                                                 // incremented.
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
            
            HashMap<String,Long> actual = helper.getCountsByFieldInDayWithTypes("NAME", "20200102", accumuloClient, null);
            
            Assertions.assertEquals(expected, actual);
        }
    }
    
    /**
     * Tests for {@link MetadataHelper#getEarliestOccurrenceOfFieldWithType(String, String, AccumuloClient, WrappedAccumuloClient)}.
     */
    @Nested
    public class GetEarliestOccurrenceOfFieldWithTypeTests {
        
        /**
         * Test against a table that has only non-aggregated entries as matches.
         */
        @Test
        void testNonAggregatedEntriesOnly() {
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200103", "20200120", 1L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "wiki", "20200101", "20200120", 2L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "maze", "20200105", "20200120", 3L);
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "data", "20200107", "20200102", 3L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "csv", "20200101", "20200120", 4L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "wiki", "20200101", "20200120", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "maze", "20200101", "20200120", 6L);
            writeMutations();
            
            Assertions.assertEquals(DateHelper.parse("20200101"), helper.getEarliestOccurrenceOfFieldWithType("NAME", null, accumuloClient, null));
            Assertions.assertEquals(DateHelper.parse("20200105"), helper.getEarliestOccurrenceOfFieldWithType("NAME", "maze", accumuloClient, null));
        }
        
        /**
         * Test against a table that has only aggregated entries as matches.
         */
        @Test
        void testAggregatedEntriesOnly() {
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createDateFrequencyMap("20200113", 1L, "20200115", 5L, "20200116", 3L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createDateFrequencyMap("20200111", 1L, "20200112", 15L, "20200113", 3L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "maze", createDateFrequencyMap("20200102", 1L, "20200104", 55L, "20200105", 3L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "data", createDateFrequencyMap("20200101", 1L, "20200103", 3L));
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "csv", createDateFrequencyMap("20200101", 2L, "20200102", 3L, "20200103", 4L));
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createDateFrequencyMap("20200101", 2L, "20200102", 3L, "20200103", 4L));
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "maze", createDateFrequencyMap("20200101", 2L, "20200102", 3L, "20200103", 4L));
            writeMutations();
            
            Assertions.assertEquals(DateHelper.parse("20200101"), helper.getEarliestOccurrenceOfFieldWithType("NAME", null, accumuloClient, null));
            Assertions.assertEquals(DateHelper.parse("20200102"), helper.getEarliestOccurrenceOfFieldWithType("NAME", "maze", accumuloClient, null));
        }
        
        /**
         * Test against a table that has both aggregated and non-aggregated entries as matches.
         */
        @Test
        void testMixedEntryFormats() {
            givenAggregatedFrequencyRow("NAME", COLF_F, "csv", createDateFrequencyMap("20200111", 1L, "20200112", 5L, "20200113", 3L));
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "csv", "20200111", "20200120", 1L);
            givenAggregatedFrequencyRow("NAME", COLF_F, "wiki", createDateFrequencyMap("20200111", 1L, "20200112", 15L, "20200113", 3L));
            givenAggregatedFrequencyRow("NAME", COLF_F, "maze", createDateFrequencyMap("20200111", 1L, "20200112", 55L, "20200113", 3L));
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "maze", "20200103", "20200120", 3L);
            givenAggregatedFrequencyRow("NAME", COLF_F, "data", createDateFrequencyMap("20200111", 1L, "20200113", 3L));
            givenNonAggregatedFrequencyRows("NAME", COLF_F, "data", "20200101", "20200115", 3L);
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "csv", createDateFrequencyMap("20200101", 2L, "20200102", 3L, "20200103", 4L));
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "wiki", createDateFrequencyMap("20200101", 2L, "20200102", 3L, "20200103", 4L));
            givenAggregatedFrequencyRow("EVENT_DATE", COLF_F, "maze", createDateFrequencyMap("20200101", 2L, "20200102", 3L, "20200103", 4L));
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "csv", "20200101", "20200120", 4L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "wiki", "20200101", "20200120", 5L);
            givenNonAggregatedFrequencyRows("EVENT_DATE", COLF_F, "maze", "20200101", "20200120", 6L);
            writeMutations();
            
            Assertions.assertEquals(DateHelper.parse("20200101"), helper.getEarliestOccurrenceOfFieldWithType("NAME", null, accumuloClient, null));
            Assertions.assertEquals(DateHelper.parse("20200103"), helper.getEarliestOccurrenceOfFieldWithType("NAME", "maze", accumuloClient, null));
        }
    }
}
