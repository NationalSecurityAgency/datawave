package datawave.query.index.day;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Range;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.planner.QueryPlan;
import datawave.util.TableName;
import datawave.util.time.DateHelper;

class DayIndexScannerStreamTest {

    private static final Logger log = LoggerFactory.getLogger(DayIndexScannerStreamTest.class);

    private static final String DAY_INDEX_TABLE = TableName.SHARD_DAY_INDEX;

    private static DayIndexIngestUtil ingestUtil;

    private ShardQueryConfiguration sqlConfig;
    private DayIndexConfig config;
    private Map<String,QueryPlan> expected;

    private String query;
    private String startDate;
    private String endDate;

    @BeforeAll
    public static void setup() throws Exception {
        InMemoryInstance instance = new InMemoryInstance(DayIndexScannerStreamTest.class.getName());
        ingestUtil = new DayIndexIngestUtil(instance);
        ingestUtil.writeData();
    }

    @BeforeEach
    public void before() {
        query = null;
        startDate = null;
        endDate = null;
        sqlConfig = new ShardQueryConfiguration();
        config = null;
        expected = new HashMap<>();
    }

    @Test
    public void testScan_singleDay_SingleField_SingleValue() {
        setQuery("FIELD_A == 'even'");
        setDateRange("20240101", "20240101");
        addExpectedEvenShards("20240101", "FIELD_A == 'even'");
        test();
    }

    @Test
    public void testScan_singleDay_SingleField_MultiValue() {
        setQuery("FIELD_A == 'even' || FIELD_A == 'odd'");
        setDateRange("20240101", "20240101");
        addExpectedEvenShards("20240101", "FIELD_A == 'even'");
        addExpectedOddShards("20240101", "FIELD_A == 'odd'");
        test();
    }

    @Test
    public void testScan_singleDay_MultiField_SingleValue() {
        setQuery("FIELD_A == 'even' && FIELD_B == 'prime'");
        setDateRange("20240101", "20240101");
        addExpectedShards("20240101", "(FIELD_A == 'even' && FIELD_B == 'prime')", 2);
        test();
    }

    @Test
    public void testScan_singleDay_MultiField_MultiValue() {
        setQuery("FIELD_A == 'prime' && (FIELD_B == 'prime' || FIELD_B == 'odd')");
        setDateRange("20240101", "20240101");
        addExpectedShards("20240101", "(FIELD_A == 'prime' && (FIELD_B == 'prime' || FIELD_B == 'odd'))", 3, 5, 7);
        addExpectedShards("20240101", "(FIELD_A == 'prime' && FIELD_B == 'prime')", 2);
        test();
    }

    @Test
    public void testScan_multiDay_singleField_singleValue() {
        setQuery("FIELD_A == 'even'");
        setDateRange("20240101", "20240103");
        addExpectedEvenShards("20240101", "FIELD_A == 'even'");
        addExpectedEvenShards("20240102", "FIELD_A == 'even'");
        addExpectedEvenShards("20240103", "FIELD_A == 'even'");
        test();
    }

    @Test
    public void testScan_multiDay_singleField_multiValue() {
        setQuery("FIELD_A == 'even' && FIELD_A == 'prime'");
        setDateRange("20240101", "20240103");
        addExpectedShards("20240101", "(FIELD_A == 'even' && FIELD_A == 'prime')", 2);
        addExpectedShards("20240102", "(FIELD_A == 'even' && FIELD_A == 'prime')", 2);
        addExpectedShards("20240103", "(FIELD_A == 'even' && FIELD_A == 'prime')", 2);
        test();
    }

    @Test
    public void testScan_multiDay_multiField_singleValue() {
        setQuery("FIELD_A == 'even' && FIELD_B == 'even'");
        setDateRange("20240101", "20240103");
        addExpectedEvenShards("20240101", "(FIELD_A == 'even' && FIELD_B == 'even')");
        addExpectedEvenShards("20240102", "(FIELD_A == 'even' && FIELD_B == 'even')");
        addExpectedEvenShards("20240103", "(FIELD_A == 'even' && FIELD_B == 'even')");
        test();
    }

    @Test
    public void testScan_multiDay_multiField_multiValue() {
        setQuery("FIELD_A == 'even' && FIELD_B == 'prime'");
        setDateRange("20240101", "20240103");
        addExpectedShards("20240101", "(FIELD_A == 'even' && FIELD_B == 'prime')", 2);
        addExpectedShards("20240102", "(FIELD_A == 'even' && FIELD_B == 'prime')", 2);
        addExpectedShards("20240103", "(FIELD_A == 'even' && FIELD_B == 'prime')", 2);
        test();
    }

    @Test
    public void testScan_singleDay_singleField_missingValue() {
        setQuery("FIELD_A == '404'");
        setDateRange("20240101", "20240101");
        test();
    }

    @Test
    public void testScan_singleDay_missingField_singleValue() {
        setQuery("FIELD_MISSING == 'prime'");
        setDateRange("20240101", "20240101");
        // field does not intersect with indexed fields, an empty field map is sent
        // to the DayIndexEntryIterator which throws an IllegalArgumentException
        // assertThrows(IllegalArgumentException.class, this::test);
        test();
    }

    @Test
    public void testScan_missingDay_singleField_singleValue() {
        setQuery("FIELD_A == 'prime'");
        setDateRange("20230101", "20230101");
        test();
    }

    private void test() {
        assertNull(config);
        setupConfig();
        assertNotNull(config);

        try (DayIndexScannerStream stream = new DayIndexScannerStream(config)) {

            List<QueryPlan> results = new LinkedList<>();
            while (stream.hasNext()) {
                QueryPlan plan = stream.next();
                assertNotNull(plan.getQueryTree());
                results.add(plan);
                log.debug("plan: {}", plan.getRanges().iterator().next().getStartKey().toStringNoTime());
            }

            assertResults(results);
        } catch (IOException e) {
            fail(e.getCause());
            throw new RuntimeException(e);
        }
    }

    private void setupConfig() {
        assertNotNull(startDate);
        assertNotNull(endDate);
        sqlConfig.setBeginDate(DateHelper.parse(startDate));
        sqlConfig.setEndDate(DateHelper.parse(endDate));

        assertNotNull(query);
        sqlConfig.setQueryString(query);
        try {
            sqlConfig.setQueryTree(JexlASTHelper.parseAndFlattenJexlQuery(query));
        } catch (Exception e) {
            fail("Failed to parse query: " + query);
        }

        sqlConfig.setClient(ingestUtil.getClient());

        sqlConfig.setDayIndexTableName(DAY_INDEX_TABLE); // clarity through verbosity

        sqlConfig.setIndexedFields(ingestUtil.getIndexedFields());

        this.config = new DayIndexConfig(sqlConfig);

        Multimap<String,String> fieldsAndValues = IndexedTermVisitor.getIndexedFieldsAndValues(sqlConfig.getQueryTree(), sqlConfig.getIndexedFields());
        this.config.setValuesAndFields(Multimaps.invertFrom(fieldsAndValues, HashMultimap.create()));
    }

    private void assertResults(List<QueryPlan> results) {
        assertEquals(expected.size(), results.size());

        for (QueryPlan result : results) {
            String shard = shardFromRanges(result.getRanges());

            assertFalse(expected.isEmpty());
            QueryPlan expectedPlan = expected.remove(shard);
            assertShard(expectedPlan.getRanges(), result.getRanges());
            assertEquals(expectedPlan.getQueryString(), result.getQueryString());
        }
    }

    private void assertShard(Collection<Range> expected, Collection<Range> result) {
        String expectedShard = shardFromRanges(expected);
        String resultShard = shardFromRanges(result);
        assertEquals(expectedShard, resultShard);
    }

    private String shardFromRanges(Collection<Range> ranges) {
        return ranges.iterator().next().getStartKey().getRow().toString();
    }

    /**
     * Helper method that adds the specified query to all even shards for the specified day
     *
     * @param day
     *            the day
     * @param query
     *            the query string
     */
    private void addExpectedEvenShards(String day, String query) {
        addExpectedShards(day, query, 0, 2, 4, 6, 8);
    }

    /**
     * Helper method that adds the specified query to all odd shards for the specified day
     *
     * @param day
     *            the day
     * @param query
     *            the query string
     */
    private void addExpectedOddShards(String day, String query) {
        addExpectedShards(day, query, 1, 3, 5, 7, 9);
    }

    /**
     * Helper method that adds the query to all shards for the day
     *
     * @param day
     *            the day
     * @param query
     *            the query string
     */
    private void addExpectedShards(String day, String query, int... shards) {
        for (int i : shards) {
            addExpected(day + "_" + i, query);
        }
    }

    /**
     * Builds an expected query plan given a day and query string
     *
     * @param shard
     *            the shard
     * @param query
     *            the query string
     */
    private void addExpected(String shard, String query) {
        Collection<Range> range = Collections.singleton(Range.exact(shard));
        QueryPlan expectedPlan = new QueryPlan().withRanges(range).withQueryString(query);
        expected.put(shard, expectedPlan);
    }

    private void setQuery(String query) {
        this.query = query;
    }

    private void setDateRange(String startDate, String endDate) {
        this.startDate = startDate;
        this.endDate = endDate;
    }
}
