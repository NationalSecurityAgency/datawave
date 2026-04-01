package datawave.query.transformer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.log4j.Logger;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import datawave.core.query.iterator.DatawaveTransformIterator;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.QueryParameters;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper;
import datawave.query.common.grouping.AggregateOperation;
import datawave.query.common.grouping.DocumentGrouper;
import datawave.query.iterator.QueryOptions;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.TestIndexTableNames;
import datawave.query.util.VisibilityWiseGuysIngest;
import datawave.query.util.VisibilityWiseGuysIngestWithModel;
import datawave.query.util.VisibilityWiseGuysNoGroupingIngestWithModel;
import datawave.util.TableName;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.result.DefaultEventQueryResponse;

/**
 * Applies grouping to queries.
 * <p>
 * This test extensively modifies the flow of {@link AbstractQueryTest}.
 */
@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = "datawave.query")
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
public class GroupingTest extends AbstractQueryTest {

    private static class QueryResult {

        private final RebuildingScannerTestHelper.TEARDOWN teardown;
        private final RebuildingScannerTestHelper.INTERRUPT interrupt;
        private final DefaultEventQueryResponse response;

        private QueryResult(RebuildingScannerTestHelper.TEARDOWN teardown, RebuildingScannerTestHelper.INTERRUPT interrupt,
                        DefaultEventQueryResponse response) {
            this.teardown = teardown;
            this.interrupt = interrupt;
            this.response = response;
        }
    }

    private static class Group {
        private final SortedSet<String> groupValues;
        private final SortedMap<String,SortedMap<AggregateOperation,String>> aggregateValues = new TreeMap<>();
        private int count;

        public static Group of(String... values) {
            return new Group(values);
        }

        public Group() {
            this.groupValues = new TreeSet<>();
        }

        public Group(String... values) {
            this.groupValues = Sets.newTreeSet(values);
        }

        public void addGroupValue(String value) {
            this.groupValues.add(value);
        }

        public Group withCount(int count) {
            this.count = count;
            return this;
        }

        public Group withAggregate(Aggregate field) {
            this.aggregateValues.put(field.field, field.values);
            return this;
        }

        public Group withFieldSum(String field, String sum) {
            putAggregate(field, AggregateOperation.SUM, sum);
            return this;
        }

        public Group withFieldMax(String field, String max) {
            putAggregate(field, AggregateOperation.MAX, max);
            return this;
        }

        public Group withFieldMin(String field, String min) {
            putAggregate(field, AggregateOperation.MIN, min);
            return this;
        }

        public Group withFieldCount(String field, String count) {
            putAggregate(field, AggregateOperation.COUNT, count);
            return this;
        }

        public Group withFieldAverage(String field, String average) {
            putAggregate(field, AggregateOperation.AVERAGE, average);
            return this;
        }

        private void putAggregate(String field, AggregateOperation operation, String value) {
            Map<AggregateOperation,String> map = aggregateValues.computeIfAbsent(field, k -> new TreeMap<>());
            map.put(operation, value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Group group = (Group) o;
            return count == group.count && Objects.equals(groupValues, group.groupValues) && Objects.equals(aggregateValues, group.aggregateValues);
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupValues, count, aggregateValues);
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this).append("groupValues", groupValues).append("aggregateValues", aggregateValues).append("count", count).toString();
        }
    }

    private static class Aggregate {
        private final String field;
        private final SortedMap<AggregateOperation,String> values = new TreeMap<>();

        public static Aggregate of(String field) {
            return new Aggregate(field);
        }

        public Aggregate(String field) {
            this.field = field;
        }

        public Aggregate withSum(String sum) {
            values.put(AggregateOperation.SUM, sum);
            return this;
        }

        public Aggregate withAverage(String average) {
            values.put(AggregateOperation.AVERAGE, average);
            return this;
        }

        public Aggregate withCount(String count) {
            values.put(AggregateOperation.COUNT, count);
            return this;
        }

        public Aggregate withMin(String min) {
            values.put(AggregateOperation.MIN, min);
            return this;
        }

        public Aggregate withMax(String max) {
            values.put(AggregateOperation.MAX, max);
            return this;
        }
    }

    private static final String COUNT_FIELD = "COUNT";
    private static final Set<String> FIELDS_OF_INTEREST = ImmutableSet.of("GENDER", "GEN", "BIRTHDAY", "AGE", "AG", "RECORD", "BIRTH_DATE");
    private static final Logger log = Logger.getLogger(GroupingTest.class);
    private static final String COLVIS_MARKING = "columnVisibility";
    private static final String REDUCED_COLVIS = "ALL&E&I";
    private static final Authorizations auths = new Authorizations("ALL", "E", "I");
    private static final EnumSet<RebuildingScannerTestHelper.TEARDOWN> TEARDOWNS = EnumSet.allOf(RebuildingScannerTestHelper.TEARDOWN.class);
    private static final EnumSet<RebuildingScannerTestHelper.INTERRUPT> INTERRUPTS = EnumSet.allOf(RebuildingScannerTestHelper.INTERRUPT.class);
    private static final Set<Authorizations> authSet = Collections.singleton(auths);

    @Autowired
    @Qualifier("metadataHelperCacheManager")
    private CacheManager cacheManager;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
    }

    @Override
    public Authorizations getAuths() {
        return auths;
    }

    @Override
    protected void extraConfigurations() {
        // no-op
    }

    /**
     * Configure the provided {@link QueryParameters#GROUP_FIELDS}
     *
     * @param option
     *            the option value
     */
    private void givenGroupFields(String option) {
        givenParameter(QueryParameters.GROUP_FIELDS, option);
    }

    /**
     * Configure the provided {@link QueryParameters#GROUP_FIELDS_BATCH_SIZE}
     *
     * @param option
     *            the option value
     */
    private void givenGroupFieldsBatchSize(String option) {
        givenParameter(QueryParameters.GROUP_FIELDS_BATCH_SIZE, option);
    }

    @Override
    protected void extraAssertions() {
        // no-op
    }

    @Override
    public List<String> getIndexTableNames() {
        return List.of(TableName.SHARD_INDEX, TestIndexTableNames.NO_UID_INDEX);
    }

    private final Map<SortedSet<String>,Group> expectedGroups = new HashMap<>();
    private final List<QueryResult> queryResults = new ArrayList<>();

    private Consumer<AccumuloClient> dataWriter;

    @BeforeAll
    public static void beforeAll() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    }

    @BeforeEach
    public void beforeEach() throws ParseException {
        this.logic.setFullTableScanEnabled(true);
        this.logic.setMaxEvaluationPipelines(1);
        this.logic.setQueryExecutionForPageTimeout(300000000000000L);

        givenDate("20091231", "20150101");

        // every query eventually has the same query plan
        expectPlan("UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");

        // clear any cache updates from previous tests
        cacheManager.getCacheNames().forEach(name -> cacheManager.getCache(name).clear());
    }

    @AfterEach
    public void afterEach() {
        this.expectedGroups.clear();
        this.queryResults.clear();
        this.dataWriter = null;
    }

    @AfterAll
    public static void afterAll() {
        TypeRegistry.reset();
    }

    private void expectGroup(Group group) {
        expectedGroups.put(group.groupValues, group);
    }

    private void givenLuceneParserForLogic() {
        logic.setParser(new LuceneToJexlQueryParser());
    }

    private void givenNonModelData() {
        dataWriter = (client) -> {
            try {
                VisibilityWiseGuysIngest.writeItAll(client);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private void givenModelData() {
        dataWriter = (client) -> {
            try {
                VisibilityWiseGuysIngestWithModel.writeItAll(client);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private void givenNonGroupedModelData() {
        dataWriter = (client) -> {
            try {
                VisibilityWiseGuysNoGroupingIngestWithModel.writeItAll(client);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private void assertGroups() {
        for (QueryResult result : queryResults) {
            Map<SortedSet<String>,Group> actualGroups = new HashMap<>();
            // noinspection rawtypes
            for (EventBase event : result.response.getEvents()) {
                Group group = new Group();
                for (Object field : event.getFields()) {
                    FieldBase<?> fieldBase = (FieldBase<?>) field;
                    String fieldName = fieldBase.getName();
                    if (fieldName.equals(COUNT_FIELD)) {
                        group.withCount(Integer.parseInt(fieldBase.getValueString()));
                    } else if (FIELDS_OF_INTEREST.contains(fieldName)) {
                        group.addGroupValue(fieldBase.getValueString());
                    } else if (fieldName.endsWith(DocumentGrouper.FIELD_SUM_SUFFIX)) {
                        fieldName = removeSuffix(fieldName, DocumentGrouper.FIELD_SUM_SUFFIX);
                        group.withFieldSum(fieldName, fieldBase.getValueString());
                    } else if (fieldName.endsWith(DocumentGrouper.FIELD_MAX_SUFFIX)) {
                        fieldName = removeSuffix(fieldName, DocumentGrouper.FIELD_MAX_SUFFIX);
                        group.withFieldMax(fieldName, fieldBase.getValueString());
                    } else if (fieldName.endsWith(DocumentGrouper.FIELD_MIN_SUFFIX)) {
                        fieldName = removeSuffix(fieldName, DocumentGrouper.FIELD_MIN_SUFFIX);
                        group.withFieldMin(fieldName, fieldBase.getValueString());
                    } else if (fieldName.endsWith(DocumentGrouper.FIELD_COUNT_SUFFIX)) {
                        fieldName = removeSuffix(fieldName, DocumentGrouper.FIELD_COUNT_SUFFIX);
                        group.withFieldCount(fieldName, fieldBase.getValueString());
                    } else if (fieldName.endsWith(DocumentGrouper.FIELD_AVERAGE_SUFFIX)) {
                        fieldName = removeSuffix(fieldName, DocumentGrouper.FIELD_AVERAGE_SUFFIX);
                        group.withFieldAverage(fieldName, fieldBase.getValueString());
                    }
                }
                actualGroups.put(group.groupValues, group);
            }
            assertThat(actualGroups).describedAs("Assert group for teardown: %s, interrupt: %s", result.teardown, result.interrupt)
                            .containsExactlyInAnyOrderEntriesOf(expectedGroups);
        }
    }

    private String removeSuffix(String str, String suffix) {
        int suffixLength = suffix.length();
        return str.substring(0, str.length() - suffixLength);
    }

    private void assertResponseEventsAreIdenticalForAllTestResults() {
        RebuildingScannerTestHelper.TEARDOWN prevTeardown = null;
        RebuildingScannerTestHelper.INTERRUPT prevInterrupt = null;
        String prevEvents = null;

        for (QueryResult result : queryResults) {
            DefaultEventQueryResponse response = result.response;
            String events = getEventFieldNamesAndValues(response);
            if (prevEvents != null) {
                assertThat(events)
                                .describedAs("Assert events are identical between result from (teardown: %s, interrupt: %s) and (teardown: %s, interrupt: %s)",
                                                result.teardown, result.interrupt, prevTeardown, prevInterrupt)
                                .isEqualTo(prevEvents);
            }
            prevEvents = events;
            prevTeardown = result.teardown;
            prevInterrupt = result.interrupt;
        }
    }

    private String getEventFieldNamesAndValues(DefaultEventQueryResponse response) {
        // @formatter:off
        //noinspection unchecked
        return response.getEvents().stream().map((event) -> ((List<FieldBase<?>>)event.getFields()))
                        .flatMap(List::stream)
                        .map((field) -> field.getName() + ":" + field.getTypedValue().getValue())
                        .collect(Collectors.joining(","));
        // @formatter:on
    }

    private RebuildingScannerTestHelper.TEARDOWN currentTeardown = null;
    private RebuildingScannerTestHelper.INTERRUPT currentInterrupt = null;

    private void collectQueryResults() throws Exception {
        for (RebuildingScannerTestHelper.TEARDOWN teardown : TEARDOWNS) {
            for (RebuildingScannerTestHelper.INTERRUPT interrupt : INTERRUPTS) {
                currentTeardown = teardown;
                currentInterrupt = interrupt;

                AccumuloClient client = createClient(teardown, interrupt);
                setClientForTest(client);

                planAndExecuteQuery();
            }
        }
    }

    /**
     * This method is overridden because of special result handling logic.
     *
     * @param logic
     *            the query logic
     */
    @Override
    protected void executeQuery(ShardQueryLogic logic) throws Exception {
        try {
            // Run the query and retrieve the response.
            DocumentTransformer transformer = (DocumentTransformer) (logic.getTransformer(getSettings()));
            List<Object> eventList = Lists.newArrayList(new DatawaveTransformIterator<>(logic.iterator(), transformer));
            DefaultEventQueryResponse response = ((DefaultEventQueryResponse) transformer.createResponse(eventList));

            // Return the test result.
            QueryResult queryResult = new QueryResult(currentTeardown, currentInterrupt, response);
            queryResults.add(queryResult);
        } finally {
            logic.close();
        }
    }

    private AccumuloClient createClient(RebuildingScannerTestHelper.TEARDOWN teardown, RebuildingScannerTestHelper.INTERRUPT interrupt) throws Exception {
        // TODO: use normal client for this
        AccumuloClient client = new QueryTestTableHelper(getClass().toString(), log, teardown, interrupt).client;
        dataWriter.accept(client);
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, TableName.METADATA);
        return client;
    }

    @Test
    public void testGroupByAgeAndGenderWithBatchSizeOfSix() throws Exception {
        givenNonModelData();

        givenGroupFields("AGE,$GENDER");
        givenGroupFieldsBatchSize("6");
        givenQuery("UUID =~ '^[CS].*'");

        expectGroup(Group.of("FEMALE", "18").withCount(2));
        expectGroup(Group.of("MALE", "30").withCount(1));
        expectGroup(Group.of("MALE", "34").withCount(1));
        expectGroup(Group.of("MALE", "16").withCount(1));
        expectGroup(Group.of("MALE", "40").withCount(2));
        expectGroup(Group.of("MALE", "20").withCount(2));
        expectGroup(Group.of("MALE", "24").withCount(1));
        expectGroup(Group.of("MALE", "22").withCount(2));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
        assertResponseEventsAreIdenticalForAllTestResults();
    }

    /**
     * Verify grouping by age with a batch size of 6 works correctly.
     */
    @Test
    public void testGroupByAgeWithBatchSizeOfSix() throws Exception {
        givenNonModelData();

        givenGroupFields("AGE");
        givenGroupFieldsBatchSize("6");
        givenQuery("UUID =~ '^[CS].*'");

        expectGroup(Group.of("18").withCount(2));
        expectGroup(Group.of("30").withCount(1));
        expectGroup(Group.of("34").withCount(1));
        expectGroup(Group.of("16").withCount(1));
        expectGroup(Group.of("40").withCount(2));
        expectGroup(Group.of("20").withCount(2));
        expectGroup(Group.of("24").withCount(1));
        expectGroup(Group.of("22").withCount(2));

        collectQueryResults();
        assertGroups();
    }

    /**
     * Verify that grouping by gender with a batch size of 0 works correctly.
     */
    @Test
    public void testGroupByGenderWithBatchSizeOfZero() throws Exception {
        givenNonModelData();

        givenGroupFields("GENDER");
        givenGroupFieldsBatchSize("0");
        givenQuery("UUID =~ '^[CS].*'");

        expectGroup(Group.of("MALE").withCount(10));
        expectGroup(Group.of("FEMALE").withCount(2));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
    }

    /**
     * Verify grouping by gender with a batch size of 6 works correctly.
     */
    @Test
    public void testGroupByGenderWithBatchSizeOfSix() throws Exception {
        givenNonModelData();

        givenGroupFields("GENDER");
        givenGroupFieldsBatchSize("6");
        givenQuery("UUID =~ '^[CS].*'");

        expectGroup(Group.of("MALE").withCount(10));
        expectGroup(Group.of("FEMALE").withCount(2));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
    }

    /**
     * Verify that reducing the response when grouping results in the correct combined visibility and markings.
     */
    @Test
    public void testGroupByWithReducedResponse() throws Exception {
        givenNonModelData();

        givenParameter(QueryOptions.REDUCED_RESPONSE, "true");
        givenGroupFields("GENDER");
        givenGroupFieldsBatchSize("0");
        givenQuery("UUID =~ '^[CS].*'");

        expectGroup(Group.of("MALE").withCount(10));
        expectGroup(Group.of("FEMALE").withCount(2));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();

        // Verify that the column visibility was appropriately reduced
        for (QueryResult result : queryResults) {
            // noinspection rawtypes
            for (EventBase event : result.response.getEvents()) {
                String eventCV = event.getMarkings().get(COLVIS_MARKING).toString();
                assertThat(eventCV).describedAs("Assert event cv for teardown: %s, interrupt: %s", result.teardown, result.interrupt).isEqualTo(REDUCED_COLVIS);
                // noinspection unchecked
                for (FieldBase<?> field : (List<FieldBase<?>>) event.getFields()) {
                    String fieldCV = field.getMarkings().get(COLVIS_MARKING);
                    assertThat(fieldCV).describedAs("Assert null field cv for field: %s, teardown: %s, interrupt: %s", field.getName(), result.teardown,
                                    result.interrupt).isNull();
                }
            }
        }
    }

    /**
     * Verify that grouping by multivalued entries with no context works correctly.
     */
    @Test
    public void testGroupByRecord() throws Exception {
        givenNonModelData();

        givenGroupFields("RECORD");
        givenQuery("UUID =~ '^[CS].*'");

        expectGroup(Group.of("1").withCount(3));
        expectGroup(Group.of("2").withCount(3));
        expectGroup(Group.of("3").withCount(1));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
    }

    /**
     * Verify that grouping multivalued entries with no context in combination with entries that have grouping context works correctly.
     */
    @Test
    public void testGroupByGenderAndRecord() throws Exception {
        givenNonModelData();

        givenGroupFields("GENDER,RECORD");
        givenQuery("UUID =~ '^[CS].*'");

        expectGroup(Group.of("FEMALE", "1").withCount(2));
        expectGroup(Group.of("FEMALE", "2").withCount(2));
        expectGroup(Group.of("MALE", "1").withCount(10));
        expectGroup(Group.of("MALE", "2").withCount(10));
        expectGroup(Group.of("MALE", "3").withCount(4));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
    }

    /**
     * Verify that specifying group fields via a JEXL function works correctly.
     */
    @Test
    public void testGroupByJexlFunction() throws Exception {
        givenNonModelData();

        givenGroupFieldsBatchSize("6");
        givenQuery("UUID =~ '^[CS].*' && f:groupby('$AGE','GENDER')");

        expectGroup(Group.of("FEMALE", "18").withCount(2));
        expectGroup(Group.of("MALE", "30").withCount(1));
        expectGroup(Group.of("MALE", "34").withCount(1));
        expectGroup(Group.of("MALE", "16").withCount(1));
        expectGroup(Group.of("MALE", "40").withCount(2));
        expectGroup(Group.of("MALE", "20").withCount(2));
        expectGroup(Group.of("MALE", "24").withCount(1));
        expectGroup(Group.of("MALE", "22").withCount(2));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
    }

    /**
     * Verify that specifying a single field via the lucene function works correctly.
     */
    @Test
    public void testGroupBySingleField() throws Exception {
        givenNonModelData();

        givenParameter(QueryParameters.RETURN_FIELDS, "AGE");
        givenParameter(QueryParameters.HIT_LIST, "true");
        givenParameter(QueryParameters.INCLUDE_GROUPING_CONTEXT, "true");
        givenParameter(QueryParameters.LIMIT_FIELDS, "_ANYFIELD_=100");
        givenParameter(QueryOptions.REDUCED_RESPONSE, "true");
        givenLuceneParserForLogic();
        givenQuery("(UUID:C* or UUID:S* ) and #GROUPBY(AGE)");
        expectPlan("(((_Eval_ = true) && (UUID =~ 'c.*?')) || ((_Eval_ = true) && (UUID =~ 's.*?')))");

        logic.setCollectTimingDetails(true);

        expectGroup(Group.of("16").withCount(1));
        expectGroup(Group.of("18").withCount(2));
        expectGroup(Group.of("20").withCount(2));
        expectGroup(Group.of("22").withCount(2));
        expectGroup(Group.of("24").withCount(1));
        expectGroup(Group.of("30").withCount(1));
        expectGroup(Group.of("34").withCount(1));
        expectGroup(Group.of("40").withCount(2));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
    }

    /**
     * Verify that specifying group fields via a LUCENE function works correctly.
     */
    @Test
    public void testGroupByLuceneFunction() throws Exception {
        givenNonModelData();

        givenGroupFieldsBatchSize("6");
        givenLuceneParserForLogic();
        givenQuery("(UUID:C* or UUID:S* ) and #GROUPBY('AGE','$GENDER')");
        expectPlan("(((_Eval_ = true) && (UUID =~ 'c.*?')) || ((_Eval_ = true) && (UUID =~ 's.*?')))");

        expectGroup(Group.of("FEMALE", "18").withCount(2));
        expectGroup(Group.of("MALE", "30").withCount(1));
        expectGroup(Group.of("MALE", "34").withCount(1));
        expectGroup(Group.of("MALE", "16").withCount(1));
        expectGroup(Group.of("MALE", "40").withCount(2));
        expectGroup(Group.of("MALE", "20").withCount(2));
        expectGroup(Group.of("MALE", "24").withCount(1));
        expectGroup(Group.of("MALE", "22").withCount(2));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
    }

    /**
     * Verify that specifying group fields via a LUCENE function with two values works correctly.
     */
    @Test
    public void testGroupByLuceneFunctionWithDuplicateValues() throws Exception {
        givenNonModelData();

        givenGroupFieldsBatchSize("6");
        givenLuceneParserForLogic();
        givenQuery("(UUID:CORLEONE) and #GROUPBY('AGE','BIRTHDAY')");
        expectPlan("UUID == 'corleone'");

        expectGroup(Group.of("4", "18").withCount(1));
        expectGroup(Group.of("5", "40").withCount(1));
        expectGroup(Group.of("3", "20").withCount(1));
        expectGroup(Group.of("1", "24").withCount(1));
        expectGroup(Group.of("2", "22").withCount(1));
        expectGroup(Group.of("22", "22").withCount(1));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
    }

    @Test
    public void testGroupingByGenderAndAllAgeMetrics() throws Exception {
        givenNonModelData();

        givenParameter(QueryParameters.MAX_FIELDS, "AGE");
        givenParameter(QueryParameters.MIN_FIELDS, "AGE");
        givenParameter(QueryParameters.SUM_FIELDS, "AGE");
        givenParameter(QueryParameters.AVERAGE_FIELDS, "AGE");
        givenParameter(QueryParameters.COUNT_FIELDS, "AGE");
        givenGroupFields("GENDER");
        givenGroupFieldsBatchSize("6");
        givenQuery("UUID =~ '^[CS].*'");

        expectGroup(Group.of("MALE").withCount(10)
                        .withAggregate(Aggregate.of("AGE").withCount("10").withMax("40").withMin("16").withSum("268").withAverage("26.8")));
        expectGroup(Group.of("FEMALE").withCount(2)
                        .withAggregate(Aggregate.of("AGE").withCount("2").withMax("18").withMin("18").withSum("36").withAverage("18")));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
    }

    @Test
    public void testGroupingByGenderAndAllAgeMetricsUsingJexlFunction() throws Exception {
        givenNonModelData();

        givenQuery("UUID =~ '^[CS].*' && f:groupby('$GENDER') && f:sum('AGE') && f:min('AGE') && f:max('AGE') && f:average('AGE') && f:count('AGE')");

        expectGroup(Group.of("MALE").withCount(10)
                        .withAggregate(Aggregate.of("AGE").withCount("10").withMax("40").withMin("16").withSum("268").withAverage("26.8")));
        expectGroup(Group.of("FEMALE").withCount(2)
                        .withAggregate(Aggregate.of("AGE").withCount("2").withMax("18").withMin("18").withSum("36").withAverage("18")));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
    }

    @Test
    public void testGroupingByGenderAndAllAgeMetricsUsingLuceneFunction() throws Exception {
        givenNonModelData();

        givenLuceneParserForLogic();
        givenQuery("(UUID:C* or UUID:S* ) and #GROUPBY('$GENDER') and #SUM('AGE') and #MAX('AGE') and #MIN('AGE') and #AVERAGE('AGE') and #COUNT('AGE')");
        expectPlan("(((_Eval_ = true) && (UUID =~ 'c.*?')) || ((_Eval_ = true) && (UUID =~ 's.*?')))");

        expectGroup(Group.of("MALE").withCount(10)
                        .withAggregate(Aggregate.of("AGE").withCount("10").withMax("40").withMin("16").withSum("268").withAverage("26.8")));
        expectGroup(Group.of("FEMALE").withCount(2)
                        .withAggregate(Aggregate.of("AGE").withCount("2").withMax("18").withMin("18").withSum("36").withAverage("18")));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
    }

    @Test
    public void testGroupByAgeAndGenderWithBatchSizeOfSixUsingModel() throws Exception {
        givenModelData();

        givenGroupFields("AG,GEN");
        givenGroupFieldsBatchSize("6");
        givenQuery("UUID =~ '^[CS].*'");

        expectGroup(Group.of("FEMALE", "18").withCount(2));
        expectGroup(Group.of("MALE", "30").withCount(1));
        expectGroup(Group.of("MALE", "34").withCount(1));
        expectGroup(Group.of("MALE", "16").withCount(1));
        expectGroup(Group.of("MALE", "40").withCount(2));
        expectGroup(Group.of("MALE", "20").withCount(2));
        expectGroup(Group.of("MALE", "24").withCount(1));
        expectGroup(Group.of("MALE", "22").withCount(2));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
        assertResponseEventsAreIdenticalForAllTestResults();
    }

    @Test
    public void testGroupByAgeWithBatchSizeOfSixUsingModel() throws Exception {
        // Set up the test.
        givenModelData();

        givenGroupFields("AG");
        givenGroupFieldsBatchSize("6");
        givenQuery("UUID =~ '^[CS].*'");

        expectGroup(Group.of("18").withCount(2));
        expectGroup(Group.of("30").withCount(1));
        expectGroup(Group.of("34").withCount(1));
        expectGroup(Group.of("16").withCount(1));
        expectGroup(Group.of("40").withCount(2));
        expectGroup(Group.of("20").withCount(2));
        expectGroup(Group.of("24").withCount(1));
        expectGroup(Group.of("22").withCount(2));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
    }

    @Test
    public void testGroupByGenderWithBatchSizeOfSixUsingModel() throws Exception {
        givenModelData();

        givenGroupFields("GEN");
        givenGroupFieldsBatchSize("6");
        givenQuery("UUID =~ '^[CS].*'");

        expectGroup(Group.of("MALE").withCount(10));
        expectGroup(Group.of("FEMALE").withCount(2));

        collectQueryResults();
        assertGroups();
    }

    @Test
    public void testGroupByGenderWithBatchSizeOfZeroUsingModel() throws Exception {
        givenModelData();

        givenGroupFields("GEN");
        givenGroupFieldsBatchSize("0");
        givenQuery("UUID =~ '^[CS].*'");

        expectGroup(Group.of("MALE").withCount(10));
        expectGroup(Group.of("FEMALE").withCount(2));

        collectQueryResults();
        assertGroups();
    }

    @Test
    public void testGroupByJexlFunctionsUsingModel() throws Exception {
        givenModelData();

        givenGroupFieldsBatchSize("6");
        givenQuery("UUID =~ '^[CS].*' && f:groupby('AG','GEN')");

        expectGroup(Group.of("FEMALE", "18").withCount(2));
        expectGroup(Group.of("MALE", "30").withCount(1));
        expectGroup(Group.of("MALE", "34").withCount(1));
        expectGroup(Group.of("MALE", "16").withCount(1));
        expectGroup(Group.of("MALE", "40").withCount(2));
        expectGroup(Group.of("MALE", "20").withCount(2));
        expectGroup(Group.of("MALE", "24").withCount(1));
        expectGroup(Group.of("MALE", "22").withCount(2));

        collectQueryResults();
        assertGroups();
    }

    @Test
    public void testGroupByLuceneFunctionUsingModel() throws Exception {
        givenModelData();

        givenGroupFieldsBatchSize("6");
        givenLuceneParserForLogic();
        givenQuery("(UUID:C* or UUID:S* ) and #GROUPBY('AG','GEN')");
        expectPlan("(((_Eval_ = true) && (UUID =~ 'c.*?')) || ((_Eval_ = true) && (UUID =~ 's.*?')))");

        expectGroup(Group.of("FEMALE", "18").withCount(2));
        expectGroup(Group.of("MALE", "30").withCount(1));
        expectGroup(Group.of("MALE", "34").withCount(1));
        expectGroup(Group.of("MALE", "16").withCount(1));
        expectGroup(Group.of("MALE", "40").withCount(2));
        expectGroup(Group.of("MALE", "20").withCount(2));
        expectGroup(Group.of("MALE", "24").withCount(1));
        expectGroup(Group.of("MALE", "22").withCount(2));

        collectQueryResults();
        assertGroups();
    }

    @Test
    public void testGroupingByGenderAndAllAgeMetricsUsingModel() throws Exception {
        givenModelData();

        givenGroupFields("GEN");
        givenGroupFieldsBatchSize("6");
        givenParameter(QueryParameters.MAX_FIELDS, "AG");
        givenParameter(QueryParameters.MIN_FIELDS, "AG");
        givenParameter(QueryParameters.SUM_FIELDS, "AG");
        givenParameter(QueryParameters.AVERAGE_FIELDS, "AG");
        givenParameter(QueryParameters.COUNT_FIELDS, "AG");

        givenQuery("UUID =~ '^[CS].*'");

        expectGroup(Group.of("MALE").withCount(10)
                        .withAggregate(Aggregate.of("AG").withCount("10").withMax("40").withMin("16").withSum("268").withAverage("26.8")));
        expectGroup(Group.of("FEMALE").withCount(2)
                        .withAggregate(Aggregate.of("AG").withCount("2").withMax("18").withMin("18").withSum("36").withAverage("18")));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
    }

    /**
     * Verify that aggregating values when grouping by multivalued entries with no context works correctly.
     */
    @Test
    public void testGroupByRecordWithAggregation() throws Exception {
        givenNonModelData();

        givenGroupFields("RECORD");
        givenQuery("UUID =~ '^[CS].*' && f:sum('AGE') && f:min('GENDER') && f:max('GENDER') && f:average('BIRTHDAY') && f:count('GENDER', 'AGE', 'BIRTHDAY')");

        // @formatter:off
        expectGroup(Group.of("1").withCount(3)
                        .withAggregate(Aggregate.of("AGE").withSum("304").withCount("12"))
                        .withAggregate(Aggregate.of("BIRTHDAY").withAverage("6.166666667").withCount("6"))
                        .withAggregate(Aggregate.of("GENDER").withMin("FEMALE").withMax("MALE").withCount("12")));
        expectGroup(Group.of("2").withCount(3)
                        .withAggregate(Aggregate.of("AGE").withSum("304").withCount("12"))
                        .withAggregate(Aggregate.of("BIRTHDAY").withAverage("6.166666667").withCount("6"))
                        .withAggregate(Aggregate.of("GENDER").withMin("FEMALE").withMax("MALE").withCount("12")));
        expectGroup(Group.of("3").withCount(1)
                        .withAggregate(Aggregate.of("AGE").withSum("124").withCount("4"))
                        .withAggregate(Aggregate.of("BIRTHDAY").withCount("0"))
                        .withAggregate(Aggregate.of("GENDER").withMin("MALE").withMax("MALE").withCount("4")));
        // @formatter:on
        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
    }

    /**
     * Verify that attempting to sum a non-numerical value results in an exception.
     */
    @Test
    public void testSummingNonNumericalValue() {
        givenNonModelData();

        givenGroupFields("RECORD");
        givenQuery("UUID =~ '^[CS].*' && f:sum('GENDER')");

        assertThatIllegalArgumentException().isThrownBy(this::collectQueryResults).withMessage("Unable to calculate a sum with non-numerical value 'MALE'");
    }

    /**
     * Verify that attempting to average a non-numerical value results in an exception.
     */
    @Test
    public void testAveragingNonNumericalValue() {
        givenNonModelData();

        givenGroupFields("RECORD");
        givenQuery("UUID =~ '^[CS].*' && f:average('GENDER')");

        assertThatIllegalArgumentException().isThrownBy(this::collectQueryResults)
                        .withMessage("Character M is neither a decimal digit number, decimal point, nor \"e\" notation exponential mark.");
    }

    @Test
    public void testGroupingWithModelByGenderAndAllAgeMetricsUsingLuceneFunction() throws Exception {
        givenModelData();

        givenLuceneParserForLogic();

        givenQuery("(UUID:C* or UUID:S* ) and #GROUPBY('GEN') and #SUM('AG') and #MAX('AG') and #MIN('AG') and #AVERAGE('AG') and #COUNT('AG')");
        expectPlan("(((_Eval_ = true) && (UUID =~ 'c.*?')) || ((_Eval_ = true) && (UUID =~ 's.*?')))");

        expectGroup(Group.of("MALE").withCount(10)
                        .withAggregate(Aggregate.of("AG").withCount("10").withMax("40").withMin("16").withSum("268").withAverage("26.8")));
        expectGroup(Group.of("FEMALE").withCount(2)
                        .withAggregate(Aggregate.of("AG").withCount("2").withMax("18").withMin("18").withSum("36").withAverage("18")));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
    }

    /**
     * Verify that when grouping and aggregating with model mapping, if multiple fields with the same value are mapped to the same root model mapping in a
     * document, that only one instance of the field-value pairing is counted towards grouping and aggregation.
     */
    @Test
    public void testFilteringOutDuplicateDatumAfterModelMapping() throws Exception {
        // Contains entries with identical values for fields that will be mapped to the same model mapping.
        givenNonGroupedModelData();

        givenLuceneParserForLogic();
        givenQuery("(UUID:C* or UUID:S* ) and #GROUPBY('GEN') and #SUM('AG') and #MAX('AG') and #MIN('AG') and #AVERAGE('AG') and #COUNT('AG')");
        expectPlan("(((_Eval_ = true) && (UUID =~ 'c.*?')) || ((_Eval_ = true) && (UUID =~ 's.*?')))");

        expectGroup(Group.of("MALE").withCount(2).withAggregate(Aggregate.of("AG").withCount("2").withMax("40").withMin("24").withSum("64").withAverage("32")));
        expectGroup(Group.of("FEMALE").withCount(1)
                        .withAggregate(Aggregate.of("AG").withCount("1").withMax("18").withMin("18").withSum("18").withAverage("18")));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
    }

    /**
     * Verify that when specifying that a field should be truncated to year when grouping via a Lucene function, that the correct grouping is performed.
     */
    @Test
    public void testGroupingWhileTruncatingToYearViaLucene() throws Exception {
        givenNonModelData();

        givenGroupFieldsBatchSize("6");
        givenLuceneParserForLogic();
        givenQuery("(UUID:CORLEONE) and #GROUPBY('GENDER','BIRTH_DATE[YEAR]')");
        expectPlan("UUID == 'corleone'");

        expectGroup(Group.of("1925-00-00T00:00:00.000", "FEMALE").withCount(1));
        expectGroup(Group.of("1925-00-00T00:00:00.000", "MALE").withCount(1));
        expectGroup(Group.of("1910-00-00T00:00:00.000", "MALE").withCount(3));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
    }

    /**
     * Verify that when specifying that a field should be truncated to year when grouping via a Jexl function, that the correct grouping is performed.
     */
    @Test
    public void testGroupingWhileTruncatingToYearViaJexl() throws Exception {
        givenNonModelData();

        givenGroupFieldsBatchSize("6");
        givenQuery("UUID =~ 'CORLEONE' && f:groupby('GENDER','BIRTH_DATE[YEAR]')");
        expectPlan("UUID == 'corleone'");

        expectGroup(Group.of("1925-00-00T00:00:00.000", "FEMALE").withCount(1));
        expectGroup(Group.of("1925-00-00T00:00:00.000", "MALE").withCount(1));
        expectGroup(Group.of("1910-00-00T00:00:00.000", "MALE").withCount(3));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
    }

    /**
     * Verify that when specifying that a field should be truncated to year when grouping via a query parameter, that the correct grouping is performed.
     */
    @Test
    public void testGroupingWhileTruncatingToYearViaQueryParameter() throws Exception {
        givenNonModelData();

        givenGroupFields("GENDER,BIRTH_DATE[YEAR]");
        givenGroupFieldsBatchSize("6");
        givenQuery("UUID =~ 'CORLEONE'");
        expectPlan("UUID == 'corleone'");

        expectGroup(Group.of("1925-00-00T00:00:00.000", "FEMALE").withCount(1));
        expectGroup(Group.of("1925-00-00T00:00:00.000", "MALE").withCount(1));
        expectGroup(Group.of("1910-00-00T00:00:00.000", "MALE").withCount(3));

        // Run the test queries and collect their results.
        collectQueryResults();
        assertGroups();
    }
}
