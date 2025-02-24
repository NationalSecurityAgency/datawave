package datawave.query.planner;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.StringJoiner;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ComparisonFailure;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Lists;

import datawave.configuration.spring.SpringBean;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.iterator.DatawaveTransformIterator;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryTestTableHelper;
import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.PushdownUnindexedFieldsVisitor;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.transformer.DocumentTransformer;
import datawave.query.util.IndexFieldHoleDataIngest;
import datawave.query.util.MetadataHelper;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.DefaultEventQueryResponse;

/**
 * Tests usage of {@link DatePartitionedQueryPlanner} in queries.
 */
public abstract class DatePartitionedQueryPlannerTest {

    private static final Logger log = Logger.getLogger(DatePartitionedQueryPlannerTest.class);

    @RunWith(Arquillian.class)
    public static class ShardRange extends DatePartitionedQueryPlannerTest {

        @Override
        protected IndexFieldHoleDataIngest.Range getRange() {
            return IndexFieldHoleDataIngest.Range.SHARD;
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRange extends DatePartitionedQueryPlannerTest {

        @Override
        protected IndexFieldHoleDataIngest.Range getRange() {
            return IndexFieldHoleDataIngest.Range.DOCUMENT;
        }
    }

    private static class Event {
        String date;
        String uid;

        public Event(String date, String uid) {
            this.date = date;
            this.uid = uid;
        }

        public String getDate() {
            return date;
        }

        public String getUid() {
            return uid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Event event = (Event) o;
            return Objects.equals(date, event.date) && Objects.equals(uid, event.uid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(date, uid);
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Event.class.getSimpleName() + "[", "]").add("date='" + date + "'").add("uid='" + uid + "'").toString();
        }
    }

    protected abstract IndexFieldHoleDataIngest.Range getRange();

    private static final Authorizations auths = new Authorizations("ALL", "E", "I");
    private static final Set<Authorizations> authSet = Collections.singleton(auths);

    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;
    protected KryoDocumentDeserializer deserializer;

    private final DateFormat formatDate = new SimpleDateFormat("yyyyMMdd");
    private final DateFormat formatDateTime = new SimpleDateFormat("yyyyMMdd HHmmss");
    private final DateFormat formatDateTimeMillis = new SimpleDateFormat("yyyyMMdd HHmmss.SSS");
    private final List<IndexFieldHoleDataIngest.EventConfig> eventConfigs = new ArrayList<>();
    private final Map<String,String> queryParameters = new HashMap<>();
    private final Set<Event> expectedEvents = new HashSet<>();
    private final Map<Pair<Date,Date>,Pair<String,String>> expectedPlans = new HashMap<>();

    private String query;
    private Date startDate;
    private Date endDate;
    private String plan;
    private Double fieldIndexHoleMinThreshold;

    @Deployment
    public static JavaArchive createDeployment() {
        return ShrinkWrap.create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class).deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .addAsManifestResource(new StringAsset(
                                        "<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" + "</alternatives>"),
                                        "beans.xml");
    }

    @BeforeClass
    public static void beforeClass() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    }

    @Before
    public void setup() throws Exception {
        // change to debug to see planning
        Logger.getLogger(DefaultQueryPlanner.class).setLevel(Level.WARN);
        this.logic.setFullTableScanEnabled(true);
        this.logic.setMaxEvaluationPipelines(1);
        this.logic.setMaxDepthThreshold(100);
        this.logic.setQueryExecutionForPageTimeout(300000000000000L);
        this.logic.setQueryPlanner(new DatePartitionedQueryPlanner());
        this.deserializer = new KryoDocumentDeserializer();
    }

    @After
    public void tearDown() {
        this.eventConfigs.clear();
        this.queryParameters.clear();
        this.expectedEvents.clear();
        this.expectedPlans.clear();
        this.query = null;
        this.startDate = null;
        this.endDate = null;
        this.plan = null;
        this.fieldIndexHoleMinThreshold = null;
    }

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }

    private void configureEvent(IndexFieldHoleDataIngest.EventConfig config) {
        this.eventConfigs.add(config);
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void givenStartDate(String date) throws ParseException {
        this.startDate = start(date);
    }

    private Date start(String date) throws ParseException {
        if (date.length() == 8) {
            return formatDate.parse(date);
        } else if (date.length() == 15) {
            return formatDateTime.parse(date);
        } else {
            return formatDateTimeMillis.parse(date);
        }
    }

    private void givenEndDate(String date) throws ParseException {
        this.endDate = end(date);
    }

    private void givenPlan(String plan) {
        this.plan = plan;
    }

    private Date end(String date) throws ParseException {
        if (date.length() == 8) {
            return formatDateTimeMillis.parse(date + " 235959.999");
        } else if (date.length() == 15) {
            return formatDateTimeMillis.parse(date + ".999");
        } else {
            return formatDateTimeMillis.parse(date);
        }
    }

    private void givenFieldIndexMinThreshold(double threshold) {
        this.fieldIndexHoleMinThreshold = threshold;
    }

    private void expectEvents(String date, String... uids) {
        for (String uid : uids) {
            this.expectedEvents.add(new Event(date, uid));
        }
    }

    private void expectPlan(Date begin, Date end, String plan) {
        this.expectedPlans.put(Pair.of(begin, end), Pair.of(plan, plan));
    }

    private void expectPlan(Date begin, Date end, String pushdownPlan, String finalPlan) {
        this.expectedPlans.put(Pair.of(begin, end), Pair.of(pushdownPlan, finalPlan));
    }

    private AccumuloClient createClient() throws Exception {
        AccumuloClient client = new QueryTestTableHelper(getClass().toString(), log).client;
        IndexFieldHoleDataIngest.writeItAll(client, getRange(), eventConfigs);
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        return client;
    }

    private Date addDays(Date date, int days) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(date.getTime()));
        calendar.add(Calendar.DATE, days);
        return calendar.getTime();
    }

    private Date midnight(Date date) {
        Calendar midnight = Calendar.getInstance();
        midnight.setTime(date);
        midnight.set(Calendar.HOUR, 0);
        midnight.set(Calendar.MINUTE, 0);
        midnight.set(Calendar.SECOND, 0);
        midnight.set(Calendar.MILLISECOND, 0);
        return midnight.getTime();
    }

    /*
     * ensure that each subRange has all days with holes or all days with no holes ensure that all milliseconds in the original date range are covered
     */
    private void assertSubrangesCorrect(AccumuloClient client) throws Exception {
        DatePartitionedQueryPlanner queryPlanner = (DatePartitionedQueryPlanner) logic.getQueryPlanner();

        // Ensure that each subRange has all days with holes or all days with no holes
        MetadataHelper helper = this.logic.getMetadataHelperFactory().createMetadataHelper(client, logic.getMetadataTableName(), authSet);
        Set<String> fieldsInQuery = queryPlanner.getFieldsForQuery(this.logic.getConfig().getQueryTree(), helper);
        Set<Date> datesWithHoles = new HashSet<>();
        Set<Date> datesWithoutHoles = new HashSet<>();
        this.eventConfigs.forEach(config -> {
            if (config.getTime() >= this.startDate.getTime() && config.getTime() <= this.endDate.getTime()) {
                // field has to be in the query and fieldIndexHoleMinThreshold != null and index / frequency < fieldIndexHoleMinThreshold
                boolean hasHoles = config.getMetadataCounts().entrySet().stream().filter(e -> fieldsInQuery.contains(e.getKey()))
                                .anyMatch(e -> this.fieldIndexHoleMinThreshold != null && ((double) (e.getValue().getValue1())
                                                / ((double) e.getValue().getValue0())) < this.fieldIndexHoleMinThreshold);
                if (hasHoles) {
                    datesWithHoles.add(new Date(config.getTime()));
                } else {
                    datesWithoutHoles.add(new Date(config.getTime()));
                }
            }
        });

        SortedMap<Pair<Date,Date>,Set<String>> subRanges = queryPlanner.getSubQueryDateRanges(logic.getConfig());
        // if the subRanges is null, then this implies no holes
        if (subRanges == null) {
            return;
        }
        // Subranges are sorted and should begin with the query beginDate and end with the query endDate
        Pair<Date,Date> firstSubRange = subRanges.keySet().stream().findFirst().get();
        Assert.assertNotNull("firstSubRange should not be null", firstSubRange);
        Assert.assertEquals("begin of lastSubRange should equal query beginDate", this.startDate.getTime(), firstSubRange.getLeft().getTime());
        Pair<Date,Date> lastSubRange = subRanges.keySet().stream().reduce((first, second) -> second).get();
        Assert.assertNotNull("lastSubRange should not be null", lastSubRange);
        Assert.assertEquals("end of lastSubRange should equal query endDate", this.endDate.getTime(), lastSubRange.getRight().getTime());

        Assert.assertEquals(getDiffs(expectedPlans.keySet(), subRanges.keySet()), expectedPlans.keySet(), subRanges.keySet());
        ASTJexlScript planTree = JexlASTHelper.parseJexlQuery(plan);

        Pair<Date,Date> previousPair = null;
        for (Pair<Date,Date> range : subRanges.keySet()) {
            Set<String> datesWithHolesInRange = new TreeSet<>();
            Set<String> datesWithoutHolesInRange = new TreeSet<>();
            // adding 24 hours to b is guaranteed to fall on the next sequential date until we are outside the range
            for (Date b = new Date(range.getLeft().getTime()); b.getTime() <= range.getRight().getTime(); b = addDays(b, 1)) {
                if (datesWithHoles.contains(midnight(b))) {
                    datesWithHolesInRange.add(formatDate.format(b));
                }
                if (datesWithoutHoles.contains(midnight(b))) {
                    datesWithoutHolesInRange.add(formatDate.format(b));
                }
            }
            // adding 24 hours to b is guaranteed to fall on the next sequential date, but this might miss
            // the range end date so perform the same check on the range end date here
            if (datesWithHoles.contains(midnight(range.getRight()))) {
                datesWithHolesInRange.add(formatDate.format(range.getRight()));
            }
            if (datesWithoutHoles.contains(midnight(range.getRight()))) {
                datesWithoutHolesInRange.add(formatDate.format(range.getRight()));
            }

            Assert.assertFalse("Subrange " + range + " must have all days with holes or all days with no holes: hasHoles:" + datesWithHolesInRange
                            + " hasNoHoles:" + datesWithoutHolesInRange, !datesWithHolesInRange.isEmpty() && !datesWithoutHolesInRange.isEmpty());

            // check that there is one millisecond difference between the end of one range and the beginning of the next
            if (previousPair != null) {
                long difference = range.getLeft().getTime() - previousPair.getRight().getTime();
                Assert.assertEquals("Expected difference of 1ms, got " + difference, 1, difference);
            }

            // check the plan
            String actualPlan = JexlStringBuildingVisitor.buildQuery(PushdownUnindexedFieldsVisitor.pushdownPredicates(planTree, subRanges.get(range)));
            String expectedPlan = expectedPlans.get(range).getLeft();
            assertPlanEquals(expectedPlan, actualPlan);

            previousPair = range;
        }
    }

    private AccumuloClient assertQueryResults() throws Exception {
        return assertQueryResults(false);
    }

    private AccumuloClient assertQueryResults(boolean fullTableScanRequired) throws Exception {
        // setup the full table scan enabled flag
        this.logic.setFullTableScanEnabled(fullTableScanRequired);

        // Initialize the query settings.
        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(this.startDate);
        settings.setEndDate(this.endDate);
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(this.query);
        settings.setParameters(this.queryParameters);
        settings.setId(UUID.randomUUID());

        log.debug("query: " + settings.getQuery());
        log.debug("queryLogicName: " + settings.getQueryLogicName());

        // Initialize the query logic.
        if (fieldIndexHoleMinThreshold != null) {
            logic.setIndexFieldHoleMinThreshold(fieldIndexHoleMinThreshold);
        }
        AccumuloClient client = createClient();

        GenericQueryConfiguration config = logic.initialize(client, settings, authSet);
        logic.setupQuery(config);

        Set<String> actualPlans = new HashSet<>();
        actualPlans.addAll(PartitionedPlanVisitor.getPlans(config.getQueryString()));

        // Run the query and retrieve the response.
        DocumentTransformer transformer = (DocumentTransformer) (logic.getTransformer(settings));
        List<Object> eventList = Lists.newArrayList(new DatawaveTransformIterator<>(logic.iterator(), transformer));
        DefaultEventQueryResponse response = ((DefaultEventQueryResponse) transformer.createResponse(eventList));

        Set<Event> actualEvents = new HashSet<>();
        // Extract the events from the response.
        for (EventBase event : response.getEvents()) {
            String row = event.getMetadata().getRow();
            String date = row.substring(0, row.indexOf("_"));
            actualEvents.add(new Event(date, event.getMetadata().getInternalId()));
        }

        Assert.assertEquals(getDiffs(expectedEvents, actualEvents), expectedEvents, actualEvents);

        assertPlanEquals(plan, ((DatePartitionedQueryPlanner) logic.getQueryPlanner()).getInitialPlan());
        Set<String> expectedFinalPlans = expectedPlans.values().stream().map(e -> e.getRight()).collect(Collectors.toSet());
        assertPlanEquals(expectedFinalPlans, actualPlans);

        // verify that the full table scan was actually required
        if (fullTableScanRequired) {
            try {
                logic.setFullTableScanEnabled(false);
                logic.initialize(client, settings, authSet);
                Assert.fail("Expected full table scan to be required");
            } catch (FullTableScansDisallowedException e) {
                // expected
            }
        }

        return client;
    }

    private String getDiffs(Set<?> expected, Set<?> actual) {
        StringBuilder builder = new StringBuilder();
        for (Object e : expected) {
            if (!actual.contains(e)) {
                builder.append("\nmissing " + e);
            }
        }
        for (Object e : expected) {
            if (actual.contains(e)) {
                builder.append("\nmatched " + e);
            }
        }
        for (Object e : actual) {
            if (!expected.contains(e)) {
                builder.append("\nextra " + e);
            }
        }
        builder.append('\n');
        return builder.toString();
    }

    /**
     * assertQuery is almost the same as Assert.assertEquals except that it will allow for different orderings of the terms within an AND or and OR.
     *
     * @param expected
     *            The expected query
     * @param query
     *            The query being tested
     */
    protected void assertPlanEquals(String expected, String query) throws org.apache.commons.jexl3.parser.ParseException {
        // first do the quick check
        if (expected.equals(query)) {
            return;
        }

        ASTJexlScript expectedTree = JexlASTHelper.parseJexlQuery(expected);
        expectedTree = TreeFlatteningRebuildingVisitor.flattenAll(expectedTree);
        ASTJexlScript queryTree = JexlASTHelper.parseJexlQuery(query);
        queryTree = TreeFlatteningRebuildingVisitor.flattenAll(queryTree);
        TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(expectedTree, queryTree);
        if (!comparison.isEqual()) {
            throw new ComparisonFailure(comparison.getReason(), expected, query);
        }
    }

    protected void assertPlanEquals(Set<String> expectedPlans, Set<String> actualPlans) throws org.apache.commons.jexl3.parser.ParseException {
        Assert.assertEquals("Expected plans differ in size from actual plans", expectedPlans.size(), actualPlans.size());

        // we will be modifying the actual set
        actualPlans = new HashSet<>(actualPlans);
        for (String expected : expectedPlans) {
            String match = null;
            for (String actual : actualPlans) {
                try {
                    assertPlanEquals(expected, actual);
                    match = actual;
                    break;
                } catch (ComparisonFailure c) {

                }
            }
            if (match == null) {
                throw new ComparisonFailure("Unable to find expected plan in actual plans", expected, actualPlans.toString());
            } else {
                actualPlans.remove(match);
            }
        }
    }

    /**
     * Test a query that does not target any fields with field index holes.
     */
    @Test
    public void testNoFieldIndexHoles() throws Exception {
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130101"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130102"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130103"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130104"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130105"));

        givenQuery("UUID =~ 'C.*' || UUID =~ 'S.*'");
        givenStartDate("20130101");
        givenEndDate("20130105");
        givenPlan("UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");

        expectPlan(start("20130101"), end("20130105"), "UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");

        expectEvents("20130101", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130102", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130103", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130104", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130105", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);

        assertSubrangesCorrect(assertQueryResults());
    }

    /**
     * Test a query that targets fields with field index holes that do not fall within the query's date range.
     */
    @Test
    public void testFieldIndexHolesOutsideDateRange() throws Exception {
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130101"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130102"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130103"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130104"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130105").withMetadataCount("UUID", 10L, 2L));

        givenQuery("UUID =~ 'C.*' || UUID =~ 'S.*'");
        givenStartDate("20130101");
        givenEndDate("20130104");
        givenPlan("UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");

        expectPlan(start("20130101"), end("20130104"), "UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");

        expectEvents("20130101", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130102", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130103", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130104", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);

        assertSubrangesCorrect(assertQueryResults());
    }

    /**
     * Test a query that targets fields with field index holes that fall fully within the query's date range.
     */
    @Test
    public void testFieldIndexHolesWithinDateRange() throws Exception {
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130101"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130102"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130103").withMetadataCount("UUID", 10L, 2L));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130104"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130105"));

        givenQuery("UUID =~ 'C.*' || UUID =~ 'S.*'");
        givenStartDate("20130101");
        givenEndDate("20130105");
        givenPlan("UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");

        expectPlan(start("20130101"), end("20130102"), "UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");
        expectPlan(start("20130103"), end("20130103"),
                        "((_Eval_ = true) && (UUID == 'capone')) || ((_Eval_ = true) && (UUID == 'corleone')) || ((_Eval_ = true) && (UUID == 'soprano'))");
        expectPlan(start("20130104"), end("20130105"), "UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");

        expectEvents("20130101", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130102", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130103", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130104", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130105", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);

        assertSubrangesCorrect(assertQueryResults(true));
    }

    /**
     * Test a query that targets fields with field index holes that fall partially within the query's date range.
     */
    @Test
    public void testFieldIndexHolesPartiallyWithinDateRange() throws Exception {
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130101"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130102"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130103").withMetadataCount("UUID", 10L, 2L));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130104").withMetadataCount("UUID", 10L, 2L));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130105").withMetadataCount("UUID", 10L, 2L));

        givenQuery("UUID =~ 'C.*' || UUID =~ 'S.*'");
        givenStartDate("20130101");
        givenEndDate("20130104");
        givenPlan("UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");

        expectPlan(start("20130101"), end("20130102"), "UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");
        expectPlan(start("20130103"), end("20130104"),
                        "((_Eval_ = true) && (UUID == 'capone')) || ((_Eval_ = true) && (UUID == 'corleone')) || ((_Eval_ = true) && (UUID == 'soprano'))");

        expectEvents("20130101", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130102", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130103", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130104", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);

        assertSubrangesCorrect(assertQueryResults(true));
    }

    /**
     * Test a query that targets fields with field index holes for different fields that are consecutive to each other.
     */
    @Test
    public void testConsecutiveFieldIndexHolesForDifferentFields() throws Exception {
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130101"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130102"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130103").withMetadataCount("GENDER", 10L, 2L));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130104").withMetadataCount("UUID", 10L, 2L));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130105"));

        givenQuery("(UUID =~ 'C.*' || UUID =~ 'S.*') && GEN == 'MALE'");
        givenStartDate("20130101");
        givenEndDate("20130105");
        givenPlan("(GENDER == 'male' || GENERE == 'male') && (UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano')");

        expectPlan(start("20130101"), end("20130102"),
                        "(GENDER == 'male' || GENERE == 'male') && (UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano')");
        // final plan delayed the clause including the GENERE term because all entries in the OR are not resolvable in the index
        expectPlan(start("20130103"), end("20130103"),
                        "(((_Eval_ = true) && (GENDER == 'male')) || GENERE == 'male') && (UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano')",
                        "((_Delayed_ = true) && (((_Eval_ = true) && (GENDER == 'male')) || GENERE == 'male')) && (UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano')");
        expectPlan(start("20130104"), end("20130104"),
                        "(GENDER == 'male' || GENERE == 'male') && (((_Eval_ = true) && (UUID == 'capone')) || ((_Eval_ = true) && (UUID == 'corleone')) || ((_Eval_ = true) && (UUID == 'soprano')))");
        expectPlan(start("20130105"), end("20130105"),
                        "(GENDER == 'male' || GENERE == 'male') && (UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano')");

        expectEvents("20130101", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130102", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130103", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130104", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130105", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);

        assertSubrangesCorrect(assertQueryResults());
    }

    /**
     * Test a query that targets fields with field index holes for different fields that overlap.
     */
    @Test
    public void testOverlappingFieldIndexHolesForDifferentFields() throws Exception {
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130101"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130102").withMetadataCount("GENDER", 10L, 2L));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130103").withMetadataCount("GENDER", 10L, 2L).withMetadataCount("UUID", 10L, 2L));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130104").withMetadataCount("UUID", 10L, 2L));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130105"));

        givenQuery("(UUID =~ 'C.*' || UUID =~ 'S.*') && GEN == 'MALE'");
        givenStartDate("20130101");
        givenEndDate("20130105");
        givenPlan("(GENDER == 'male' || GENERE == 'male') && (UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano')");

        expectPlan(start("20130101"), end("20130101"),
                        "(GENDER == 'male' || GENERE == 'male') && (UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano')");
        // final plan delayed the clause including the GENERE term because all entries in the OR are not resolvable in the index
        expectPlan(start("20130102"), end("20130102"),
                        "(((_Eval_ = true) && (GENDER == 'male')) || GENERE == 'male') && (UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano')",
                        "((_Delayed_ = true) && (((_Eval_ = true) && (GENDER == 'male')) || GENERE == 'male')) && (UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano')");
        // final plan delayed the clause including the GENERE term because all entries in the OR are not resolvable in the index
        expectPlan(start("20130103"), end("20130103"),
                        "(((_Eval_ = true) && (GENDER == 'male')) || GENERE == 'male') && (((_Eval_ = true) && (UUID == 'capone')) || ((_Eval_ = true) && (UUID == 'corleone')) || ((_Eval_ = true) && (UUID == 'soprano')))",
                        "((_Delayed_ = true) && (((_Eval_ = true) && (GENDER == 'male')) || GENERE == 'male')) && (((_Eval_ = true) && (UUID == 'capone')) || ((_Eval_ = true) && (UUID == 'corleone')) || ((_Eval_ = true) && (UUID == 'soprano')))");
        expectPlan(start("20130104"), end("20130104"),
                        "(GENDER == 'male' || GENERE == 'male') && (((_Eval_ = true) && (UUID == 'capone')) || ((_Eval_ = true) && (UUID == 'corleone')) || ((_Eval_ = true) && (UUID == 'soprano')))");
        expectPlan(start("20130105"), end("20130105"),
                        "(GENDER == 'male' || GENERE == 'male') && (UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano')");

        expectEvents("20130101", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130102", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130103", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130104", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130105", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);

        assertSubrangesCorrect(assertQueryResults(true));
    }

    /**
     * Test a query that targets fields with field index holes for different fields that overlap.
     */
    @Test
    public void testOverlappingFieldIndexHolesForDifferentFieldsNoSingleDates() throws Exception {
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130101").withMetadataCount("GENDER", 10L, 2L).withMetadataCount("UUID", 10L, 2L));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130102").withMetadataCount("GENDER", 10L, 2L).withMetadataCount("UUID", 10L, 2L));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130103").withMetadataCount("GENDER", 10L, 2L).withMetadataCount("UUID", 10L, 2L));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130104").withMetadataCount("UUID", 10L, 2L));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130105").withMetadataCount("UUID", 10L, 2L));

        givenQuery("(UUID =~ 'C.*' || UUID =~ 'S.*') && GEN == 'MALE'");
        givenStartDate("20130101");
        givenEndDate("20130105");
        givenPlan("(GENDER == 'male' || GENERE == 'male') && (((_Delayed_ = true) && (UUID =~ 'c.*')) || ((_Delayed_ = true) && (UUID =~ 's.*')))");

        // final plan delayed the clause including the GENERE term because all entries in the OR are not resolvable in the index
        expectPlan(start("20130101"), end("20130103"),
                        "(((_Eval_ = true) && (GENDER == 'male')) || GENERE == 'male') && (((_Eval_ = true) && (UUID =~ 'c.*')) || ((_Eval_ = true) && (UUID =~ 's.*')))",
                        "((_Delayed_ = true) && (((_Eval_ = true) && (GENDER == 'male')) || GENERE == 'male')) && (((_Eval_ = true) && (UUID =~ 'c.*')) || ((_Eval_ = true) && (UUID =~ 's.*')))");
        expectPlan(start("20130104"), end("20130105"),
                        "(GENDER == 'male' || GENERE == 'male') && (((_Eval_ = true) && (UUID =~ 'c.*')) || ((_Eval_ = true) && (UUID =~ 's.*')))");

        expectEvents("20130101", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130102", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130103", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130104", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130105", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);

        assertSubrangesCorrect(assertQueryResults(true));
    }

    /**
     * Test a query that targets fields with field index holes for different fields that overlap.
     */
    @Test
    public void testMissingIndexedData() throws Exception {
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130101"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130102").withMetadataCount("GENDER", 10L, 2L));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130103").withMetadataCount("GENDER", 10L, 2L).withMetadataCount("UUID", 10L, 2L));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130104").withMetadataCount("UUID", 10L, 2L));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130105"));

        givenQuery("GEN == 'MALE'");
        givenStartDate("20130101");
        givenEndDate("20130105");
        givenPlan("GENDER == 'male' || GENERE == 'male'");

        expectPlan(start("20130101"), end("20130101"), "GENDER == 'male' || GENERE == 'male'");
        expectPlan(start("20130102"), end("20130103"), "GENERE == 'male' || ((_Eval_ = true) && (GENDER == 'male'))");
        expectPlan(start("20130104"), end("20130105"), "GENDER == 'male' || GENERE == 'male'");

        expectEvents("20130101", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130102", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130103", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130104", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130105", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);

        assertSubrangesCorrect(assertQueryResults(true));
    }

    /**
     * Test a query that targets fields with field index holes at the start of the query target range.
     */
    @Test
    public void testFieldIndexHolesAtStartOfRange() throws Exception {
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130101").withMetadataCount("UUID", 10L, 2L));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130102").withMetadataCount("UUID", 20L, 2L));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130103"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130104"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130105"));

        givenQuery("UUID =~ 'C.*' || UUID =~ 'S.*'");
        givenStartDate("20130101");
        givenEndDate("20130105");
        givenPlan("UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");

        expectPlan(start("20130101"), end("20130102"),
                        "((_Eval_ = true) && (UUID == 'capone')) || ((_Eval_ = true) && (UUID == 'corleone')) || ((_Eval_ = true) && (UUID == 'soprano'))");
        expectPlan(start("20130103"), end("20130105"), "UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");

        expectEvents("20130101", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130102", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130103", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130104", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130105", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);

        assertSubrangesCorrect(assertQueryResults(true));
    }

    @Test
    public void testFieldIndexHolesAtEndOfRange() throws Exception {
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130101"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130102").withMetadataCount("UUID", 10L, 1L));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130103"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130104").withMetadataCount("UUID", 10L, 1L));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130105").withMetadataCount("UUID", 10L, 1L));

        givenQuery("UUID =~ 'C.*' || UUID =~ 'S.*'");
        givenStartDate("20120101");
        givenEndDate("20130105 120000");
        givenPlan("UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");

        expectPlan(start("20120101"), end("20130101"), "UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");
        expectPlan(start("20130102"), end("20130102"),
                        "((_Eval_ = true) && (UUID == 'capone')) || ((_Eval_ = true) && (UUID == 'corleone')) || ((_Eval_ = true) && (UUID == 'soprano'))");
        expectPlan(start("20130103"), end("20130103"), "UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");
        expectPlan(start("20130104"), end("20130105 120000"),
                        "((_Eval_ = true) && (UUID == 'capone')) || ((_Eval_ = true) && (UUID == 'corleone')) || ((_Eval_ = true) && (UUID == 'soprano'))");

        expectEvents("20130101", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130102", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130103", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130104", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130105", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);

        assertSubrangesCorrect(assertQueryResults(true));
    }

    /**
     * Test a field index hole min threshold that some differing counts do and do not meet.
     */
    @Test
    public void testFieldIndexMinThresholdWithAllMeeting() throws Exception {
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130101").withMetadataCount("UUID", 20L, 19L)); // Meets min threshold.
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130102").withMetadataCount("UUID", 20L, 19L)); // Meets min threshold.
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130103"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130104").withMetadataCount("UUID", 20L, 19L)); // Meets min threshold.
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130105"));

        givenQuery("UUID =~ 'C.*' || UUID =~ 'S.*'");
        givenStartDate("20130101");
        givenEndDate("20130105");
        givenPlan("UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");
        givenFieldIndexMinThreshold(.9);

        expectPlan(start("20130101"), end("20130105"), "UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");

        expectEvents("20130101", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130102", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130103", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130104", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130105", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);

        assertSubrangesCorrect(assertQueryResults());
    }

    /**
     * Test a field index hole min threshold that some differing counts do and do not meet.
     */
    @Test
    public void testFieldIndexMinThresholdWithSomeNotMeeting() throws Exception {
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130101").withMetadataCount("UUID", 20L, 19L)); // Meets min threshold.
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130102").withMetadataCount("UUID", 20L, 19L)); // Meets min threshold.
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130103"));
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130104").withMetadataCount("UUID", 20L, 15L)); // Does not meet min threshold.
        configureEvent(IndexFieldHoleDataIngest.EventConfig.forDate("20130105"));

        givenQuery("UUID =~ 'C.*' || UUID =~ 'S.*'");
        givenStartDate("20130101");
        givenEndDate("20130105");
        givenPlan("UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");
        givenFieldIndexMinThreshold(.9);

        expectPlan(start("20130101"), end("20130103"), "UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");
        expectPlan(start("20130104"), end("20130104"),
                        "((_Eval_ = true) && (UUID == 'capone')) || ((_Eval_ = true) && (UUID == 'corleone')) || ((_Eval_ = true) && (UUID == 'soprano'))");
        expectPlan(start("20130105"), end("20130105"), "UUID == 'capone' || UUID == 'corleone' || UUID == 'soprano'");

        expectEvents("20130101", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130102", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130103", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130104", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);
        expectEvents("20130105", IndexFieldHoleDataIngest.corleoneUID, IndexFieldHoleDataIngest.caponeUID, IndexFieldHoleDataIngest.sopranoUID);

        assertSubrangesCorrect(assertQueryResults(true));
    }
}
