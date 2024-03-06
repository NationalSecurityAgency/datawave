package datawave.query.planner;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TimeZone;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
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
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Lists;

import datawave.configuration.spring.SpringBean;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper;
import datawave.query.function.JexlEvaluation;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.transformer.DocumentTransformer;
import datawave.query.util.FieldIndexHoleDataIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.iterator.DatawaveTransformIterator;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.DefaultEventQueryResponse;

/**
 * Tests usage of {@link FederatedQueryPlanner} in queries.
 */
public abstract class FederatedQueryTest {

    private static final Logger log = Logger.getLogger(FederatedQueryTest.class);

    @RunWith(Arquillian.class)
    public static class ShardRange extends FederatedQueryTest {

        @Override
        protected FieldIndexHoleDataIngest.Range getRange() {
            return FieldIndexHoleDataIngest.Range.SHARD;
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRange extends FederatedQueryTest {

        @Override
        protected FieldIndexHoleDataIngest.Range getRange() {
            return FieldIndexHoleDataIngest.Range.DOCUMENT;
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

    protected abstract FieldIndexHoleDataIngest.Range getRange();

    private static final Authorizations auths = new Authorizations("ALL", "E", "I");
    private static final Set<Authorizations> authSet = Collections.singleton(auths);

    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;
    protected KryoDocumentDeserializer deserializer;

    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    private final List<FieldIndexHoleDataIngest.EventConfig> eventConfigs = new ArrayList<>();
    private final Map<String,String> queryParameters = new HashMap<>();
    private final Set<Event> expectedEvents = new HashSet<>();

    private String query;
    private Date startDate;
    private Date endDate;
    private Double fieldIndexHoleMinThreshold;

    @Deployment
    public static JavaArchive createDeployment() {
        return ShrinkWrap.create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class).deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class).deleteClass(datawave.query.metrics.ShardTableQueryMetricHandler.class)
                        .addAsManifestResource(new StringAsset(
                                        "<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" + "</alternatives>"),
                                        "beans.xml");
    }

    @BeforeClass
    public static void beforeClass() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    }

    @Before
    public void setup() throws ParseException {
        this.logic.setFullTableScanEnabled(true);
        this.logic.setMaxEvaluationPipelines(1);
        this.logic.setQueryExecutionForPageTimeout(300000000000000L);
        this.logic.setQueryPlanner(new FederatedQueryPlanner());
        this.deserializer = new KryoDocumentDeserializer();
    }

    @After
    public void tearDown() {
        this.eventConfigs.clear();
        this.queryParameters.clear();
        this.expectedEvents.clear();
        this.query = null;
        this.startDate = null;
        this.endDate = null;
        this.fieldIndexHoleMinThreshold = null;
    }

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }

    private void configureEvent(FieldIndexHoleDataIngest.EventConfig config) {
        this.eventConfigs.add(config);
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void givenStartDate(String date) throws ParseException {
        this.startDate = format.parse(date);
    }

    private void givenEndDate(String date) throws ParseException {
        this.endDate = format.parse(date);
    }

    private void givenFieldIndexMinThreshold(double threshold) {
        this.fieldIndexHoleMinThreshold = threshold;
    }

    private void expectEvents(String date, String... uids) {
        for (String uid : uids) {
            this.expectedEvents.add(new Event(date, uid));
        }
    }

    private AccumuloClient createClient() throws Exception {
        AccumuloClient client = new QueryTestTableHelper(getClass().toString(), log, RebuildingScannerTestHelper.TEARDOWN.NEVER,
                        RebuildingScannerTestHelper.INTERRUPT.NEVER).client;
        FieldIndexHoleDataIngest.writeItAll(client, getRange(), eventConfigs);
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        return client;
    }

    private void assertQueryResults() throws Exception {
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
            logic.setFieldIndexHoleMinThreshold(fieldIndexHoleMinThreshold);
        }
        AccumuloClient client = createClient();
        GenericQueryConfiguration config = logic.initialize(client, settings, authSet);
        logic.setupQuery(config);

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
    }

    private String getDiffs(Set<Event> expectedEvents, Set<Event> actualEvents) {
        StringBuilder builder = new StringBuilder();
        for (Event e : expectedEvents) {
            if (!actualEvents.contains(e)) {
                builder.append("\nmissing " + e);
            }
        }
        for (Event e : expectedEvents) {
            if (actualEvents.contains(e)) {
                builder.append("\nmatched " + e);
            }
        }
        for (Event e : actualEvents) {
            if (!expectedEvents.contains(e)) {
                builder.append("\nextra " + e);
            }
        }
        return builder.toString();
    }

    /**
     * Test a query that does not target any fields with field index holes.
     */
    @Test
    public void testNoFieldIndexHoles() throws Exception {
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130101"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130102"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130103"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130104"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130105"));

        givenQuery("UUID =~ '^[CS].*'");
        givenStartDate("20130101");
        givenEndDate("20130105");

        expectEvents("20130101", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130102", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130103", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130104", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130105", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);

        assertQueryResults();
    }

    /**
     * Test a query that targets fields with field index holes that do not fall within the query's date range.
     */
    @Test
    public void testFieldIndexHolesOutsideDateRange() throws Exception {
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130101"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130102"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130103"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130104"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130105").withMetadataCount("UUID", 10L, 2L));

        givenQuery("UUID =~ '^[CS].*'");
        givenStartDate("20130101");
        givenEndDate("20130104");

        expectEvents("20130101", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130102", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130103", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130104", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);

        assertQueryResults();
    }

    /**
     * Test a query that targets fields with field index holes that fall fully within the query's date range.
     */
    @Test
    public void testFieldIndexHolesWithinDateRange() throws Exception {
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130101"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130102"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130103").withMetadataCount("UUID", 10L, 2L));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130104"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130105"));

        givenQuery("UUID =~ '^[CS].*'");
        givenStartDate("20130101");
        givenEndDate("20130105");

        expectEvents("20130101", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130102", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130103", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130104", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130105", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);

        assertQueryResults();
    }

    /**
     * Test a query that targets fields with field index holes that fall partially within the query's date range.
     */
    @Test
    public void testFieldIndexHolesPartiallyWithinDateRange() throws Exception {
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130101"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130102"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130103").withMetadataCount("UUID", 10L, 2L));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130104").withMetadataCount("UUID", 10L, 2L));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130105").withMetadataCount("UUID", 10L, 2L));

        givenQuery("UUID =~ '^[CS].*'");
        givenStartDate("20130101");
        givenEndDate("20130104");

        expectEvents("20130101", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130102", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130103", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130104", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);

        assertQueryResults();
    }

    /**
     * Test a query that targets fields with field index holes for different fields that are consecutive to each other.
     */
    @Test
    public void testConsecutiveFieldIndexHolesForDifferentFields() throws Exception {
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130101"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130102"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130103").withMetadataCount("GENDER", 10L, 2L));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130104").withMetadataCount("UUID", 10L, 2L));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130105"));

        givenQuery("UUID =~ '^[CS].*' && GEN == 'MALE'");
        givenStartDate("20130101");
        givenEndDate("20130105");

        expectEvents("20130101", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130102", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130103", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130104", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130105", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);

        assertQueryResults();
    }

    /**
     * Test a query that targets fields with field index holes for different fields that overlap.
     */
    @Test
    public void testOverlappingFieldIndexHolesForDifferentFields() throws Exception {
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130101"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130102").withMetadataCount("GENDER", 10L, 2L));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130103").withMetadataCount("GENDER", 10L, 2L).withMetadataCount("UUID", 10L, 2L));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130104").withMetadataCount("UUID", 10L, 2L));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130105"));

        givenQuery("UUID =~ '^[CS].*' && GEN == 'MALE'");
        givenStartDate("20130101");
        givenEndDate("20130105");

        expectEvents("20130101", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130102", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130103", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130104", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130105", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);

        assertQueryResults();
    }

    /**
     * Test a query that targets fields with field index holes for different fields that overlap.
     */
    @Test
    public void testOverlappingFieldIndexHolesForDifferentFieldsNoSingleDates() throws Exception {
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130101"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130102").withMetadataCount("GENDER", 10L, 2L));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130103").withMetadataCount("GENDER", 10L, 2L).withMetadataCount("UUID", 10L, 2L));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130104").withMetadataCount("UUID", 10L, 2L));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130105"));

        givenQuery("UUID =~ '^[CS].*' && GEN == 'MALE'");
        givenStartDate("20130101");
        givenEndDate("20130105");

        expectEvents("20130101", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130102", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130103", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130104", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130105", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);

        assertQueryResults();
    }

    /**
     * Test a query that targets fields with field index holes at the start of the query target range.
     */
    @Test
    public void testFieldIndexHolesAtStartOfRange() throws Exception {
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130101").withMetadataCount("UUID", 10L, 2L));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130102").withMetadataCount("UUID", 20L, 2L));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130103"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130104"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130105"));

        givenQuery("UUID =~ '^[CS].*'");
        givenStartDate("20130101");
        givenEndDate("20130105");

        expectEvents("20130101", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130102", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130103", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130104", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130105", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);

        assertQueryResults();
    }

    /**
     * Test a field index hole min threshold that some differing counts do and do not meet.
     */
    @Test
    public void testFieldIndexMinThresholdWithAllMeeting() throws Exception {
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130101").withMetadataCount("UUID", 20L, 19L)); // Meets min threshold.
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130102").withMetadataCount("UUID", 20L, 19L)); // Meets min threshold.
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130103"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130104").withMetadataCount("UUID", 20L, 19L)); // Meets min threshold.
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130105"));

        givenQuery("UUID =~ '^[CS].*'");
        givenStartDate("20130101");
        givenEndDate("20130105");
        givenFieldIndexMinThreshold(.9);

        expectEvents("20130101", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130102", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130103", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130104", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130105", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);

        assertQueryResults();
    }

    /**
     * Test a field index hole min threshold that some differing counts do and do not meet.
     */
    @Test
    public void testFieldIndexMinThresholdWithSomeNotMeeting() throws Exception {
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130101").withMetadataCount("UUID", 20L, 19L)); // Meets min threshold.
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130102").withMetadataCount("UUID", 20L, 19L)); // Meets min threshold.
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130103"));
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130104").withMetadataCount("UUID", 20L, 15L)); // Does not meet min threshold.
        configureEvent(FieldIndexHoleDataIngest.EventConfig.forDate("20130105"));

        givenQuery("UUID =~ '^[CS].*'");
        givenStartDate("20130101");
        givenEndDate("20130105");
        givenFieldIndexMinThreshold(.9);

        expectEvents("20130101", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130102", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130103", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130104", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);
        expectEvents("20130105", FieldIndexHoleDataIngest.corleoneUID, FieldIndexHoleDataIngest.caponeUID, FieldIndexHoleDataIngest.sopranoUID);

        assertQueryResults();
    }
}
