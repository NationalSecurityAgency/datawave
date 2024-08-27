package datawave.query.jexl.functions;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.assertj.core.api.Assertions;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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
import datawave.query.RebuildingScannerTestHelper;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.transformer.DocumentTransformer;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.result.DefaultEventQueryResponse;

/**
 * Base test set up for functions in {@link GroupingRequiredFilterFunctions}.
 */
public abstract class GroupingRequiredFilterFunctionsIntegrationTest {

    @RunWith(Arquillian.class)
    public static class ShardRangeTest extends GroupingRequiredFilterFunctionsIntegrationTest {

        @Override
        protected String getRange() {
            return "SHARD";
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRangeTest extends GroupingRequiredFilterFunctionsIntegrationTest {

        @Override
        protected String getRange() {
            return "DOCUMENT";
        }
    }

    private static final Logger log = Logger.getLogger(GroupingRequiredFilterFunctionsIntegrationTest.class);
    private static final Authorizations auths = new Authorizations("ALL", "E", "I");
    private static final Set<Authorizations> authSet = Collections.singleton(auths);

    @Inject
    @SpringBean(name = "EventQuery")
    private ShardQueryLogic logic;

    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    private final Map<String,String> queryParameters = new HashMap<>();

    private String query;
    private Date startDate;
    private Date endDate;

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
    public void setup() throws ParseException {
        this.logic.setFullTableScanEnabled(true);
        this.logic.setMaxEvaluationPipelines(1);
        this.logic.setQueryExecutionForPageTimeout(300000000000000L);
        this.startDate = format.parse("20091231");
        this.endDate = format.parse("20150101");
    }

    @After
    public void tearDown() {
        this.queryParameters.clear();
        this.query = null;
        this.startDate = null;
        this.endDate = null;
    }

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }

    protected abstract String getRange();

    private void givenQuery(String query) {
        this.query = query;
    }

    private void givenLuceneParserForLogic() {
        logic.setParser(new LuceneToJexlQueryParser());
    }

    private DefaultEventQueryResponse getQueryResponse(RebuildingScannerTestHelper.TEARDOWN teardown, RebuildingScannerTestHelper.INTERRUPT interrupt)
                    throws Exception {
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
        log.debug("teardown: " + teardown);
        log.debug("interrupt: " + interrupt);

        // Initialize the query logic.
        AccumuloClient client = createClient(teardown, interrupt);
        GenericQueryConfiguration config = logic.initialize(client, settings, authSet);
        logic.setupQuery(config);

        // Run the query and retrieve the response.
        DocumentTransformer transformer = (DocumentTransformer) (logic.getTransformer(settings));
        List<Object> eventList = Lists.newArrayList(new DatawaveTransformIterator<>(logic.iterator(), transformer));
        return ((DefaultEventQueryResponse) transformer.createResponse(eventList));
    }

    private void assertNoResults() throws Exception {
        DefaultEventQueryResponse response = getQueryResponse(RebuildingScannerTestHelper.TEARDOWN.NEVER, RebuildingScannerTestHelper.INTERRUPT.NEVER);
        Assertions.assertThat(response.getEvents()).isNull();
    }

    private void assertTotalEvents(int expected) throws Exception {
        DefaultEventQueryResponse response = getQueryResponse(RebuildingScannerTestHelper.TEARDOWN.NEVER, RebuildingScannerTestHelper.INTERRUPT.NEVER);
        Assertions.assertThat(response.getEvents()).hasSize(expected);
    }

    private AccumuloClient createClient(RebuildingScannerTestHelper.TEARDOWN teardown, RebuildingScannerTestHelper.INTERRUPT interrupt) throws Exception {
        AccumuloClient client = new QueryTestTableHelper(getClass().toString(), log, teardown, interrupt).client;
        GroupingFiltersIngest.writeItAll(client, getRange());
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        return client;
    }

    @Test
    public void testMatchInGroupWithNoMatches() throws Exception {
        givenLuceneParserForLogic();

        // Will match on name, but not on gender or age.
        givenQuery("NAME:Santino AND #MATCHES_IN_GROUP(GENDER,'FEMALE',AGE,99)");

        assertNoResults();
    }

    @Test
    public void testMatchInGroupWithPartialMatches() throws Exception {
        givenLuceneParserForLogic();

        // Will match on name and gender, but not age.
        givenQuery("NAME:Santino AND #MATCHES_IN_GROUP(GENDER,'MALE',AGE,99)");

        assertNoResults();
    }

    @Test
    public void testMatchInGroupWithMatches() throws Exception {
        givenLuceneParserForLogic();

        // Will match on name, gender, and age.
        givenQuery("NAME:Santino AND #MATCHES_IN_GROUP(GENDER,'MALE',AGE,24)");

        assertTotalEvents(1);
    }

    @Test
    public void testMatchInGroupWithNumericRegex() throws Exception {
        givenQuery("UUID =~ '^[CS].*' && grouping:matchesInGroup(GENDER,'MALE',AGE,'2*')");

        assertTotalEvents(2);
    }

    @Test
    public void testMatchInGroupWithRegex_Lucene() throws Exception {
        givenLuceneParserForLogic();

        givenQuery("(UUID:C* OR UUID:S*) AND #MATCHES_IN_GROUP(GENDER,'MALE',AGE,2*)");

        assertTotalEvents(2);
    }
}
