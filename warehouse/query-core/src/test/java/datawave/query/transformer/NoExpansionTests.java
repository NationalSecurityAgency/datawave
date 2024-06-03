package datawave.query.transformer;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.log4j.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import datawave.configuration.spring.SpringBean;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryParameters;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.VisibilityWiseGuysIngestWithModel;
import datawave.test.JexlNodeAssert;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;

/**
 * Tests for usage of #NO_EXPANSION in queries.
 */
public abstract class NoExpansionTests {

    @RunWith(Arquillian.class)
    public static class ShardRange extends NoExpansionTests {

        @Override
        protected VisibilityWiseGuysIngestWithModel.WhatKindaRange getRange() {
            return VisibilityWiseGuysIngestWithModel.WhatKindaRange.SHARD;
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRange extends NoExpansionTests {

        @Override
        protected VisibilityWiseGuysIngestWithModel.WhatKindaRange getRange() {
            return VisibilityWiseGuysIngestWithModel.WhatKindaRange.DOCUMENT;
        }
    }

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final Logger log = Logger.getLogger(NoExpansionTests.class);
    private static final Authorizations auths = new Authorizations("ALL", "E", "I");

    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;
    protected Set<Authorizations> authSet = Collections.singleton(auths);
    protected KryoDocumentDeserializer deserializer;

    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    private final Map<String,String> queryParameters = new HashMap<>();
    private Date startDate;
    private Date endDate;
    private String query;
    private String expectedPlan;

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

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }

    @Before
    public void setup() throws ParseException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        this.logic.setFullTableScanEnabled(true);
        this.logic.setMaxEvaluationPipelines(1);
        this.logic.setQueryExecutionForPageTimeout(300000000000000L);
        this.deserializer = new KryoDocumentDeserializer();
        this.startDate = format.parse("20091231");
        this.endDate = format.parse("20150101");
    }

    @After
    public void tearDown() {
        queryParameters.clear();
    }

    protected abstract VisibilityWiseGuysIngestWithModel.WhatKindaRange getRange();

    private void runTestQuery() throws Exception {
        log.debug("test plan against expected plan");

        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(this.startDate);
        settings.setEndDate(this.endDate);
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(this.query);
        settings.setParameters(this.queryParameters);
        settings.setId(UUID.randomUUID());

        log.debug("query: " + settings.getQuery());
        log.debug("logic: " + settings.getQueryLogicName());

        AccumuloClient client = createClient();
        GenericQueryConfiguration config = logic.initialize(client, settings, authSet);
        logic.setupQuery(config);

        String plan = logic.getPlan(client, settings, authSet, true, true);

        // order of terms in planned script is arbitrary, fall back to comparing the jexl trees
        ASTJexlScript plannedScript = JexlASTHelper.parseJexlQuery(plan);
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(this.expectedPlan);
        JexlNodeAssert.assertThat(plannedScript).isEqualTo(expectedScript);
    }

    private AccumuloClient createClient() throws Exception {
        AccumuloClient client = new QueryTestTableHelper(getClass().getName(), log, RebuildingScannerTestHelper.TEARDOWN.NEVER,
                        RebuildingScannerTestHelper.INTERRUPT.NEVER).client;
        VisibilityWiseGuysIngestWithModel.writeItAll(client, getRange());
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        return client;
    }

    private void givenQueryParameter(String key, String value) {
        this.queryParameters.put(key, value);
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void givenExpectedPlan(String expectedPlan) {
        this.expectedPlan = expectedPlan;
    }

    /**
     * Base test to verify expansion happens by default.
     */
    @Test
    public void testDefaultQueryModelExpansion() throws Exception {
        givenQuery("COLOR == 'blue' && FASTENER == 'bolt'");
        givenExpectedPlan("(COLOR == 'blue' || HUE == 'blue') && (FASTENER == 'bolt' || FIXTURE == 'bolt')");

        runTestQuery();
    }

    /**
     * Verify that when #NO_EXPANSION is specified in the query string itself, expansion does not occur.
     */
    @Test
    public void testNoExpansionViaFunction() throws Exception {
        givenQuery("COLOR == 'blue' && f:noExpansion(COLOR)");
        givenExpectedPlan("COLOR == 'blue'");

        runTestQuery();
    }

    /**
     * Verify that when #NO_EXPANSION is specified in the query string itself with multiple fields, expansion does not occur.
     */
    @Test
    public void testNoExpansionViaFunctionWithMultipleFields() throws Exception {
        givenQuery("COLOR == 'blue' && FASTENER == 'bolt' && f:noExpansion(COLOR,FASTENER)");
        givenExpectedPlan("COLOR == 'blue' && FASTENER == 'bolt'");

        runTestQuery();
    }

    /**
     * Verify that when #NO_EXPANSION is specified via the query parameters, expansion does not occur.
     */
    @Test
    public void testNoExpansionViaQueryParameters() throws Exception {
        givenQuery("COLOR == 'blue'");
        givenQueryParameter(QueryParameters.NO_EXPANSION_FIELDS, "COLOR");
        givenExpectedPlan("COLOR == 'blue'");

        runTestQuery();
    }

    /**
     * Verify that when #NO_EXPANSION is specified via the query parameters, expansion does not occur.
     */
    @Test
    public void testNoExpansionViaQueryParametersWithMultipleFields() throws Exception {
        givenQuery("COLOR == 'blue' && FASTENER == 'bolt'");
        givenQueryParameter(QueryParameters.NO_EXPANSION_FIELDS, "COLOR,FASTENER");
        givenExpectedPlan("COLOR == 'blue' && FASTENER == 'bolt'");

        runTestQuery();
    }

    /**
     * Verify that when #NO_EXPANSION is specified in the query string itself and in query parameters, expansion does not occur.
     */
    @Test
    public void testNoExpansionViaFunctionAndQueryParameters() throws Exception {
        givenQuery("COLOR == 'blue' && f:noExpansion(COLOR)");
        givenQueryParameter(QueryParameters.NO_EXPANSION_FIELDS, "COLOR");
        givenExpectedPlan("COLOR == 'blue'");

        runTestQuery();
    }

    /**
     * Verify that when #NO_EXPANSION is specified with the correct field in the query string, and the wrong field in the query parameters, that the correct
     * field is retained.
     */
    @Test
    public void testConflictingNoExpansionFields() throws Exception {
        givenQuery("COLOR == 'blue' && f:noExpansion(COLOR)");
        givenQueryParameter(QueryParameters.NO_EXPANSION_FIELDS, "HUE");
        givenExpectedPlan("COLOR == 'blue'");

        runTestQuery();
    }
}
