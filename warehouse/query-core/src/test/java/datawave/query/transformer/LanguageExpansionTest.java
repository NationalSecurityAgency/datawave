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
 * Tests for usage of #LANGUAGE_EXPANSION in queries.
 */
public abstract class LanguageExpansionTest {

    @RunWith(Arquillian.class)
    public static class ShardRange extends LanguageExpansionTest {

        @Override
        protected VisibilityWiseGuysIngestWithModel.WhatKindaRange getRange() {
            return VisibilityWiseGuysIngestWithModel.WhatKindaRange.SHARD;
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRange extends LanguageExpansionTest {

        @Override
        protected VisibilityWiseGuysIngestWithModel.WhatKindaRange getRange() {
            return VisibilityWiseGuysIngestWithModel.WhatKindaRange.DOCUMENT;
        }
    }

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final Logger log = Logger.getLogger(LanguageExpansionTest.class);
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
        JexlNodeAssert.assertThat(expectedScript).isEqualTo(plannedScript);
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

    @Test
    public void testNoLanguageExpansion() throws Exception {
        givenQuery("QUOTE == 'break' && QUOTE == 'violeta'");
        givenExpectedPlan("QUOTE == 'break' && QUOTE == 'violeta'");
        runTestQuery();
    }

    @Test
    public void testEnglish_JexlSyntax_QueryOptions() throws Exception {
        givenQuery("QUOTE == 'boy' && QUOTE == 'violeta' && f:options('language.expansion','en')");
        givenExpectedPlan("QUOTE == 'violeta' && (QUOTE == 'boi' || QUOTE == 'boy')");
        runTestQuery();
    }

    @Test
    public void testEnglish_JexlSyntax_QueryParameter() throws Exception {
        givenQueryParameter(QueryParameters.LANGUAGE_EXPANSION, "en");
        givenQuery("QUOTE == 'boy' && QUOTE == 'violeta'");
        givenExpectedPlan("QUOTE == 'violeta' && (QUOTE == 'boi' || QUOTE == 'boy')");
        runTestQuery();
    }

    @Test
    public void testEnglish_LuceneSyntax_QueryOptions() throws Exception {
        givenQuery("QUOTE:boy AND QUOTE:violeta AND #OPTIONS('language.expansion','en')");
        givenExpectedPlan("QUOTE == 'violeta' && (QUOTE == 'boi' || QUOTE == 'boy')");
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        runTestQuery();
    }

    @Test
    public void testEnglish_LuceneSyntax_QueryParameter() throws Exception {
        givenQueryParameter(QueryParameters.LANGUAGE_EXPANSION, "en");
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        givenQuery("QUOTE:boy AND QUOTE:violeta");
        givenExpectedPlan("QUOTE == 'violeta' && (QUOTE == 'boi' || QUOTE == 'boy')");
        runTestQuery();
    }

    @Test
    public void testSpanish_JexlSyntax_QueryOptions() throws Exception {
        givenQuery("QUOTE == 'boy' && QUOTE == 'violeta' && f:options('language.expansion', 'es')");
        givenExpectedPlan("QUOTE == 'boy' && (QUOTE == 'violet' || QUOTE == 'violeta')");
        runTestQuery();
    }

    @Test
    public void testSpanish_JexlSyntax_QueryParameter() throws Exception {
        givenQueryParameter(QueryParameters.LANGUAGE_EXPANSION, "es");
        givenQuery("QUOTE == 'boy' && QUOTE == 'violeta'");
        givenExpectedPlan("QUOTE == 'boy' && (QUOTE == 'violet' || QUOTE == 'violeta')");
        runTestQuery();
    }

    @Test
    public void testSpanish_LuceneSyntax_QueryOptions() throws Exception {
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        givenQuery("QUOTE:boy AND QUOTE:violeta AND #OPTIONS('language.expansion', 'es')");
        givenExpectedPlan("QUOTE == 'boy' && (QUOTE == 'violet' || QUOTE == 'violeta')");
        runTestQuery();
    }

    @Test
    public void testSpanish_LuceneSyntax_QueryParameter() throws Exception {
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        givenQueryParameter(QueryParameters.LANGUAGE_EXPANSION, "es");
        givenQuery("QUOTE:boy AND QUOTE:violeta");
        givenExpectedPlan("QUOTE == 'boy' && (QUOTE == 'violet' || QUOTE == 'violeta')");
        runTestQuery();
    }

    @Test
    public void testEnglishAndSpanish_JexlSyntax_QueryOptions() throws Exception {
        givenQuery("QUOTE == 'boy' && QUOTE == 'violeta' && f:options('language.expansion','en,es')");
        givenExpectedPlan("(QUOTE == 'boi' || QUOTE == 'boy') && (QUOTE == 'violet' || QUOTE == 'violeta')");
        runTestQuery();
    }

    @Test
    public void testEnglishAndSpanish_JexlSyntax_QueryParameter() throws Exception {
        givenQueryParameter(QueryParameters.LANGUAGE_EXPANSION, "en,es");
        givenQuery("QUOTE == 'boy' && QUOTE == 'violeta'");
        givenExpectedPlan("(QUOTE == 'boi' || QUOTE == 'boy') && (QUOTE == 'violet' || QUOTE == 'violeta')");
        runTestQuery();
    }

    @Test
    public void testEnglishAndSpanish_LuceneSyntax_QueryOptions() throws Exception {
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        givenQuery("QUOTE:boy AND QUOTE:violeta AND #OPTIONS('language.expansion','en,es')");
        givenExpectedPlan("(QUOTE == 'boi' || QUOTE == 'boy') && (QUOTE == 'violet' || QUOTE == 'violeta')");
        runTestQuery();
    }

    @Test
    public void testEnglishAndSpanish_LuceneSyntax_QueryParameter() throws Exception {
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        givenQueryParameter(QueryParameters.LANGUAGE_EXPANSION, "en,es");
        givenQuery("QUOTE:boy AND QUOTE:violeta");
        givenExpectedPlan("(QUOTE == 'boi' || QUOTE == 'boy') && (QUOTE == 'violet' || QUOTE == 'violeta')");
        runTestQuery();
    }

    @Test
    public void testAllExpansion_JexlSyntax_QueryOptions() throws Exception {
        givenQuery("QUOTE == 'boy' && QUOTE == 'violeta' && f:options('language.expansion','all')");
        givenExpectedPlan(
                        "(QUOTE == 'boi' || QUOTE == 'boy') && (QUOTE == 'violt' || QUOTE == 'viole' || QUOTE == 'viol' || QUOTE == 'violet' || QUOTE == 'violeta')");
        runTestQuery();
    }

    @Test
    public void testAllExpansion_JexlSyntax_QueryParameter() throws Exception {
        givenQueryParameter(QueryParameters.LANGUAGE_EXPANSION, "all");
        givenQuery("QUOTE == 'boy' && QUOTE == 'violeta'");
        givenExpectedPlan(
                        "(QUOTE == 'boi' || QUOTE == 'boy') && (QUOTE == 'violt' || QUOTE == 'viole' || QUOTE == 'viol' || QUOTE == 'violet' || QUOTE == 'violeta')");
        runTestQuery();
    }

    @Test
    public void testAllExpansion_LuceneSyntax_QueryOptions() throws Exception {
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        givenQuery("QUOTE:boy AND QUOTE:violeta AND #OPTIONS('language.expansion','all')");
        givenExpectedPlan(
                        "(QUOTE == 'boi' || QUOTE == 'boy') && (QUOTE == 'violt' || QUOTE == 'viole' || QUOTE == 'viol' || QUOTE == 'violet' || QUOTE == 'violeta')");
        runTestQuery();
    }

    @Test
    public void testAllExpansion_LuceneSyntax_QueryParameter() throws Exception {
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        givenQueryParameter(QueryParameters.LANGUAGE_EXPANSION, "all");
        givenQuery("QUOTE:boy AND QUOTE:violeta");
        givenExpectedPlan(
                        "(QUOTE == 'boi' || QUOTE == 'boy') && (QUOTE == 'violt' || QUOTE == 'viole' || QUOTE == 'viol' || QUOTE == 'violet' || QUOTE == 'violeta')");
        runTestQuery();
    }

    // (jexl) expansion via options, stemming via options
    @Test
    public void testEnglish_JexlSyntax_ExpansionViaOptions_StemmingDisabledViaOptions() throws Exception {
        givenQuery("QUOTE == 'boy' && QUOTE == 'violeta' && f:options('language.expansion','en') && f:options('disable.stemming', 'en')");
        givenExpectedPlan("QUOTE == 'boy' && QUOTE == 'violeta'");
        runTestQuery();
    }

    // (jexl) expansion via options, stemming via parameters
    @Test
    public void testEnglish_JexlSyntax_ExpansionViaOptions_StemmingDisabledViaParameters() throws Exception {
        givenQueryParameter(QueryParameters.DISABLE_STEMMING, "en");
        givenQuery("QUOTE == 'boy' && QUOTE == 'violeta' && f:options('language.expansion','en')");
        givenExpectedPlan("QUOTE == 'boy' && QUOTE == 'violeta'");
        runTestQuery();
    }

    // (jexl) expansion via parameters, stemming via options
    @Test
    public void testEnglish_JexlSyntax_ExpansionViaParameters_StemmingDisabledViaOptions() throws Exception {
        givenQueryParameter(QueryParameters.LANGUAGE_EXPANSION, "en");
        givenQuery("QUOTE == 'boy' && QUOTE == 'violeta' && f:options('disable.stemming','en')");
        givenExpectedPlan("QUOTE == 'boy' && QUOTE == 'violeta'");
        runTestQuery();
    }

    // (jexl) expansion via parameters, stemming via parameters
    @Test
    public void testEnglish_JexlSyntax_ExpansionViaParameters_StemmingDisabledViaParameters() throws Exception {
        givenQueryParameter(QueryParameters.LANGUAGE_EXPANSION, "en");
        givenQueryParameter(QueryParameters.DISABLE_STEMMING, "en");
        givenQuery("QUOTE == 'boy' && QUOTE == 'violeta'");
        givenExpectedPlan("QUOTE == 'boy' && QUOTE == 'violeta'");
        runTestQuery();
    }

    // (lucene) expansion via options, stemming via options
    @Test
    public void testEnglish_LuceneSyntax_ExpansionViaOptions_StemmingDisabledViaOptions() throws Exception {
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        givenQuery("QUOTE:boy AND QUOTE:violeta AND #OPTIONS('language.expansion','en') AND #OPTIONS('disable.stemming', 'en')");
        givenExpectedPlan("QUOTE == 'boy' && QUOTE == 'violeta'");
        runTestQuery();
    }

    // (lucene) expansion via options, stemming via parameters
    @Test
    public void testEnglish_LuceneSyntax_ExpansionViaOptions_StemmingDisabledViaParameters() throws Exception {
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        givenQueryParameter(QueryParameters.DISABLE_STEMMING, "en");
        givenQuery("QUOTE:boy AND QUOTE:violeta #OPTIONS('language.expansion','en')");
        givenExpectedPlan("QUOTE == 'boy' && QUOTE == 'violeta'");
        runTestQuery();
    }

    // (lucene) expansion via parameters, stemming via options
    @Test
    public void testEnglish_LuceneSyntax_ExpansionViaParameters_StemmingDisabledViaOptions() throws Exception {
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        givenQueryParameter(QueryParameters.LANGUAGE_EXPANSION, "en");
        givenQuery("QUOTE:boy AND QUOTE:violeta #OPTIONS('disable.stemming','en')");
        givenExpectedPlan("QUOTE == 'boy' && QUOTE == 'violeta'");
        runTestQuery();
    }

    // (lucene) expansion via parameters, stemming via parameters
    @Test
    public void testEnglish_LuceneSyntax_ExpansionViaParameters_StemmingDisabledViaParameters() throws Exception {
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        givenQueryParameter(QueryParameters.LANGUAGE_EXPANSION, "en");
        givenQueryParameter(QueryParameters.DISABLE_STEMMING, "en");
        givenQuery("QUOTE:boy AND QUOTE:violeta");
        givenExpectedPlan("QUOTE == 'boy' && QUOTE == 'violeta'");
        runTestQuery();
    }

    @Test
    public void testNoOpAnalyzer() throws Exception {
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        givenQueryParameter(QueryParameters.LANGUAGE_EXPANSION, "it");
        givenQuery("QUOTE:vendicativi");
        // would normally expand into 'vendicativ' as well, but the test query logic factory
        // has the NoOpLuceneAnalyzer configured for Italian
        givenExpectedPlan("QUOTE == 'vendicativi'");
        runTestQuery();
    }

}
