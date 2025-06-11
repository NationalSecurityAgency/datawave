package datawave.query.tables;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import org.easymock.EasyMock;
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

import datawave.configuration.spring.SpringBean;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.Constants;
import datawave.query.QueryParameters;
import datawave.query.QueryTestTableHelper;
import datawave.query.RebuildingScannerTestHelper;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.rules.AmbiguousNotRule;
import datawave.query.rules.AmbiguousOrPhrasesRule;
import datawave.query.rules.AmbiguousUnquotedPhrasesRule;
import datawave.query.rules.FieldExistenceRule;
import datawave.query.rules.FieldPatternPresenceRule;
import datawave.query.rules.IncludeExcludeArgsRule;
import datawave.query.rules.IncludeExcludeIndexFieldsRule;
import datawave.query.rules.InvalidQuoteRule;
import datawave.query.rules.MinimumSlopProximityRule;
import datawave.query.rules.NumericValueRule;
import datawave.query.rules.QueryRule;
import datawave.query.rules.QueryRuleResult;
import datawave.query.rules.QueryValidationResult;
import datawave.query.rules.TimeFunctionRule;
import datawave.query.rules.UnescapedSpecialCharsRule;
import datawave.query.rules.UnescapedWildcardsInPhrasesRule;
import datawave.query.rules.UnfieldedTermsRule;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.WiseGuysIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.exception.QueryException;

@RunWith(Arquillian.class)
public class ShardQueryLogicQueryValidationTest {

    private static final Logger log = Logger.getLogger(ShardQueryLogicQueryValidationTest.class);

    private static final Authorizations auths = new Authorizations("ALL");
    private static final Set<Authorizations> authSet = Collections.singleton(auths);

    private final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    private final Map<String,String> queryParameters = new HashMap<>();

    private String query;
    private Date startDate;
    private Date endDate;

    private final List<QueryRuleResult> expectedRuleResults = new ArrayList<>();
    private Class<? extends Throwable> expectedExceptionType;
    private String expectedExceptionMessage;

    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;
    protected KryoDocumentDeserializer deserializer;

    @Deployment
    public static JavaArchive createDeployment() throws Exception {
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
    public static void beforeClass() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
    }

    @AfterClass
    public static void afterClass() throws Exception {
        TypeRegistry.reset();
    }

    @Before
    public void setup() throws ParseException {
        this.logic.setFullTableScanEnabled(true);
        this.deserializer = new KryoDocumentDeserializer();
        this.startDate = dateFormat.parse("20091231");
        this.endDate = dateFormat.parse("20150101");
    }

    @After
    public void tearDown() throws Exception {
        this.logic = null;
        this.query = null;
        this.queryParameters.clear();
        this.startDate = null;
        this.endDate = null;
        this.expectedRuleResults.clear();
        this.expectedExceptionType = null;
        this.expectedExceptionMessage = null;
    }

    private AccumuloClient createClient() throws Exception {
        AccumuloClient client = new QueryTestTableHelper(ShardQueryLogicTest.ShardRange.class.toString(), log,
                        RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER_SANS_CONSISTENCY, RebuildingScannerTestHelper.INTERRUPT.EVERY_OTHER).client;
        WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD);
        PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        return client;
    }

    private Query createSettings() {
        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(this.startDate);
        settings.setEndDate(this.endDate);
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(this.query);
        settings.setParameters(this.queryParameters);
        settings.setId(UUID.randomUUID());
        return settings;
    }

    /**
     * Validate that query rules are instantiated properly when defined from the QueryLogicFactory.xml.
     */
    @Test
    public void testBeanCreation() {
        List<QueryRule> expectedRules = new ArrayList<>();
        expectedRules.add(new InvalidQuoteRule("Check for Invalid Quote"));
        expectedRules.add(new UnfieldedTermsRule("Check for Unfielded Terms"));
        expectedRules.add(new UnescapedWildcardsInPhrasesRule("Check Quoted Phrases for Unescaped Wildcard"));
        expectedRules.add(new AmbiguousNotRule("Check for Ambiguous Usage of NOT"));
        expectedRules.add(new AmbiguousOrPhrasesRule("Check for Unfielded Terms That Could Be Wrapped"));
        expectedRules.add(new AmbiguousUnquotedPhrasesRule("Check for Unfielded Terms That Could Be Quoted"));
        expectedRules.add(new MinimumSlopProximityRule("Validate Slop Proximity"));
        expectedRules.add(new IncludeExcludeArgsRule("Validate Args of #INCLUDE and #EXCLUDE"));
        expectedRules.add(new FieldExistenceRule("Check Field Existence", Set.of("I_DO_NOT_EXIST", "_NOFIELD_", "_ANYFIELD_")));
        expectedRules.add(new UnescapedSpecialCharsRule("Check for Unescaped Special Characters", Set.of('?'), Set.of('_'), false, false));
        expectedRules.add(new FieldPatternPresenceRule("Check Presence of Field or Pattern", Map.of("_ANYFIELD_", "Detected presence of _ANYFIELD_"),
                        Map.of(".*", "Detected pattern '.*' that will match everything")));
        expectedRules.add(new IncludeExcludeIndexFieldsRule("Check #INCLUDE and #EXCLUDE for Indexed Fields"));
        expectedRules.add(new NumericValueRule("Validate Numeric Values Only Given for Numeric Fields"));
        expectedRules.add(new TimeFunctionRule("Validate #TIME_FUNCTION has Date Fields"));

        List<QueryRule> actual = logic.getValidationRules();
        Assertions.assertThat(actual).containsExactlyElementsOf(expectedRules);
    }

    /**
     * Test that attempting to validate a query when no rules are configured results in an exception.
     */
    @Test
    public void testNoRulesConfigured() {
        logic.setValidationRules(null);

        Assert.assertThrows(UnsupportedOperationException.class, this::assertResult);
    }

    /**
     * Test that when unparseable LUCENE is given, verify that the exception is encapsulated within the validation result.
     */
    @Test
    public void testLuceneParseException() throws Exception {
        givenQuery("FOO:ab:c");
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, Constants.LUCENE);

        expectExceptionInResult(QueryException.class, "Failed to parse query as LUCENE");

        assertResult();
    }

    /**
     * Test that when unparseable JEXL is given, verify that the exception is encapsulated within the validation result.
     */
    @Test
    public void testJexlParseException() throws Exception {
        givenQuery("FOO == (ab");
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, Constants.JEXL);

        expectExceptionInResult(QueryException.class, "Failed to parse query as JEXL");

        assertResult();
    }

    /**
     * Test that when an exception is thrown by a query rule validating a LUCENE query, that the exception is encapsulated within the validation result.
     */
    @Test
    public void testExceptionThrownByLuceneQueryRule() throws Exception {
        // Set up a mock rule that will throw an exception when trying to validate a lucene query.
        QueryRule mockRule = EasyMock.createMock(QueryRule.class);
        EasyMock.expect(mockRule.getName()).andReturn("Mock Rule").anyTimes();
        EasyMock.expect(mockRule.copy()).andReturn(mockRule).anyTimes();
        EasyMock.expect(mockRule.canValidate(EasyMock.anyObject())).andThrow(new IllegalArgumentException("I failed!"));
        EasyMock.replay(mockRule);
        logic.setValidationRules(List.of(mockRule));

        givenQuery("FOO:abc");
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, Constants.LUCENE);

        expectExceptionInResult(QueryException.class, "Error occurred when validating against rule Mock Rule");

        assertResult();
    }

    /**
     * Test that when an exception is thrown by a query rule validating a JEXL query, that the exception is encapsulated within the validation result.
     */
    @Test
    public void testExceptionThrownByJexlQueryRule() throws Exception {
        // Set up a mock rule that will throw an exception when trying to validate a lucene query.
        QueryRule mockRule = EasyMock.createMock(QueryRule.class);
        EasyMock.expect(mockRule.getName()).andReturn("Mock Rule").anyTimes();
        EasyMock.expect(mockRule.copy()).andReturn(mockRule).anyTimes();
        EasyMock.expect(mockRule.canValidate(EasyMock.anyObject())).andThrow(new IllegalArgumentException("I failed!"));
        EasyMock.replay(mockRule);
        logic.setValidationRules(List.of(mockRule));

        givenQuery("FOO == abc");
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, Constants.JEXL);

        expectExceptionInResult(QueryException.class, "Error occurred when validating against rule Mock Rule");

        assertResult();
    }

    /**
     * Test a LUCENE query that will result in a message from a {@link InvalidQuoteRule}.
     */
    @Test
    public void testLuceneQueryFlaggedByLuceneRule() throws Exception {
        givenQuery("FOO:`abc`");
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, Constants.LUCENE);

        // Update the rules to just the one we want to test for simplicity.
        logic.setValidationRules(List.of(new InvalidQuoteRule("Check for Invalid Quote")));

        expectRuleResult(QueryRuleResult.of("Check for Invalid Quote", "Invalid quote ` used in phrase \"FOO:`abc`\". Use ' instead."));

        assertResult();
    }

    /**
     * Test a LUCENE query that will not result in a message from a {@link InvalidQuoteRule}.
     */
    @Test
    public void testLuceneQueryNotFlaggedByLuceneRule() throws Exception {
        givenQuery("FOO:'abc'");
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, Constants.LUCENE);

        // Update the rules to just the one we want to test for simplicity.
        logic.setValidationRules(List.of(new InvalidQuoteRule("Check for Invalid Quote")));

        // We should still get a query rule result with the rule name, just not with any message.
        expectRuleResult(QueryRuleResult.of("Check for Invalid Quote"));

        assertResult();
    }

    /**
     * Test a JEXL query that will result in a message from a {@link FieldPatternPresenceRule}.
     */
    @Test
    public void testJexlQueryFlaggedByJexlRule() throws Exception {
        givenQuery("_ANYFIELD_ == '123'");
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, Constants.JEXL);

        // Update the rules to just the one we want to test for simplicity.
        logic.setValidationRules(List.of(new FieldPatternPresenceRule("Check Presence of Field or Pattern",
                        Map.of("_ANYFIELD_", "Detected presence of _ANYFIELD_"), Map.of(".*", "Detected pattern '.*' that will match everything"))));

        expectRuleResult(QueryRuleResult.of("Check Presence of Field or Pattern", "Detected presence of _ANYFIELD_"));

        assertResult();
    }

    /**
     * Test a JEXL query that will not result in a message from a {@link FieldPatternPresenceRule}.
     */
    @Test
    public void testJexlQueryNotFlaggedByJexlRule() throws Exception {
        givenQuery("FOO == '123'");
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, Constants.JEXL);

        // Update the rules to just the one we want to test for simplicity.
        logic.setValidationRules(List.of(new FieldPatternPresenceRule("Check Presence of Field or Pattern",
                        Map.of("_ANYFIELD_", "Detected presence of _ANYFIELD_"), Map.of(".*", "Detected pattern '.*' that will match everything"))));

        // We should still get a query rule result with the rule name, just not with any message.
        expectRuleResult(QueryRuleResult.of("Check Presence of Field or Pattern"));

        assertResult();
    }

    /**
     * Test a LUCENE query that should get flagged by both LUCENE and JEXL rules.
     */
    @Test
    public void testQueryFlaggedByMultipleRules() throws Exception {
        givenQuery("FOO:`abc` AND BAR:abc def ghi OR _ANYFIELD_:123");
        givenQueryParameter(QueryParameters.QUERY_SYNTAX, Constants.LUCENE);

        // Update the rules to just the one we want to test for simplicity.
        // @formatter:off
        logic.setValidationRules(List.of(
                        new InvalidQuoteRule("Check For Invalid Quote"),
                        new AmbiguousUnquotedPhrasesRule("Check for Unfielded Terms That Could Be Quoted"),
                        new FieldPatternPresenceRule("Check Presence of Field or Pattern",
                        Map.of("_ANYFIELD_", "Detected presence of _ANYFIELD_"), Map.of(".*", "Detected pattern '.*' that will match everything"))));
        // @formatter:on

        expectRuleResult(QueryRuleResult.of("Check For Invalid Quote", "Invalid quote ` used in phrase \"FOO:`abc`\". Use ' instead."));
        expectRuleResult(QueryRuleResult.of("Check for Unfielded Terms That Could Be Quoted",
                        "Ambiguous unfielded terms AND'd with fielded term detected: BAR:abc AND def AND ghi. Recommended: BAR:\"abc def ghi\""));
        expectRuleResult(QueryRuleResult.of("Check Presence of Field or Pattern", "Detected presence of _ANYFIELD_"));
        assertResult();
    }

    private void givenQuery(String query) {
        this.query = query;
    }

    private void givenQueryParameter(String parameter, String value) {
        this.queryParameters.put(parameter, value);
    }

    private void expectRuleResult(QueryRuleResult ruleResult) {
        this.expectedRuleResults.add(ruleResult);
    }

    private void expectExceptionInResult(Class<? extends Throwable> type, String message) {
        this.expectedExceptionType = type;
        this.expectedExceptionMessage = message;
    }

    private void assertResult() throws Exception {
        AccumuloClient client = createClient();
        Query settings = createSettings();
        QueryValidationResult actualResult = (QueryValidationResult) logic.validateQuery(client, settings, authSet);

        Assertions.assertThat(actualResult.getRuleResults()).isEqualTo(expectedRuleResults);
        if (expectedExceptionType == null) {
            Assertions.assertThat(actualResult.getException()).isNull();
        } else {
            Assertions.assertThat(actualResult.getException()).hasMessage(expectedExceptionMessage).isInstanceOf(expectedExceptionType);
        }
    }

}
