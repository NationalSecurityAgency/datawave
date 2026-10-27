package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import datawave.data.type.GeometryType;
import datawave.data.type.Type;
import datawave.helpers.PrintUtility;
import datawave.query.QueryTestTableHelper;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.ExceededOr;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MockMetadataHelper;
import datawave.query.util.WiseGuysIngest;
import datawave.table.constants.TableName;

@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = "datawave.query")
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:beanRefContext.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
public class ExecutableExpansionVisitorTest extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(ExecutableExpansionVisitorTest.class);
    private static final Authorizations auths = new Authorizations("ALL");

    private static AccumuloClient sharedClient;

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
        disableQueryPlanAssertion();
    }

    @Override
    protected void extraAssertions() {
        // no-op
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        QueryTestTableHelper qtth = new QueryTestTableHelper(ExecutableExpansionVisitorTest.class.toString(), log);
        sharedClient = qtth.client;

        WiseGuysIngest.writeItAll(sharedClient, WiseGuysIngest.WhatKindaRange.DOCUMENT);
        PrintUtility.printTable(sharedClient, auths, TableName.SHARD);
        PrintUtility.printTable(sharedClient, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(sharedClient, auths, QueryTestTableHelper.METADATA_TABLE_NAME);
        PrintUtility.printTable(sharedClient, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }

    @BeforeEach
    public void setupLogic() {
        setClientForTest(sharedClient);
        logic.setFullTableScanEnabled(false);
        logic.setMaxDepthThreshold(11);
        logic.setInitialMaxTermThreshold(12);
        logic.setFinalMaxTermThreshold(12);
    }

    private void runTestQuery(List<String> expected, String querystr, Map<String,String> extraParms) throws Exception {
        givenDate("20091231", "20150101");
        givenQuery(querystr);
        givenParameters(extraParms);
        expectResultCount(expected.size());
        if (!expected.isEmpty()) {
            expectUUIDs(new HashSet<>(expected));
        }

        planAndExecuteQuery();
    }

    @Test
    public void testMixedIndexOnly() {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        assertThrows(DatawaveFatalQueryException.class,
                        () -> runTestQuery(Arrays.asList(), "filter:isNull(LOCATION) || LOCATION == 'chicago'", extraParameters));
    }

    @Test
    public void testExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        runTestQuery(Arrays.asList("CAPONE"), "UUID == 'capone' && (filter:isNull(MAGIC) || LOCATION == 'chicago')", extraParameters);
    }

    @Test
    public void testGeowaveExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        MockMetadataHelper helper = new MockMetadataHelper();
        Multimap<String,Type<?>> dataTypes = ArrayListMultimap.create();
        dataTypes.put("GEO", new GeometryType());
        helper.setDataTypes(dataTypes);

        logic.setMaxDepthThreshold(30);
        logic.setInitialMaxTermThreshold(10000);
        logic.setIntermediateMaxTermThreshold(10000);
        logic.setIndexedMaxTermThreshold(10000);
        logic.setFinalMaxTermThreshold(10000);

        //  @formatter:off
        String query =
                "( " +
                   "(((_Bounded_ = true) && (NUMBER >= 0 && NUMBER <= 1000)) && geowave:intersects(GEO, 'POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))')) || " +
                   "(((_Bounded_ = true) && (NUMBER >= 0 && NUMBER <= 1000)) && geowave:intersects(GEO, 'POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))')) " +
                ") " +
                "&& GENDER == 'MALE' " +
                "&& (NOME == 'THIS' || NOME == 'THAT') " +
                "&& !filter:includeRegex(ETA, 'blah') " +
                "&& ( LOCATION == 'chicago' || LOCATION == 'newyork' || LOCATION == 'newjersey' )";
        //  @formatter:on
        String expandedQuery = JexlStringBuildingVisitor
                        .buildQuery(FunctionIndexQueryExpansionVisitor.expandFunctions(logic.getConfig(), helper, null, JexlASTHelper.parseJexlQuery(query)));

        runTestQuery(Arrays.asList(), expandedQuery, extraParameters);

        String finalQuery = JexlStringBuildingVisitor.buildQuery(logic.getConfig().getQueryTree());
        assertNotEquals("GENDER == 'male' && (NOME == 'this' || NOME == 'that') && !filter:includeRegex(ETA, 'blah') && (LOCATION == 'chicago' || LOCATION == 'newyork' || LOCATION == 'newjersey')",
                        finalQuery);

        ASTJexlScript expectedQuery = JexlASTHelper.parseJexlQuery(
                        "GENDER == 'male' && (NOME == 'that' || NOME == 'this') && (LOCATION == 'chicago' || LOCATION == 'newjersey' || LOCATION == 'newyork') && (GEO == '00' || GEO == '0202' || GEO == '020b' || GEO == '1f200a80a80a80a80a' || GEO == '1f202a02a02a02a02a' || GEO == '1f2088888888888888') && geowave:intersects(GEO, 'POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))') && !filter:includeRegex(ETA, 'blah') && ((_Bounded_ = true) && (NUMBER >= '0' && NUMBER <= '1000'))");

        assertTrue(TreeEqualityVisitor.isEqual(expectedQuery, logic.getConfig().getQueryTree()));
    }

    @Test
    public void testNestedOrExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        runTestQuery(Arrays.asList("CAPONE"), "UUID == 'capone' && ( LOCATION == 'newyork' || (filter:isNull(MAGIC) || LOCATION == 'chicago'))",
                        extraParameters);
    }

    @Test
    public void testMethodNoExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        runTestQuery(Arrays.asList("CAPONE"), "UUID == 'capone' && ( AGE.size() > 1 || AGE == '18')", extraParameters);
    }

    @Test
    public void testNumericExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        // SOPRANO is the only one with a 0 after the 1234
        runTestQuery(Arrays.asList("SOPRANO"), "BAIL=~'12340.*?'", extraParameters);

        String expectedQueryStr = "(BAIL == '+eE1.2345' || BAIL == '+fE1.23401') && ((_Eval_ = true) && (BAIL =~ '12340.*?'))";
        String plan = JexlFormattedStringBuildingVisitor.buildQuery(logic.getConfig().getQueryTree());
        assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseJexlQuery(expectedQueryStr), logic.getConfig().getQueryTree()),
                        "Expected equality: " + expectedQueryStr + " vs " + plan);
    }

    @Test
    public void testNumericExpansionIndexOnly() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        // SOPRANO is the only one with a 0 after the 1234
        runTestQuery(Arrays.asList("CAPONE"), "SENTENCE_DAYS=~'4015\\.0*'", extraParameters);

        String expectedQueryStr = "SENTENCE_DAYS == '+dE4.015'";
        String plan = JexlFormattedStringBuildingVisitor.buildQuery(logic.getConfig().getQueryTree());
        assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseJexlQuery(expectedQueryStr), logic.getConfig().getQueryTree()),
                        "Expected equality: " + expectedQueryStr + " vs " + plan);
    }

    @Test
    public void testAnyfieldNumericExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        logic.getConfig().setExcludeUnfieldedTypes(Collections.emptyList());

        // SOPRANO is the only one with a 0 after the 1234
        runTestQuery(Arrays.asList("SOPRANO"), "_ANYFIELD_ =~'12340.*?'", extraParameters);

        String expectedQueryStr = "(BAIL == '+eE1.2345' || BAIL == '+fE1.23401') && ((_Eval_ = true) && (_ANYFIELD_ =~ '12340.*?'))";
        String plan = JexlFormattedStringBuildingVisitor.buildQuery(logic.getConfig().getQueryTree());
        assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseJexlQuery(expectedQueryStr), logic.getConfig().getQueryTree()),
                        "Expected equality: " + expectedQueryStr + " vs " + plan);
    }

    @Test
    public void testAnyfieldTypeExclusion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        // ANYFIELD numeric regex normalization is no longer used, so expect no results for this query
        runTestQuery(new ArrayList<>(), "_ANYFIELD_ =~'12340.*?'", extraParameters);

        // No longer expect normalized numeric regexes for ANYFIELD
        String expectedQueryStr = "_NOFIELD_ =~ '12340.*?'";
        String plan = JexlFormattedStringBuildingVisitor.buildQuery(logic.getConfig().getQueryTree());
        assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseJexlQuery(expectedQueryStr), logic.getConfig().getQueryTree()),
                        "Expected equality: " + expectedQueryStr + " vs " + plan);
    }

    @Test
    public void testLeadingNumericExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        runTestQuery(Arrays.asList("CAPONE"), "(UUID == 'capone' || UUID == 'soprano') && BAIL=~'.*?05'", extraParameters);

        String expectedQueryStr = "(UUID == 'capone' || UUID == 'soprano') && ((_Eval_ = true) && (BAIL =~ '.*?05'))";
        String plan = JexlFormattedStringBuildingVisitor.buildQuery(logic.getConfig().getQueryTree());
        assertTrue(TreeEqualityVisitor.isEqual(JexlASTHelper.parseJexlQuery(expectedQueryStr), logic.getConfig().getQueryTree()),
                        "Expected equality: " + expectedQueryStr + " vs " + plan);
    }

    @Test
    public void testMethodExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        runTestQuery(Arrays.asList("CAPONE"), "UUID == 'capone' && ( QUOTE.size() == 1 || QUOTE == 'kind')", extraParameters);
    }

    @Test
    public void testNonEventExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        runTestQuery(Arrays.asList("CAPONE"), "UUID == 'capone' && (QUOTE == 'kind'|| BIRTH_DATE == '123')", extraParameters);
    }

    @Test
    public void testFilterExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        runTestQuery(Arrays.asList("CAPONE"), "UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind')", extraParameters);
    }

    @Test
    public void testDisableExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        ((DefaultQueryPlanner) logic.getQueryPlanner()).setExecutableExpansion(false);

        runTestQuery(Arrays.asList("CAPONE"), "UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind')", extraParameters);
    }

    private static Stream<Arguments> delayedBridgeExpansionArgs() {
        // @formatter:off
        Object[][] cases = {
                {"UUID == 'capone' && ( LOCATION == 'newyork' || !(filter:isNull(MAGIC) || LOCATION == 'chicago'))", Arrays.asList()},
                {"UUID == 'capone' && ( LOCATION == 'newyork' || !filter:isNull(MAGIC) || LOCATION == 'chicago')", Arrays.asList("CAPONE")},
        };
        // @formatter:on
        return Stream.of(cases).map(c -> Arguments.of(c[0], c[1]));
    }

    @ParameterizedTest
    @MethodSource("delayedBridgeExpansionArgs")
    public void testDelayedBridgeExpansion(String query, List<String> expected) throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        runTestQuery(expected, query, extraParameters);
    }

    @Test
    public void testMultipleExpansionsRequired() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        logic.setIntermediateMaxTermThreshold(15);

        runTestQuery(Arrays.asList("CAPONE"),
                        "UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind') && (QUOTE == 'kind'|| BIRTH_DATE == '123')",
                        extraParameters);
    }

    @Test
    public void testMinimumExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        runTestQuery(Arrays.asList("CAPONE"), "UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind' || BIRTH_DATE == '123')",
                        extraParameters);
    }

    @Test
    public void testIndexOnlyNoType() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        runTestQuery(Arrays.asList("CAPONE"), "(UUID == 'capone' &&  SENTENCE == '11y')", extraParameters);
    }

    @Test
    public void testTypedAndNotIndexed() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        runTestQuery(Arrays.asList("CAPONE"), "(UUID == 'capone' && BIRTH_DATE == '1910-12-28T00:00:05.000Z')", extraParameters);
    }

    // The tests below are pure unit tests of ExecutableExpansionVisitor/ExecutableDeterminationVisitor against
    // mocked configuration/metadata - they never touch the ingested data or query logic, so unlike the tests
    // above there is no SHARD/DOCUMENT range to parameterize over.

    @Test
    public void testMinimumExpansionParse() throws Exception {
        // make sure this works when references/referenceExpressions are/aren't included
        ASTJexlScript origQueryTree = JexlASTHelper
                        .parseJexlQuery("UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind' || BIRTH_DATE == '123')");
        ASTJexlScript derefQueryTree = (ASTJexlScript) DereferencingVisitor.dereference(origQueryTree);

        for (ASTJexlScript queryTree : Arrays.asList(origQueryTree, derefQueryTree)) {
            ShardQueryConfiguration config = Mockito.mock(ShardQueryConfiguration.class);
            MetadataHelper helper = Mockito.mock(MetadataHelper.class);

            HashSet<String> indexedFields = new HashSet<>();
            indexedFields.add("UUID");
            indexedFields.add("QUOTE");

            Mockito.doReturn(indexedFields).when(config).getIndexedFields();

            ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);

            assertFalse(JexlStringBuildingVisitor.buildQuery(queryTree).equals(JexlStringBuildingVisitor.buildQuery(newTree)));
            String expected = "(QUOTE == 'kind' && UUID == 'capone') || ((filter:includeRegex(QUOTE, '.*kind.*') || BIRTH_DATE == '123') && UUID == 'capone')";
            assertEquals(expected, JexlStringBuildingVisitor.buildQuery(newTree));
        }
    }

    @Test
    public void testArbitraryNodeExpansionFailNoFlatten() throws Exception {
        // make sure this works when references/referenceExpressions are/aren't included
        ASTJexlScript origQueryTree = JexlASTHelper
                        .parseJexlQuery("UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind' || BIRTH_DATE == '123')");
        ASTJexlScript derefQueryTree = (ASTJexlScript) DereferencingVisitor.dereference(origQueryTree);

        for (ASTJexlScript queryTree : Arrays.asList(origQueryTree, derefQueryTree)) {
            ShardQueryConfiguration config = Mockito.mock(ShardQueryConfiguration.class);
            MetadataHelper helper = Mockito.mock(MetadataHelper.class);

            HashSet<String> indexedFields = new HashSet<>();
            indexedFields.add("UUID");
            indexedFields.add("QUOTE");

            Mockito.doReturn(indexedFields).when(config).getIndexedFields();

            // find an orNode in the tree
            ExecutableExpansionVisitor visitor = new ExecutableExpansionVisitor(config, helper);
            Object data = queryTree.jjtGetChild(0).childrenAccept(visitor, null);

            assertFalse(data instanceof ExecutableExpansionVisitor.ExpansionTracker);
        }
    }

    @Test
    public void testArbitraryNodeExpansionFlatten() throws Exception {
        // make sure this works when references/referenceExpressions are/aren't included
        ASTJexlScript origQueryTree = JexlASTHelper
                        .parseJexlQuery("UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind' || BIRTH_DATE == '123')");
        ASTJexlScript derefQueryTree = (ASTJexlScript) DereferencingVisitor.dereference(origQueryTree);

        for (ASTJexlScript queryTree : Arrays.asList(origQueryTree, derefQueryTree)) {
            ShardQueryConfiguration config = Mockito.mock(ShardQueryConfiguration.class);
            MetadataHelper helper = Mockito.mock(MetadataHelper.class);

            HashSet<String> indexedFields = new HashSet<>();
            indexedFields.add("UUID");
            indexedFields.add("QUOTE");

            Mockito.doReturn(indexedFields).when(config).getIndexedFields();

            // find an orNode in the tree
            ExecutableExpansionVisitor visitor = new ExecutableExpansionVisitor(config, helper);
            ASTJexlScript rebuilt = TreeFlatteningRebuildingVisitor.flatten(queryTree);
            rebuilt.jjtGetChild(0).jjtAccept(visitor, null);

            String expected = "(QUOTE == 'kind' && UUID == 'capone') || ((filter:includeRegex(QUOTE, '.*kind.*') || BIRTH_DATE == '123') && UUID == 'capone')";
            assertEquals(expected, JexlStringBuildingVisitor.buildQuery(rebuilt));
        }
    }

    @Test
    public void testNestedExpansionWithFailures() throws Exception {
        // make sure this works when references/referenceExpressions are/aren't included
        ASTJexlScript origQueryTree = JexlASTHelper.parseJexlQuery(
                        "UUID == 'A' && (QUOTE == 'kind' || BIRTH_DATE == '234'|| (BIRTH_DATE == '123' && QUOTE == 'kind' && !(filter:includeRegex(QUOTE, '.*unkind.*') || BIRTH_DATE =='555' )))");
        ASTJexlScript derefQueryTree = (ASTJexlScript) DereferencingVisitor.dereference(origQueryTree);

        for (ASTJexlScript queryTree : Arrays.asList(origQueryTree, derefQueryTree)) {
            ShardQueryConfiguration config = Mockito.mock(ShardQueryConfiguration.class);
            MetadataHelper helper = Mockito.mock(MetadataHelper.class);

            HashSet<String> indexedFields = new HashSet<>();
            indexedFields.add("UUID");
            indexedFields.add("QUOTE");

            Mockito.doReturn(indexedFields).when(config).getIndexedFields();

            // find an orNode in the tree
            ExecutableExpansionVisitor visitor = new ExecutableExpansionVisitor(config, helper);
            ASTJexlScript rebuilt = TreeFlatteningRebuildingVisitor.flatten(queryTree);
            rebuilt.jjtGetChild(0).jjtAccept(visitor, null);

            assertTrue(ExecutableDeterminationVisitor.isExecutable(rebuilt, config, helper));
            String expected = "(QUOTE == 'kind' && UUID == 'A') || (BIRTH_DATE == '123' && QUOTE == 'kind' && !(filter:includeRegex(QUOTE, '.*unkind.*') || BIRTH_DATE == '555') && UUID == 'A') || (BIRTH_DATE == '234' && UUID == 'A')";
            assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(rebuilt));
        }
    }

    @Test
    public void testExceededThresholdExpansionExternal() throws Exception {
        // make sure this works when references/referenceExpressions are/aren't included
        ASTJexlScript origQueryTree = JexlASTHelper
                        .parseJexlQuery("UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind' || BIRTH_DATE == '123')");
        ASTJexlScript derefQueryTree = (ASTJexlScript) DereferencingVisitor.dereference(origQueryTree);

        for (ASTJexlScript queryTree : Arrays.asList(origQueryTree, derefQueryTree)) {
            // update the generated queryTree to have an ExceededThreshold marker
            JexlNode child = QueryPropertyMarker.create(queryTree.jjtGetChild(0).jjtGetChild(0), EXCEEDED_VALUE);
            // unlink the old node
            queryTree.jjtGetChild(0).jjtGetChild(0).jjtSetParent(null);
            // overwrite the old UUID==capone with the ExceededThreshold marker
            queryTree.jjtGetChild(0).jjtAddChild(child, 0);

            ShardQueryConfiguration config = Mockito.mock(ShardQueryConfiguration.class);
            MetadataHelper helper = Mockito.mock(MetadataHelper.class);

            HashSet<String> indexedFields = new HashSet<>();
            indexedFields.add("UUID");
            indexedFields.add("QUOTE");

            Mockito.doReturn(indexedFields).when(config).getIndexedFields();

            ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);

            // included ExceededValueThresholdMarker before
            assertTrue(JexlStringBuildingVisitor.buildQuery(queryTree).equals(
                            "((_Value_ = true) && (UUID == 'capone')) && (filter:includeRegex(QUOTE, '.*kind.*') || QUOTE == 'kind' || BIRTH_DATE == '123')"),
                            JexlStringBuildingVisitor.buildQuery(queryTree));

            // not executable
            assertFalse(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
            // what came out is executable
            assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
            // it looks like what we'd expect
            String expected = "(QUOTE == 'kind' && ((_Value_ = true) && (UUID == 'capone'))) || "
                            + "((filter:includeRegex(QUOTE, '.*kind.*') || BIRTH_DATE == '123') && ((_Value_ = true) && (UUID == 'capone')))";
            assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(newTree));
        }
    }

    @Test
    public void testExceededThresholdExpansionInternal() throws Exception {
        // make sure this works when references/referenceExpressions are/aren't included
        ASTJexlScript origQueryTree = JexlASTHelper
                        .parseJexlQuery("UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind' || BIRTH_DATE == '123')");
        ASTJexlScript derefQueryTree = (ASTJexlScript) DereferencingVisitor.dereference(origQueryTree);

        // update the generated queryTree to have an ExceededThreshold marker for BIRTH_DATE
        JexlNode child = QueryPropertyMarker.create(origQueryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).jjtGetChild(1), EXCEEDED_VALUE);
        // unlink the old node
        origQueryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).jjtGetChild(1).jjtSetParent(null);
        // overwrite the old BIRTH_DATE==123 with the ExceededThreshold marker
        origQueryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).jjtAddChild(child, 1);

        // update the generated queryTree to have an ExceededThreshold marker for BIRTH_DATE
        child = QueryPropertyMarker.create(derefQueryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(1), EXCEEDED_VALUE);
        // unlink the old node
        derefQueryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(1).jjtSetParent(null);
        // overwrite the old BIRTH_DATE==123 with the ExceededThreshold marker
        derefQueryTree.jjtGetChild(0).jjtGetChild(1).jjtAddChild(child, 1);

        for (ASTJexlScript queryTree : Arrays.asList(origQueryTree, derefQueryTree)) {
            ShardQueryConfiguration config = Mockito.mock(ShardQueryConfiguration.class);
            MetadataHelper helper = Mockito.mock(MetadataHelper.class);

            HashSet<String> indexedFields = new HashSet<>();
            indexedFields.add("UUID");
            indexedFields.add("QUOTE");

            Mockito.doReturn(indexedFields).when(config).getIndexedFields();

            ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);

            // included ExceededValueThresholdMarker before
            assertTrue(JexlStringBuildingVisitor.buildQuery(queryTree)
                            .equals("UUID == 'capone' && (filter:includeRegex(QUOTE, '.*kind.*') || QUOTE == 'kind' || "
                                            + "((_Value_ = true) && (BIRTH_DATE == '123')))"),
                            JexlStringBuildingVisitor.buildQuery(queryTree));

            // not executable
            assertFalse(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
            // what came out is executable
            assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
            // it looks like what we'd expect
            String expected = "(QUOTE == 'kind' && UUID == 'capone') || " + "(((_Value_ = true) && (BIRTH_DATE == '123')) && UUID == 'capone') || "
                            + "(filter:includeRegex(QUOTE, '.*kind.*') && UUID == 'capone')";
            assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(newTree));
        }
    }

    @Test
    public void testExceededOrThresholdExpansion() throws Exception {
        // make sure this works when references/referenceExpressions are/aren't included
        ASTJexlScript origQueryTree = JexlASTHelper
                        .parseJexlQuery("UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind' || BIRTH_DATE == '123')");
        ASTJexlScript derefQueryTree = (ASTJexlScript) DereferencingVisitor.dereference(origQueryTree);

        // update the generated queryTree to have an ExceededOrThreshold marker for BIRTH_DATE
        SortedSet<String> birthdates = new TreeSet<>();
        birthdates.add("123");
        birthdates.add("234");
        birthdates.add("345");
        JexlNode child = QueryPropertyMarker.create(new ExceededOr("BIRTH_DATE", birthdates).getJexlNode(), EXCEEDED_OR);

        // unlink the old node
        origQueryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).jjtGetChild(1).jjtSetParent(null);
        // overwrite the old BIRTH_DATE==123 with the ExceededThreshold marker
        origQueryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).jjtAddChild(child, 1);

        // unlink the old node
        derefQueryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(1).jjtSetParent(null);
        // overwrite the old BIRTH_DATE==123 with the ExceededThreshold marker
        derefQueryTree.jjtGetChild(0).jjtGetChild(1).jjtAddChild(child, 1);

        for (ASTJexlScript queryTree : Arrays.asList(origQueryTree, derefQueryTree)) {
            ShardQueryConfiguration config = Mockito.mock(ShardQueryConfiguration.class);
            MetadataHelper helper = Mockito.mock(MetadataHelper.class);

            HashSet<String> indexedFields = new HashSet<>();
            indexedFields.add("UUID");
            indexedFields.add("QUOTE");

            Mockito.doReturn(indexedFields).when(config).getIndexedFields();

            ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);

            String queryString = JexlStringBuildingVisitor.buildQuery(queryTree);
            String id = queryString.substring(queryString.indexOf("id = '") + 6, queryString.indexOf("') && (field"));

            // included ExceededValueThresholdMarker before
            assertTrue(queryString.equals("UUID == 'capone' && (filter:includeRegex(QUOTE, '.*kind.*') || QUOTE == 'kind' || " + "((_List_ = true) && ((id = '"
                            + id + "') && (field = 'BIRTH_DATE') && (params = '{\"values\":[\"123\",\"234\",\"345\"]}'))))"), queryString);

            // not executable
            assertFalse(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
            // what came out is executable
            assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));

            queryString = JexlStringBuildingVisitor.buildQuery(newTree);
            id = queryString.substring(queryString.indexOf("id = '") + 6, queryString.indexOf("') && (field"));

            // it looks like what we'd expect

            String expected = "(QUOTE == 'kind' && UUID == 'capone') || " + "(((_List_ = true) && ((id = '" + id
                            + "') && (field = 'BIRTH_DATE') && (params = '{\"values\":[\"123\",\"234\",\"345\"]}'))) && UUID == 'capone') || "
                            + "(filter:includeRegex(QUOTE, '.*kind.*') && UUID == 'capone')";
            assertEquals(expected, queryString);
        }
    }

    @Test
    public void testExceededOrThresholdCannotExpand() throws Exception {
        // make sure this works when references/referenceExpressions are/aren't included
        ASTJexlScript origQueryTree = JexlASTHelper.parseJexlQuery(
                        "UUID == 'capone' && (((_List_ = true) && (((id = 'some-bogus-id') && (field = 'QUOTE') && (params = '{\"values\":[\"a\",\"b\",\"c\"]}')))))");
        ASTJexlScript derefQueryTree = (ASTJexlScript) DereferencingVisitor.dereference(origQueryTree);

        for (ASTJexlScript queryTree : Arrays.asList(origQueryTree, derefQueryTree)) {
            ShardQueryConfiguration config = Mockito.mock(ShardQueryConfiguration.class);
            MetadataHelper helper = Mockito.mock(MetadataHelper.class);

            HashSet<String> indexedFields = new HashSet<>();
            indexedFields.add("UUID");
            indexedFields.add("QUOTE");

            Mockito.doReturn(indexedFields).when(config).getIndexedFields();

            ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);

            if (queryTree == origQueryTree) {
                // included ExceededValueThresholdMarker before
                assertTrue(JexlStringBuildingVisitor.buildQuery(queryTree).equals(
                                "UUID == 'capone' && (((_List_ = true) && (((id = 'some-bogus-id') && (field = 'QUOTE') && (params = '{\"values\":[\"a\",\"b\",\"c\"]}')))))"),
                                JexlStringBuildingVisitor.buildQuery(queryTree));
            } else {
                // included ExceededValueThresholdMarker before
                assertTrue(JexlStringBuildingVisitor.buildQuery(queryTree).equals(
                                "UUID == 'capone' && (_List_ = true) && (id = 'some-bogus-id') && (field = 'QUOTE') && (params = '{\"values\":[\"a\",\"b\",\"c\"]}')"),
                                JexlStringBuildingVisitor.buildQuery(queryTree));
            }

            // starts off executable
            assertTrue(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
            // what came out is executable
            assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
            // the visitor changed nothing
            assertTrue(JexlStringBuildingVisitor.buildQuery(newTree).equals(JexlStringBuildingVisitor.buildQuery(queryTree)),
                            JexlStringBuildingVisitor.buildQuery(newTree));
        }
    }

    @Test
    public void testDelayed() throws Exception {
        // make sure this works when references/referenceExpressions are/aren't included
        ASTJexlScript origQueryTree = JexlASTHelper.parseJexlQuery("UUID == 'capone' && (QUOTE == 'kind' || " + "((_Delayed_ = true) && BIRTH_DATE == '123'))");
        ASTJexlScript derefQueryTree = (ASTJexlScript) DereferencingVisitor.dereference(origQueryTree);

        for (ASTJexlScript queryTree : Arrays.asList(origQueryTree, derefQueryTree)) {
            ShardQueryConfiguration config = Mockito.mock(ShardQueryConfiguration.class);
            MetadataHelper helper = Mockito.mock(MetadataHelper.class);

            HashSet<String> indexedFields = new HashSet<>();
            indexedFields.add("UUID");
            indexedFields.add("QUOTE");

            Mockito.doReturn(indexedFields).when(config).getIndexedFields();
            Set<String> dataTypes = new HashSet<>();
            dataTypes.add("test");
            Set<String> nonEventFields = new HashSet<>();
            nonEventFields.add("QUOTE");
            Mockito.doReturn(dataTypes).when(config).getDatatypeFilter();
            Mockito.doReturn(nonEventFields).when(helper).getNonEventFields(dataTypes);

            ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);

            // included ExceededValueThresholdMarker before
            assertTrue(JexlStringBuildingVisitor.buildQuery(queryTree)
                            .equals("UUID == 'capone' && (QUOTE == 'kind' || " + "((_Delayed_ = true) && BIRTH_DATE == '123'))"),
                            JexlStringBuildingVisitor.buildQuery(queryTree));

            // starts off executable
            assertFalse(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
            // what came out is executable
            assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
            // the visitor changed nothing
            String expected = "(QUOTE == 'kind' && UUID == 'capone') || (((_Delayed_ = true) && BIRTH_DATE == '123') && UUID == 'capone')";
            assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(newTree));
        }
    }

    @Test
    public void testDelayedDoubleExpansion() throws Exception {
        // make sure this works when references/referenceExpressions are/aren't included
        ASTJexlScript origQueryTree = JexlASTHelper
                        .parseJexlQuery("UUID == 'capone' && (((_Delayed_ = true) && QUOTE == 'kind') || " + "((_Delayed_ = true) && BIRTH_DATE == '123'))");
        ASTJexlScript derefQueryTree = (ASTJexlScript) DereferencingVisitor.dereference(origQueryTree);

        for (ASTJexlScript queryTree : Arrays.asList(origQueryTree, derefQueryTree)) {
            ShardQueryConfiguration config = Mockito.mock(ShardQueryConfiguration.class);
            MetadataHelper helper = Mockito.mock(MetadataHelper.class);

            HashSet<String> indexedFields = new HashSet<>();
            indexedFields.add("UUID");
            indexedFields.add("QUOTE");

            Mockito.doReturn(indexedFields).when(config).getIndexedFields();
            Set<String> dataTypes = new HashSet<>();
            dataTypes.add("test");
            // QUOTE being delayed creates a query that is non-executable we cannot delay a field which is nonEvent
            Set<String> nonEventFields = new HashSet<>();
            nonEventFields.add("QUOTE");
            Mockito.doReturn(dataTypes).when(config).getDatatypeFilter();
            Mockito.doReturn(nonEventFields).when(helper).getNonEventFields(dataTypes);

            ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);

            // included ExceededValueThresholdMarker before
            assertTrue(JexlStringBuildingVisitor.buildQuery(queryTree)
                            .equals("UUID == 'capone' && (((_Delayed_ = true) && QUOTE == 'kind') || " + "((_Delayed_ = true) && BIRTH_DATE == '123'))"),
                            JexlStringBuildingVisitor.buildQuery(queryTree));

            // starts off executable
            assertTrue(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
            // what came out is executable
            assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
            // the visitor changed nothing
            assertTrue(JexlStringBuildingVisitor.buildQuery(newTree).equals(JexlStringBuildingVisitor.buildQuery(queryTree)),
                            JexlStringBuildingVisitor.buildQuery(newTree));
        }
    }

    @Test
    public void testSingleOr() throws Exception {
        // make sure this works when references/referenceExpressions are/aren't included
        ASTJexlScript origQueryTree = JexlASTHelper.parseJexlQuery("UUID == 'capone' && (QUOTE =='kind' || BIRTH_DATE == '123')");
        ASTJexlScript derefQueryTree = (ASTJexlScript) DereferencingVisitor.dereference(origQueryTree);

        for (ASTJexlScript queryTree : Arrays.asList(origQueryTree, derefQueryTree)) {
            // grab the reference to the QUOTE=='kind' eqnode
            JexlNode quoteNode = queryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).jjtGetChild(0);
            JexlNode origOrNode = queryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0);
            ASTOrNode newOr = new ASTOrNode(ParserTreeConstants.JJTORNODE);
            newOr.jjtAddChild(quoteNode, 0);
            quoteNode.jjtSetParent(newOr);

            // attach the new or node
            origOrNode.jjtGetParent().jjtAddChild(newOr, 0);
            newOr.jjtSetParent(origOrNode.jjtGetParent());

            ShardQueryConfiguration config = Mockito.mock(ShardQueryConfiguration.class);
            MetadataHelper helper = Mockito.mock(MetadataHelper.class);

            HashSet<String> indexedFields = new HashSet<>();
            indexedFields.add("UUID");
            indexedFields.add("QUOTE");

            Mockito.doReturn(indexedFields).when(config).getIndexedFields();

            ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);

            // starts executable
            assertTrue(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
            // what came out is executable
            assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
            // the visitor changed nothing
            assertTrue(JexlStringBuildingVisitor.buildQuery(newTree).equals(JexlStringBuildingVisitor.buildQuery(queryTree)),
                            JexlStringBuildingVisitor.buildQuery(newTree));
        }
    }

    @Test
    public void testSingleOrNonExecutableCantFix() throws Exception {
        // make sure this works when references/referenceExpressions are/aren't included
        ASTJexlScript origQueryTree = JexlASTHelper.parseJexlQuery("BIRTH_DATE =='123' && (filter:includeRegex(QUOTE, '.*kind.*') || BIRTH_DATE == '234')");
        ASTJexlScript derefQueryTree = (ASTJexlScript) DereferencingVisitor.dereference(origQueryTree);

        for (ASTJexlScript queryTree : Arrays.asList(origQueryTree, derefQueryTree)) {
            // grab the reference to the QUOTE=='kind' eqnode
            JexlNode quoteNode;
            if (queryTree == origQueryTree) {
                quoteNode = queryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).jjtGetChild(0);
            } else {
                quoteNode = queryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0);
            }
            ASTOrNode newOr = new ASTOrNode(ParserTreeConstants.JJTORNODE);
            newOr.jjtAddChild(quoteNode, 0);
            quoteNode.jjtSetParent(newOr);

            // attach the new or node
            queryTree.jjtGetChild(0).jjtGetChild(1).jjtAddChild(newOr, 0);
            newOr.jjtSetParent(queryTree.jjtGetChild(0).jjtGetChild(1));

            ShardQueryConfiguration config = Mockito.mock(ShardQueryConfiguration.class);
            MetadataHelper helper = Mockito.mock(MetadataHelper.class);

            HashSet<String> indexedFields = new HashSet<>();
            indexedFields.add("UUID");
            indexedFields.add("QUOTE");

            Mockito.doReturn(indexedFields).when(config).getIndexedFields();

            ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);

            // starts executable
            assertFalse(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
            // what came out is executable
            assertFalse(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
            // the visitor changed nothing
            assertTrue(JexlStringBuildingVisitor.buildQuery(newTree).equals(JexlStringBuildingVisitor.buildQuery(queryTree)),
                            JexlStringBuildingVisitor.buildQuery(newTree));
        }
    }

    @Test
    public void testNoReferenceOrReferenceExpressions() throws Exception {
        // make sure this works when references/referenceExpressions are/aren't included
        ASTJexlScript origQueryTree = JexlASTHelper
                        .parseJexlQuery("UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind' || BIRTH_DATE == '123')");
        ASTJexlScript derefQueryTree = (ASTJexlScript) DereferencingVisitor.dereference(origQueryTree);

        for (ASTJexlScript queryTree : Arrays.asList(origQueryTree, derefQueryTree)) {
            // strip reference/referenceExpressions
            ASTJexlScript flattened = TreeFlatteningRebuildingVisitor.flattenAll(queryTree);

            ShardQueryConfiguration config = Mockito.mock(ShardQueryConfiguration.class);
            MetadataHelper helper = Mockito.mock(MetadataHelper.class);

            HashSet<String> indexedFields = new HashSet<>();
            indexedFields.add("UUID");
            indexedFields.add("QUOTE");

            Mockito.doReturn(indexedFields).when(config).getIndexedFields();

            ASTJexlScript newTree = ExecutableExpansionVisitor.expand(flattened, config, helper);

            // starts executable
            assertFalse(ExecutableDeterminationVisitor.isExecutable(flattened, config, helper));
            // what came out is executable
            assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
            // the visitor changed nothing
            assertTrue(JexlStringBuildingVisitor.buildQuery(newTree)
                            .equals("(QUOTE == 'kind' && UUID == 'capone') || "
                                            + "((filter:includeRegex(QUOTE, '.*kind.*') || BIRTH_DATE == '123') && UUID == 'capone')"),
                            JexlStringBuildingVisitor.buildQuery(newTree));
        }
    }

    private static class DereferencingVisitor extends RebuildingVisitor {

        private boolean derefExprOrs = true;
        private boolean derefExprAnds = true;
        private boolean derefOrs = false;
        private boolean derefAnds = false;

        private DereferencingVisitor() {

        }

        private DereferencingVisitor(boolean derefExprOrs, boolean derefExprAnds, boolean derefOrs, boolean derefAnds) {
            this.derefExprOrs = derefExprOrs;
            this.derefExprAnds = derefExprAnds;
            this.derefOrs = derefOrs;
            this.derefAnds = derefAnds;
        }

        public static JexlNode dereference(JexlNode node) {
            DereferencingVisitor visitor = new DereferencingVisitor();
            return (JexlNode) node.jjtAccept(visitor, null);
        }

        public static JexlNode dereference(JexlNode node, boolean derefExprOrs, boolean derefExprAnds, boolean derefOrs, boolean derefAnds) {
            DereferencingVisitor visitor = new DereferencingVisitor(derefExprOrs, derefExprAnds, derefOrs, derefAnds);
            return (JexlNode) node.jjtAccept(visitor, null);
        }

        @Override
        public Object visit(ASTReference node, Object data) {
            JexlNode finalNode = node;
            if (node.jjtGetNumChildren() == 1) {
                JexlNode child = node.jjtGetChild(0);

                if ((derefOrs && child instanceof ASTOrNode) || (derefAnds && child instanceof ASTAndNode)) {
                    finalNode = child;
                } else if (child instanceof ASTReferenceExpression && child.jjtGetNumChildren() == 1) {
                    JexlNode grandchild = child.jjtGetChild(0);
                    JexlNode derefGrandchild = JexlASTHelper.dereference(grandchild);
                    if ((derefOrs && derefGrandchild instanceof ASTOrNode) || (derefAnds && derefGrandchild instanceof ASTAndNode)) {
                        finalNode = child;
                    }
                }
            }

            if (finalNode != node) {
                finalNode = (JexlNode) finalNode.jjtAccept(this, data);
            } else {
                finalNode = (JexlNode) super.visit(node, data);
            }

            return finalNode;
        }

        @Override
        public Object visit(ASTReferenceExpression node, Object data) {
            JexlNode finalNode = node;
            if (node.jjtGetNumChildren() == 1) {
                JexlNode child = node.jjtGetChild(0);
                JexlNode derefChild = JexlASTHelper.dereference(child);
                if ((derefExprOrs && derefChild instanceof ASTOrNode) || (derefExprAnds && derefChild instanceof ASTAndNode)) {
                    finalNode = child;
                }
            }

            if (finalNode != node) {
                finalNode = (JexlNode) finalNode.jjtAccept(this, data);
            } else {
                finalNode = (JexlNode) super.visit(node, data);
            }

            return finalNode;
        }
    }
}
