package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_VALUE;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.apache.log4j.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import datawave.configuration.spring.SpringBean;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.data.type.GeometryType;
import datawave.data.type.Type;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.microservice.query.QueryImpl;
import datawave.query.CompositeFunctionsTest;
import datawave.query.QueryTestTableHelper;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.attributes.TypeAttribute;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.ExceededOr;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MockMetadataHelper;
import datawave.query.util.WiseGuysIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;

public abstract class ExecutableExpansionVisitorTest {
    @RunWith(Arquillian.class)
    public static class ShardRangeExecutableExpansion extends ExecutableExpansionVisitorTest {
        protected static AccumuloClient client = null;

        @BeforeClass
        public static void setUp() throws Exception {

            QueryTestTableHelper qtth = new QueryTestTableHelper(CompositeFunctionsTest.ShardRange.class.toString(), log);
            client = qtth.client;

            WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.SHARD);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(client, auths, TableName.SHARD);
            PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.METADATA_TABLE_NAME);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms)
                        throws ParseException, Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, client);
        }
    }

    @RunWith(Arquillian.class)
    public static class DocumentRange extends ExecutableExpansionVisitorTest {
        protected static AccumuloClient client = null;

        @BeforeClass
        public static void setUp() throws Exception {

            QueryTestTableHelper qtth = new QueryTestTableHelper(CompositeFunctionsTest.DocumentRange.class.toString(), log);
            client = qtth.client;

            WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.DOCUMENT);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(client, auths, TableName.SHARD);
            PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.METADATA_TABLE_NAME);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }

        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms)
                        throws ParseException, Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, client);
        }
    }

    private static final Logger log = Logger.getLogger(ExecutableExpansionVisitorTest.class);

    protected Authorizations auths = new Authorizations("ALL");

    protected Set<Authorizations> authSet = Collections.singleton(auths);

    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;

    protected KryoDocumentDeserializer deserializer;

    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");

    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        // basically this is avoiding WELD exceptions for beans that cannot be created because of various injection points
        // that would be null in this environment. For example the QueryMetricQueryLogic would fail to be created because
        // we do not have a caller principal, and the io.astefunutti.metrics.cdi classes are not available to this test case.
        // Also we are adding the beans.xml to enable resolving some beans. The MockAlternative is a placeholder for any
        // mock bean alternatives we may use if annotated as such.
        return ShrinkWrap.create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class).deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .deleteClass(datawave.query.tables.edge.DefaultEdgeEventQueryLogic.class)
                        .addAsManifestResource(new StringAsset(
                                        "<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" + "</alternatives>"),
                                        "beans.xml");
    }

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }

    @Before
    public void setup() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        logic.setFullTableScanEnabled(false);
        logic.setMaxDepthThreshold(11);
        logic.setInitialMaxTermThreshold(12);
        logic.setFinalMaxTermThreshold(12);
        deserializer = new KryoDocumentDeserializer();
    }

    protected abstract void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms)
                    throws ParseException, Exception;

    protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms, AccumuloClient client)
                    throws ParseException, Exception {
        log.debug("runTestQuery");
        log.trace("Creating QueryImpl");
        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(startDate);
        settings.setEndDate(endDate);
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(querystr);
        settings.setParameters(extraParms);
        settings.setId(UUID.randomUUID());

        log.debug("query: " + settings.getQuery());
        log.debug("logic: " + settings.getQueryLogicName());

        GenericQueryConfiguration config = logic.initialize(client, settings, authSet);
        logic.setupQuery(config);

        HashSet<String> expectedSet = new HashSet<String>(expected);
        HashSet<String> resultSet;
        resultSet = new HashSet<String>();
        Set<Document> docs = new HashSet<Document>();
        for (Map.Entry<Key,Value> entry : logic) {
            Document d = deserializer.apply(entry).getValue();

            log.trace(entry.getKey() + " => " + d);

            Attribute<?> attr = d.get("UUID");
            if (attr == null)
                attr = d.get("UUID.0");

            Assert.assertNotNull("Result Document did not contain a 'UUID'", attr);
            Assert.assertTrue("Expected result to be an instance of DatwawaveTypeAttribute, was: " + attr.getClass().getName(),
                            attr instanceof TypeAttribute || attr instanceof PreNormalizedAttribute);

            TypeAttribute<?> UUIDAttr = (TypeAttribute<?>) attr;

            String UUID = UUIDAttr.getType().getDelegate().toString();
            Assert.assertTrue("Received unexpected UUID: " + UUID, expected.contains(UUID));

            resultSet.add(UUID);
            docs.add(d);
        }

        if (expected.size() > resultSet.size()) {
            expectedSet.addAll(expected);
            expectedSet.removeAll(resultSet);

            for (String s : expectedSet) {
                log.warn("Missing: " + s);
            }
        }

        if (!expected.containsAll(resultSet)) {
            log.error("Expected results " + expected + " differ form actual results " + resultSet);
        }
        Assert.assertTrue("Expected results " + expected + " differ form actual results " + resultSet, expected.containsAll(resultSet));
        Assert.assertEquals("Unexpected number of records", expected.size(), resultSet.size());
    }

    @Test(expected = DatawaveFatalQueryException.class)
    public void testMixedIndexOnly() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        if (log.isDebugEnabled()) {
            log.debug("testMatchesAtLeastCountOf");
        }
        String[] queryStrings = {"filter:isNull(LOCATION) || LOCATION == 'chicago'"};
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList(), Arrays.asList()};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
            Assert.fail("should not have gotten here");
        }
    }

    @Test
    public void testExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        if (log.isDebugEnabled()) {
            log.debug("testMatchesAtLeastCountOf");
        }
        String[] queryStrings = {"UUID == 'capone' && (filter:isNull(MAGIC) || LOCATION == 'chicago')"};
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CAPONE"), Arrays.asList()};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }

    @Test
    public void testGeowaveExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        if (log.isDebugEnabled()) {
            log.debug("testGeowaveExpansion");
        }

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
        String[] queryStrings = {expandedQuery};
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList(), Arrays.asList()};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }

        String finalQuery = JexlStringBuildingVisitor.buildQuery(logic.getConfig().getQueryTree());
        Assert.assertNotEquals(
                        "GENDER == 'male' && (NOME == 'this' || NOME == 'that') && !filter:includeRegex(ETA, 'blah') && (LOCATION == 'chicago' || LOCATION == 'newyork' || LOCATION == 'newjersey')",
                        finalQuery);

        ASTJexlScript expectedQuery = JexlASTHelper.parseJexlQuery(
                        "((((_Bounded_ = true) && (NUMBER >= '0' && NUMBER <= '1000')) && geowave:intersects(GEO, 'POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))') && (GEO == '00' || GEO == '0202' || GEO == '020b' || GEO == '1f202a02a02a02a02a' || GEO == '1f2088888888888888' || GEO == '1f200a80a80a80a80a') && (GEO == '00' || GEO == '0202' || GEO == '020b' || GEO == '1f202a02a02a02a02a' || GEO == '1f2088888888888888' || GEO == '1f200a80a80a80a80a')) || (((_Bounded_ = true) && (NUMBER >= '0' && NUMBER <= '1000')) && geowave:intersects(GEO, 'POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))') && (GEO == '00' || GEO == '0202' || GEO == '020b' || GEO == '1f202a02a02a02a02a' || GEO == '1f2088888888888888' || GEO == '1f200a80a80a80a80a') && (GEO == '00' || GEO == '0202' || GEO == '020b' || GEO == '1f202a02a02a02a02a' || GEO == '1f2088888888888888' || GEO == '1f200a80a80a80a80a'))) && GENDER == 'male' && (NOME == 'this' || NOME == 'that') && !filter:includeRegex(ETA, 'blah') && (LOCATION == 'chicago' || LOCATION == 'newyork' || LOCATION == 'newjersey')");
        Assert.assertTrue(TreeEqualityVisitor.isEqual(expectedQuery, logic.getConfig().getQueryTree()));
    }

    @Test
    public void testNestedOrExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        if (log.isDebugEnabled()) {
            log.debug("testMatchesAtLeastCountOf");
        }
        String[] queryStrings = {"UUID == 'capone' && ( LOCATION == 'newyork' || (filter:isNull(MAGIC) || LOCATION == 'chicago'))"};
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CAPONE"), Arrays.asList()};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }

    @Test
    public void testMethodNoExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        if (log.isDebugEnabled()) {
            log.debug("testMatchesAtLeastCountOf");
        }
        String[] queryStrings = {"UUID == 'capone' && ( AGE.size() > 1 || AGE == '18')"};
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CAPONE"), Arrays.asList()};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }

    @Test
    public void testMethodExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        if (log.isDebugEnabled()) {
            log.debug("testMatchesAtLeastCountOf");
        }
        String[] queryStrings = {"UUID == 'capone' && ( QUOTE.size() == 1 || QUOTE == 'kind')"};
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CAPONE"), Arrays.asList()};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }

    @Test
    public void testNonEventExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        if (log.isDebugEnabled()) {
            log.debug("testMatchesAtLeastCountOf");
        }
        String[] queryStrings = {"UUID == 'capone' && (QUOTE == 'kind'|| BIRTH_DATE == '123')"};
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CAPONE"), Arrays.asList()};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }

    @Test
    public void testFilterExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        if (log.isDebugEnabled()) {
            log.debug("testMatchesAtLeastCountOf");
        }
        String[] queryStrings = {"UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind')"};
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CAPONE"), Arrays.asList()};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }

    @Test
    public void testDisableExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        ((DefaultQueryPlanner) logic.getQueryPlanner()).setExecutableExpansion(false);

        if (log.isDebugEnabled()) {
            log.debug("testMatchesAtLeastCountOf");
        }
        String[] queryStrings = {"UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind')"};
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CAPONE")};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }

    @Test
    public void testDelayedBridgeExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        if (log.isDebugEnabled()) {
            log.debug("testMatchesAtLeastCountOf");
        }
        String[] queryStrings = {"UUID == 'capone' && ( LOCATION == 'newyork' || !(filter:isNull(MAGIC) || LOCATION == 'chicago'))",
                "UUID == 'capone' && ( LOCATION == 'newyork' || !filter:isNull(MAGIC) || LOCATION == 'chicago')"};
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList(), Arrays.asList("CAPONE")};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }

    @Test
    public void testMultipleExpansionsRequired() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        logic.setIntermediateMaxTermThreshold(15);

        if (log.isDebugEnabled()) {
            log.debug("testMatchesAtLeastCountOf");
        }
        String[] queryStrings = {"UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind') && (QUOTE == 'kind'|| BIRTH_DATE == '123')"};
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CAPONE"), Arrays.asList()};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }

    @Test
    public void testMinimumExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        if (log.isDebugEnabled()) {
            log.debug("testMatchesAtLeastCountOf");
        }
        String[] queryStrings = {"UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind' || BIRTH_DATE == '123')"};
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CAPONE"), Arrays.asList()};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }

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

            Assert.assertFalse(JexlStringBuildingVisitor.buildQuery(queryTree).equals(JexlStringBuildingVisitor.buildQuery(newTree)));
            String expected = "(QUOTE == 'kind' && UUID == 'capone') || ((filter:includeRegex(QUOTE, '.*kind.*') || BIRTH_DATE == '123') && UUID == 'capone')";
            Assert.assertEquals(expected, JexlStringBuildingVisitor.buildQuery(newTree));
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

            Assert.assertFalse(data instanceof ExecutableExpansionVisitor.ExpansionTracker);
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
            Assert.assertEquals(expected, JexlStringBuildingVisitor.buildQuery(rebuilt));
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

            Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(rebuilt, config, helper));
            String expected = "(QUOTE == 'kind' && UUID == 'A') || (BIRTH_DATE == '123' && QUOTE == 'kind' && !(filter:includeRegex(QUOTE, '.*unkind.*') || BIRTH_DATE == '555') && UUID == 'A') || (BIRTH_DATE == '234' && UUID == 'A')";
            Assert.assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(rebuilt));
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
            Assert.assertTrue(JexlStringBuildingVisitor.buildQuery(queryTree), JexlStringBuildingVisitor.buildQuery(queryTree).equals(
                            "((_Value_ = true) && (UUID == 'capone')) && (filter:includeRegex(QUOTE, '.*kind.*') || QUOTE == 'kind' || BIRTH_DATE == '123')"));

            // not executable
            Assert.assertFalse(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
            // what came out is executable
            Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
            // it looks like what we'd expect
            String expected = "(QUOTE == 'kind' && ((_Value_ = true) && (UUID == 'capone'))) || "
                            + "((filter:includeRegex(QUOTE, '.*kind.*') || BIRTH_DATE == '123') && ((_Value_ = true) && (UUID == 'capone')))";
            Assert.assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(newTree));
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
            Assert.assertTrue(JexlStringBuildingVisitor.buildQuery(queryTree),
                            JexlStringBuildingVisitor.buildQuery(queryTree)
                                            .equals("UUID == 'capone' && (filter:includeRegex(QUOTE, '.*kind.*') || QUOTE == 'kind' || "
                                                            + "((_Value_ = true) && (BIRTH_DATE == '123')))"));

            // not executable
            Assert.assertFalse(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
            // what came out is executable
            Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
            // it looks like what we'd expect
            String expected = "(QUOTE == 'kind' && UUID == 'capone') || " + "(((_Value_ = true) && (BIRTH_DATE == '123')) && UUID == 'capone') || "
                            + "(filter:includeRegex(QUOTE, '.*kind.*') && UUID == 'capone')";
            Assert.assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(newTree));
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
            Assert.assertTrue(queryString, queryString.equals("UUID == 'capone' && (filter:includeRegex(QUOTE, '.*kind.*') || QUOTE == 'kind' || "
                            + "((_List_ = true) && ((id = '" + id + "') && (field = 'BIRTH_DATE') && (params = '{\"values\":[\"123\",\"234\",\"345\"]}'))))"));

            // not executable
            Assert.assertFalse(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
            // what came out is executable
            Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));

            queryString = JexlStringBuildingVisitor.buildQuery(newTree);
            id = queryString.substring(queryString.indexOf("id = '") + 6, queryString.indexOf("') && (field"));

            // it looks like what we'd expect

            String expected = "(QUOTE == 'kind' && UUID == 'capone') || " + "(((_List_ = true) && ((id = '" + id
                            + "') && (field = 'BIRTH_DATE') && (params = '{\"values\":[\"123\",\"234\",\"345\"]}'))) && UUID == 'capone') || "
                            + "(filter:includeRegex(QUOTE, '.*kind.*') && UUID == 'capone')";
            Assert.assertEquals(expected, queryString);
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
                Assert.assertTrue(JexlStringBuildingVisitor.buildQuery(queryTree), JexlStringBuildingVisitor.buildQuery(queryTree).equals(
                                "UUID == 'capone' && (((_List_ = true) && (((id = 'some-bogus-id') && (field = 'QUOTE') && (params = '{\"values\":[\"a\",\"b\",\"c\"]}')))))"));
            } else {
                // included ExceededValueThresholdMarker before
                Assert.assertTrue(JexlStringBuildingVisitor.buildQuery(queryTree), JexlStringBuildingVisitor.buildQuery(queryTree).equals(
                                "UUID == 'capone' && (_List_ = true) && (id = 'some-bogus-id') && (field = 'QUOTE') && (params = '{\"values\":[\"a\",\"b\",\"c\"]}')"));
            }

            // starts off executable
            Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
            // what came out is executable
            Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
            // the visitor changed nothing
            Assert.assertTrue(JexlStringBuildingVisitor.buildQuery(newTree),
                            JexlStringBuildingVisitor.buildQuery(newTree).equals(JexlStringBuildingVisitor.buildQuery(queryTree)));
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
            Assert.assertTrue(JexlStringBuildingVisitor.buildQuery(queryTree), JexlStringBuildingVisitor.buildQuery(queryTree)
                            .equals("UUID == 'capone' && (QUOTE == 'kind' || " + "((_Delayed_ = true) && BIRTH_DATE == '123'))"));

            // starts off executable
            Assert.assertFalse(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
            // what came out is executable
            Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
            // the visitor changed nothing
            String expected = "(QUOTE == 'kind' && UUID == 'capone') || (((_Delayed_ = true) && BIRTH_DATE == '123') && UUID == 'capone')";
            Assert.assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(newTree));
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
            Assert.assertTrue(JexlStringBuildingVisitor.buildQuery(queryTree), JexlStringBuildingVisitor.buildQuery(queryTree)
                            .equals("UUID == 'capone' && (((_Delayed_ = true) && QUOTE == 'kind') || " + "((_Delayed_ = true) && BIRTH_DATE == '123'))"));

            // starts off executable
            Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
            // what came out is executable
            Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
            // the visitor changed nothing
            Assert.assertTrue(JexlStringBuildingVisitor.buildQuery(newTree),
                            JexlStringBuildingVisitor.buildQuery(newTree).equals(JexlStringBuildingVisitor.buildQuery(queryTree)));
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
            Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
            // what came out is executable
            Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
            // the visitor changed nothing
            Assert.assertTrue(JexlStringBuildingVisitor.buildQuery(newTree),
                            JexlStringBuildingVisitor.buildQuery(newTree).equals(JexlStringBuildingVisitor.buildQuery(queryTree)));
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
            Assert.assertFalse(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
            // what came out is executable
            Assert.assertFalse(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
            // the visitor changed nothing
            Assert.assertTrue(JexlStringBuildingVisitor.buildQuery(newTree),
                            JexlStringBuildingVisitor.buildQuery(newTree).equals(JexlStringBuildingVisitor.buildQuery(queryTree)));
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
            queryTree = TreeFlatteningRebuildingVisitor.flattenAll(queryTree);

            ShardQueryConfiguration config = Mockito.mock(ShardQueryConfiguration.class);
            MetadataHelper helper = Mockito.mock(MetadataHelper.class);

            HashSet<String> indexedFields = new HashSet<>();
            indexedFields.add("UUID");
            indexedFields.add("QUOTE");

            Mockito.doReturn(indexedFields).when(config).getIndexedFields();

            ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);

            // starts executable
            Assert.assertFalse(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
            // what came out is executable
            Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
            // the visitor changed nothing
            Assert.assertTrue(JexlStringBuildingVisitor.buildQuery(newTree),
                            JexlStringBuildingVisitor.buildQuery(newTree).equals("(QUOTE == 'kind' && UUID == 'capone') || "
                                            + "((filter:includeRegex(QUOTE, '.*kind.*') || BIRTH_DATE == '123') && UUID == 'capone')"));
        }
    }

    @Test
    public void testIndexOnlyNoType() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        String[] queryStrings = {"(UUID == 'capone' &&  SENTENCE == '11y')"};
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CAPONE"), Arrays.asList()};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }

    @Test
    public void testTypedAndNotIndexed() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        String[] queryStrings = {"(UUID == 'capone' && BIRTH_DATE == '1910-12-28T00:00:05.000Z')"};
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CAPONE"), Arrays.asList()};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
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
