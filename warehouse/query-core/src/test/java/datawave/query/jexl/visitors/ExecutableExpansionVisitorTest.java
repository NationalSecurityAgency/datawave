package datawave.query.jexl.visitors;

import datawave.configuration.spring.SpringBean;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
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
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.MetadataHelper;
import datawave.query.util.WiseGuysIngest;
import datawave.util.TableName;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
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

import javax.inject.Inject;
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
import java.util.TimeZone;
import java.util.UUID;

public abstract class ExecutableExpansionVisitorTest {
    @RunWith(Arquillian.class)
    public static class ShardRangeExecutableExpansion extends ExecutableExpansionVisitorTest {
        protected static Connector connector = null;
        
        @BeforeClass
        public static void setUp() throws Exception {
            
            QueryTestTableHelper qtth = new QueryTestTableHelper(CompositeFunctionsTest.ShardRange.class.toString(), log);
            connector = qtth.connector;
            
            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.SHARD);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.METADATA_TABLE_NAME);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }
        
        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws ParseException,
                        Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, connector);
        }
    }
    
    @RunWith(Arquillian.class)
    public static class DocumentRange extends ExecutableExpansionVisitorTest {
        protected static Connector connector = null;
        
        @BeforeClass
        public static void setUp() throws Exception {
            
            QueryTestTableHelper qtth = new QueryTestTableHelper(CompositeFunctionsTest.DocumentRange.class.toString(), log);
            connector = qtth.connector;
            
            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.DOCUMENT);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.METADATA_TABLE_NAME);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }
        
        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws ParseException,
                        Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, connector);
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
        
        return ShrinkWrap
                        .create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "nsa.datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event")
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .deleteClass(datawave.query.metrics.ShardTableQueryMetricHandler.class)
                        .deleteClass(datawave.query.tables.edge.DefaultEdgeEventQueryLogic.class)
                        .addAsManifestResource(
                                        new StringAsset("<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>"
                                                        + "</alternatives>"), "beans.xml");
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
        logic.setMaxTermThreshold(12);
        deserializer = new KryoDocumentDeserializer();
    }
    
    protected abstract void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms)
                    throws ParseException, Exception;
    
    protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms, Connector connector)
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
        
        GenericQueryConfiguration config = logic.initialize(connector, settings, authSet);
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
            Assert.assertTrue("Expected result to be an instance of DatwawaveTypeAttribute, was: " + attr.getClass().getName(), attr instanceof TypeAttribute
                            || attr instanceof PreNormalizedAttribute);
            
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
            Assert.assertTrue("should not have gotten here", 1 == 2);
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
        ASTJexlScript queryTree = JexlASTHelper
                        .parseJexlQuery("UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind' || BIRTH_DATE == '123')");
        ShardQueryConfiguration config = EasyMock.createMock(ShardQueryConfiguration.class);
        MetadataHelper helper = EasyMock.createMock(MetadataHelper.class);
        
        HashSet<String> indexedFields = new HashSet<>();
        indexedFields.add("UUID");
        indexedFields.add("QUOTE");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        
        EasyMock.replay(config, helper);
        
        ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);
        
        EasyMock.verify(config, helper);
        
        Assert.assertFalse(JexlStringBuildingVisitor.buildQuery(queryTree).equals(JexlStringBuildingVisitor.buildQuery(newTree)));
        String expected = "(QUOTE == 'kind' && UUID == 'capone') || ((filter:includeRegex(QUOTE, '.*kind.*') || BIRTH_DATE == '123') && UUID == 'capone')";
        Assert.assertEquals(expected, JexlStringBuildingVisitor.buildQuery(newTree));
    }
    
    @Test
    public void testArbitraryNodeExpansionFailNoFlatten() throws Exception {
        ASTJexlScript queryTree = JexlASTHelper
                        .parseJexlQuery("UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind' || BIRTH_DATE == '123')");
        ShardQueryConfiguration config = EasyMock.createMock(ShardQueryConfiguration.class);
        MetadataHelper helper = EasyMock.createMock(MetadataHelper.class);
        
        HashSet<String> indexedFields = new HashSet<>();
        indexedFields.add("UUID");
        indexedFields.add("QUOTE");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        
        EasyMock.replay(config, helper);
        
        // find an orNode in the tree
        ExecutableExpansionVisitor visitor = new ExecutableExpansionVisitor(config, helper);
        Object data = visitor.visit(queryTree.jjtGetChild(0), null);
        
        EasyMock.verify(config, helper);
        
        Assert.assertFalse(data instanceof ExecutableExpansionVisitor.ExpansionTracker);
    }
    
    @Test
    public void testArbitraryNodeExpansionFlatten() throws Exception {
        ASTJexlScript queryTree = JexlASTHelper
                        .parseJexlQuery("UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind' || BIRTH_DATE == '123')");
        ShardQueryConfiguration config = EasyMock.createMock(ShardQueryConfiguration.class);
        MetadataHelper helper = EasyMock.createMock(MetadataHelper.class);
        
        HashSet<String> indexedFields = new HashSet<>();
        indexedFields.add("UUID");
        indexedFields.add("QUOTE");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        
        EasyMock.replay(config, helper);
        
        // find an orNode in the tree
        ExecutableExpansionVisitor visitor = new ExecutableExpansionVisitor(config, helper);
        ASTJexlScript rebuilt = TreeFlatteningRebuildingVisitor.flatten(queryTree);
        visitor.visit(rebuilt.jjtGetChild(0), null);
        
        EasyMock.verify(config, helper);
        
        String expected = "(QUOTE == 'kind' && UUID == 'capone') || ((filter:includeRegex(QUOTE, '.*kind.*') || BIRTH_DATE == '123') && UUID == 'capone')";
        Assert.assertEquals(expected, JexlStringBuildingVisitor.buildQuery(rebuilt));
    }
    
    @Test
    public void testNestedExpansionWithFailures() throws Exception {
        ASTJexlScript queryTree = JexlASTHelper
                        .parseJexlQuery("UUID == 'A' && (QUOTE == 'kind' || BIRTH_DATE == '234'|| (BIRTH_DATE == '123' && QUOTE == 'kind' && !(filter:includeRegex(QUOTE, '.*unkind.*') || BIRTH_DATE =='555' )))");
        ShardQueryConfiguration config = EasyMock.createMock(ShardQueryConfiguration.class);
        MetadataHelper helper = EasyMock.createMock(MetadataHelper.class);
        
        HashSet<String> indexedFields = new HashSet<>();
        indexedFields.add("UUID");
        indexedFields.add("QUOTE");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        
        EasyMock.replay(config, helper);
        
        // find an orNode in the tree
        ExecutableExpansionVisitor visitor = new ExecutableExpansionVisitor(config, helper);
        ASTJexlScript rebuilt = TreeFlatteningRebuildingVisitor.flatten(queryTree);
        visitor.visit(rebuilt.jjtGetChild(0), null);
        
        EasyMock.verify(config, helper);
        
        Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(rebuilt, config, helper));
        String expected = "(QUOTE == 'kind' && UUID == 'A') || (BIRTH_DATE == '123' && QUOTE == 'kind' && !(filter:includeRegex(QUOTE, '.*unkind.*') || BIRTH_DATE == '555') && UUID == 'A') || (BIRTH_DATE == '234' && UUID == 'A')";
        Assert.assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(rebuilt));
    }
    
    @Test
    public void testExceededThresholdExpansionExternal() throws Exception {
        ASTJexlScript queryTree = JexlASTHelper
                        .parseJexlQuery("UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind' || BIRTH_DATE == '123')");
        
        // update the generated queryTree to have an ExceededThreshold marker
        JexlNode child = new ExceededValueThresholdMarkerJexlNode(queryTree.jjtGetChild(0).jjtGetChild(0));
        // unlink the old node
        queryTree.jjtGetChild(0).jjtGetChild(0).jjtSetParent(null);
        // overwrite the old UUID==capone with the ExceededThreshold marker
        queryTree.jjtGetChild(0).jjtAddChild(child, 0);
        
        ShardQueryConfiguration config = EasyMock.createMock(ShardQueryConfiguration.class);
        MetadataHelper helper = EasyMock.createMock(MetadataHelper.class);
        
        HashSet<String> indexedFields = new HashSet<>();
        indexedFields.add("UUID");
        indexedFields.add("QUOTE");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        
        EasyMock.replay(config, helper);
        
        ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);
        
        EasyMock.verify(config, helper);
        
        // included ExceededValueThresholdMarker before
        Assert.assertTrue(
                        JexlStringBuildingVisitor.buildQuery(queryTree),
                        JexlStringBuildingVisitor
                                        .buildQuery(queryTree)
                                        .equals("((_Value_ = true) && (UUID == 'capone')) && (filter:includeRegex(QUOTE, '.*kind.*') || QUOTE == 'kind' || BIRTH_DATE == '123')"));
        
        // not executable
        Assert.assertFalse(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
        // what came out is executable
        Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
        // it looks like what we'd expect
        String expected = "(QUOTE == 'kind' && ((_Value_ = true) && (UUID == 'capone'))) || "
                        + "((filter:includeRegex(QUOTE, '.*kind.*') || BIRTH_DATE == '123') && ((_Value_ = true) && (UUID == 'capone')))";
        Assert.assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(newTree));
    }
    
    @Test
    public void testExceededThresholdExpansionInternal() throws Exception {
        ASTJexlScript queryTree = JexlASTHelper
                        .parseJexlQuery("UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind' || BIRTH_DATE == '123')");
        
        // update the generated queryTree to have an ExceededThreshold marker for BIRTH_DATE
        JexlNode child = new ExceededValueThresholdMarkerJexlNode(queryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).jjtGetChild(0).jjtGetChild(1));
        // unlink the old node
        queryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).jjtGetChild(0).jjtGetChild(1).jjtSetParent(null);
        // overwrite the old BIRTH_DATE==123 with the ExceededThreshold marker
        queryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).jjtGetChild(0).jjtAddChild(child, 1);
        
        ShardQueryConfiguration config = EasyMock.createMock(ShardQueryConfiguration.class);
        MetadataHelper helper = EasyMock.createMock(MetadataHelper.class);
        
        HashSet<String> indexedFields = new HashSet<>();
        indexedFields.add("UUID");
        indexedFields.add("QUOTE");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        
        EasyMock.replay(config, helper);
        
        ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);
        
        EasyMock.verify(config, helper);
        
        // included ExceededValueThresholdMarker before
        Assert.assertTrue(JexlStringBuildingVisitor.buildQuery(queryTree), JexlStringBuildingVisitor.buildQuery(queryTree).equals(
                        "UUID == 'capone' && (filter:includeRegex(QUOTE, '.*kind.*') || QUOTE == 'kind' || " + "((_Value_ = true) && (BIRTH_DATE == '123')))"));
        
        // not executable
        Assert.assertFalse(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
        // what came out is executable
        Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
        // it looks like what we'd expect
        String expected = "(QUOTE == 'kind' && UUID == 'capone') || " + "(((_Value_ = true) && (BIRTH_DATE == '123')) && UUID == 'capone') || "
                        + "(filter:includeRegex(QUOTE, '.*kind.*') && UUID == 'capone')";
        Assert.assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(newTree));
    }
    
    @Test
    public void testExceededOrThresholdExpansion() throws Exception {
        ASTJexlScript queryTree = JexlASTHelper
                        .parseJexlQuery("UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind' || BIRTH_DATE == '123')");
        
        // update the generated queryTree to have an ExceededOrThreshold marker for BIRTH_DATE
        Set<String> birthdates = new HashSet<>();
        birthdates.add("123");
        birthdates.add("234");
        birthdates.add("345");
        JexlNode child = ExceededOrThresholdMarkerJexlNode.createFromValues("BIRTH_DATE", birthdates);
        // unlink the old node
        queryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).jjtGetChild(0).jjtGetChild(1).jjtSetParent(null);
        // overwrite the old BIRTH_DATE==123 with the ExceededThreshold marker
        queryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).jjtGetChild(0).jjtAddChild(child, 1);
        
        ShardQueryConfiguration config = EasyMock.createMock(ShardQueryConfiguration.class);
        MetadataHelper helper = EasyMock.createMock(MetadataHelper.class);
        
        HashSet<String> indexedFields = new HashSet<>();
        indexedFields.add("UUID");
        indexedFields.add("QUOTE");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        
        EasyMock.replay(config, helper);
        
        ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);
        
        EasyMock.verify(config, helper);
        
        String queryString = JexlStringBuildingVisitor.buildQuery(queryTree);
        String id = queryString.substring(queryString.indexOf("id = '") + 6, queryString.indexOf("') && (field"));
        
        // included ExceededValueThresholdMarker before
        Assert.assertTrue(
                        queryString,
                        queryString.equals("UUID == 'capone' && (filter:includeRegex(QUOTE, '.*kind.*') || QUOTE == 'kind' || "
                                        + "((_List_ = true) && ((id = '" + id
                                        + "') && (field = 'BIRTH_DATE') && (params = '{\"values\":[\"123\",\"234\",\"345\"]}'))))"));
        
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
    
    @Test
    public void testExceededOrThresholdCannotExpand() throws Exception {
        ASTJexlScript queryTree = JexlASTHelper
                        .parseJexlQuery("UUID == 'capone' && (((_List_ = true) && (((id = 'some-bogus-id') && (field = 'QUOTE') && (params = '{\"values\":[\"a\",\"b\",\"c\"]}')))))");
        
        ShardQueryConfiguration config = EasyMock.createMock(ShardQueryConfiguration.class);
        MetadataHelper helper = EasyMock.createMock(MetadataHelper.class);
        
        HashSet<String> indexedFields = new HashSet<>();
        indexedFields.add("UUID");
        indexedFields.add("QUOTE");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        
        EasyMock.replay(config, helper);
        
        ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);
        
        EasyMock.verify(config, helper);
        
        // included ExceededValueThresholdMarker before
        Assert.assertTrue(
                        JexlStringBuildingVisitor.buildQuery(queryTree),
                        JexlStringBuildingVisitor
                                        .buildQuery(queryTree)
                                        .equals("UUID == 'capone' && (((_List_ = true) && (((id = 'some-bogus-id') && (field = 'QUOTE') && (params = '{\"values\":[\"a\",\"b\",\"c\"]}')))))"));
        
        // starts off executable
        Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
        // what came out is executable
        Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
        // the visitor changed nothing
        Assert.assertTrue(JexlStringBuildingVisitor.buildQuery(newTree),
                        JexlStringBuildingVisitor.buildQuery(newTree).equals(JexlStringBuildingVisitor.buildQuery(queryTree)));
    }
    
    @Test
    public void testDelayed() throws Exception {
        ASTJexlScript queryTree = JexlASTHelper.parseJexlQuery("UUID == 'capone' && (QUOTE == 'kind' || " + "((_Delayed_ = true) && BIRTH_DATE == '123'))");
        
        ShardQueryConfiguration config = EasyMock.createMock(ShardQueryConfiguration.class);
        MetadataHelper helper = EasyMock.createMock(MetadataHelper.class);
        
        HashSet<String> indexedFields = new HashSet<>();
        indexedFields.add("UUID");
        indexedFields.add("QUOTE");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        Set<String> dataTypes = new HashSet<>();
        dataTypes.add("test");
        Set<String> nonEventFields = new HashSet<>();
        nonEventFields.add("QUOTE");
        EasyMock.expect(config.getDatatypeFilter()).andReturn(dataTypes).anyTimes();
        EasyMock.expect(helper.getNonEventFields(dataTypes)).andReturn(nonEventFields).anyTimes();
        EasyMock.replay(config, helper);
        
        ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);
        
        EasyMock.verify(config, helper);
        
        // included ExceededValueThresholdMarker before
        Assert.assertTrue(JexlStringBuildingVisitor.buildQuery(queryTree), JexlStringBuildingVisitor.buildQuery(queryTree).equals(
                        "UUID == 'capone' && (QUOTE == 'kind' || " + "((_Delayed_ = true) && BIRTH_DATE == '123'))"));
        
        // starts off executable
        Assert.assertFalse(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
        // what came out is executable
        Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
        // the visitor changed nothing
        String expected = "(QUOTE == 'kind' && UUID == 'capone') || (((_Delayed_ = true) && BIRTH_DATE == '123') && UUID == 'capone')";
        Assert.assertEquals(expected, JexlStringBuildingVisitor.buildQueryWithoutParse(newTree));
    }
    
    @Test
    public void testDelayedDoubleExpansion() throws Exception {
        ASTJexlScript queryTree = JexlASTHelper.parseJexlQuery("UUID == 'capone' && (((_Delayed_ = true) && QUOTE == 'kind') || "
                        + "((_Delayed_ = true) && BIRTH_DATE == '123'))");
        
        ShardQueryConfiguration config = EasyMock.createMock(ShardQueryConfiguration.class);
        MetadataHelper helper = EasyMock.createMock(MetadataHelper.class);
        
        HashSet<String> indexedFields = new HashSet<>();
        indexedFields.add("UUID");
        indexedFields.add("QUOTE");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        Set<String> dataTypes = new HashSet<>();
        dataTypes.add("test");
        // QUOTE being delayed creates a query that is non-executable we cannot delay a field which is nonEvent
        Set<String> nonEventFields = new HashSet<>();
        nonEventFields.add("QUOTE");
        EasyMock.expect(config.getDatatypeFilter()).andReturn(dataTypes).anyTimes();
        EasyMock.expect(helper.getNonEventFields(dataTypes)).andReturn(nonEventFields).anyTimes();
        EasyMock.replay(config, helper);
        
        ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);
        
        EasyMock.verify(config, helper);
        
        // included ExceededValueThresholdMarker before
        Assert.assertTrue(JexlStringBuildingVisitor.buildQuery(queryTree), JexlStringBuildingVisitor.buildQuery(queryTree).equals(
                        "UUID == 'capone' && (((_Delayed_ = true) && QUOTE == 'kind') || " + "((_Delayed_ = true) && BIRTH_DATE == '123'))"));
        
        // starts off executable
        Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
        // what came out is executable
        Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
        // the visitor changed nothing
        Assert.assertTrue(JexlStringBuildingVisitor.buildQuery(newTree),
                        JexlStringBuildingVisitor.buildQuery(newTree).equals(JexlStringBuildingVisitor.buildQuery(queryTree)));
    }
    
    @Test
    public void testSingleOr() throws Exception {
        ASTJexlScript queryTree = JexlASTHelper.parseJexlQuery("UUID == 'capone' && (QUOTE =='kind' || BIRTH_DATE == '123')");
        
        // grab the reference to the QUOTE=='kind' eqnode
        JexlNode quoteNode = queryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).jjtGetChild(0).jjtGetChild(0);
        ASTOrNode newOr = new ASTOrNode(ParserTreeConstants.JJTORNODE);
        newOr.jjtAddChild(quoteNode, 0);
        quoteNode.jjtSetParent(newOr);
        
        // attach the new or node
        queryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).jjtAddChild(newOr, 0);
        newOr.jjtSetParent(queryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0));
        
        ShardQueryConfiguration config = EasyMock.createMock(ShardQueryConfiguration.class);
        MetadataHelper helper = EasyMock.createMock(MetadataHelper.class);
        
        HashSet<String> indexedFields = new HashSet<>();
        indexedFields.add("UUID");
        indexedFields.add("QUOTE");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        
        EasyMock.replay(config, helper);
        
        ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);
        
        EasyMock.verify(config, helper);
        
        // starts executable
        Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
        // what came out is executable
        Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
        // the visitor changed nothing
        Assert.assertTrue(JexlStringBuildingVisitor.buildQuery(newTree),
                        JexlStringBuildingVisitor.buildQuery(newTree).equals(JexlStringBuildingVisitor.buildQuery(queryTree)));
    }
    
    @Test
    public void testSingleOrNonExecutableCantFix() throws Exception {
        ASTJexlScript queryTree = JexlASTHelper.parseJexlQuery("BIRTH_DATE =='123' && (filter:includeRegex(QUOTE, '.*kind.*') || BIRTH_DATE == '234')");
        
        // grab the reference to the QUOTE=='kind' eqnode
        JexlNode quoteNode = queryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).jjtGetChild(0).jjtGetChild(0);
        ASTOrNode newOr = new ASTOrNode(ParserTreeConstants.JJTORNODE);
        newOr.jjtAddChild(quoteNode, 0);
        quoteNode.jjtSetParent(newOr);
        
        // attach the new or node
        queryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0).jjtAddChild(newOr, 0);
        newOr.jjtSetParent(queryTree.jjtGetChild(0).jjtGetChild(1).jjtGetChild(0));
        
        ShardQueryConfiguration config = EasyMock.createMock(ShardQueryConfiguration.class);
        MetadataHelper helper = EasyMock.createMock(MetadataHelper.class);
        
        HashSet<String> indexedFields = new HashSet<>();
        indexedFields.add("UUID");
        indexedFields.add("QUOTE");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        
        EasyMock.replay(config, helper);
        
        ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);
        
        EasyMock.verify(config, helper);
        
        // starts executable
        Assert.assertFalse(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
        // what came out is executable
        Assert.assertFalse(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
        // the visitor changed nothing
        Assert.assertTrue(JexlStringBuildingVisitor.buildQuery(newTree),
                        JexlStringBuildingVisitor.buildQuery(newTree).equals(JexlStringBuildingVisitor.buildQuery(queryTree)));
    }
    
    @Test
    public void testNoReferenceOrReferenceExpressions() throws Exception {
        ASTJexlScript queryTree = JexlASTHelper
                        .parseJexlQuery("UUID == 'capone' && (filter:includeRegex(QUOTE,'.*kind.*') || QUOTE == 'kind' || BIRTH_DATE == '123')");
        
        // strip reference/referenceExpressions
        queryTree = TreeFlatteningRebuildingVisitor.flattenAll(queryTree);
        
        ShardQueryConfiguration config = EasyMock.createMock(ShardQueryConfiguration.class);
        MetadataHelper helper = EasyMock.createMock(MetadataHelper.class);
        
        HashSet<String> indexedFields = new HashSet<>();
        indexedFields.add("UUID");
        indexedFields.add("QUOTE");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        
        EasyMock.replay(config, helper);
        
        ASTJexlScript newTree = ExecutableExpansionVisitor.expand(queryTree, config, helper);
        
        EasyMock.verify(config, helper);
        
        // starts executable
        Assert.assertFalse(ExecutableDeterminationVisitor.isExecutable(queryTree, config, helper));
        // what came out is executable
        Assert.assertTrue(ExecutableDeterminationVisitor.isExecutable(newTree, config, helper));
        // the visitor changed nothing
        Assert.assertTrue(JexlStringBuildingVisitor.buildQuery(newTree), JexlStringBuildingVisitor.buildQuery(newTree).equals(
                        "(QUOTE == 'kind' && UUID == 'capone') || " + "((filter:includeRegex(QUOTE, '.*kind.*') || BIRTH_DATE == '123') && UUID == 'capone')"));
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
    
}
