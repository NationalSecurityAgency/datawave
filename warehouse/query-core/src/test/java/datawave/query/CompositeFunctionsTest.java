package datawave.query;

import datawave.configuration.spring.SpringBean;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.attributes.TypeAttribute;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.WiseGuysIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import javax.inject.Inject;
import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Tests the composite functions, the #JEXL lucene function, the matchesAtLeastCountOf function. and others
 * 
 */
public abstract class CompositeFunctionsTest {
    
    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    @RunWith(Arquillian.class)
    public static class ShardRange extends CompositeFunctionsTest {
        protected static Connector connector = null;
        
        @BeforeClass
        public static void setUp() throws Exception {
            // this will get property substituted into the TypeMetadataBridgeContext.xml file
            // for the injection test (when this unit test is first created)
            File tempDir = temporaryFolder.newFolder("TempDirForCompositeFunctionsTestShardRange");
            System.setProperty("type.metadata.dir", tempDir.getCanonicalPath());
            
            QueryTestTableHelper qtth = new QueryTestTableHelper(CompositeFunctionsTest.ShardRange.class.toString(), log);
            connector = qtth.connector;
            
            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.SHARD);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.METADATA_TABLE_NAME);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }
        
        @AfterClass
        public static void teardown() {
            TypeRegistry.reset();
        }
        
        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, connector, eventQueryLogic);
        }
        
        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms, ShardQueryLogic logic)
                        throws Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, connector, logic);
        }
    }
    
    @RunWith(Arquillian.class)
    public static class DocumentRange extends CompositeFunctionsTest {
        protected static Connector connector = null;
        
        @BeforeClass
        public static void setUp() throws Exception {
            // this will get property substituted into the TypeMetadataBridgeContext.xml file
            // for the injection test (when this unit test is first created)
            File tempDir = temporaryFolder.newFolder("TempDirForCompositeFunctionsTestDocumentRange");
            System.setProperty("type.metadata.dir", tempDir.getCanonicalPath());
            
            QueryTestTableHelper qtth = new QueryTestTableHelper(CompositeFunctionsTest.DocumentRange.class.toString(), log);
            connector = qtth.connector;
            
            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.DOCUMENT);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.METADATA_TABLE_NAME);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }
        
        @AfterClass
        public static void teardown() {
            TypeRegistry.reset();
        }
        
        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, connector, eventQueryLogic);
        }
        
        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms, ShardQueryLogic logic)
                        throws Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, connector, logic);
        }
    }
    
    private static final Logger log = Logger.getLogger(CompositeFunctionsTest.class);
    
    protected Authorizations auths = new Authorizations("ALL");
    
    private Set<Authorizations> authSet = Collections.singleton(auths);
    
    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic eventQueryLogic;
    
    @Inject
    @SpringBean(name = "TLDEventQuery")
    protected ShardQueryLogic tldEventQueryLogic;
    
    private KryoDocumentDeserializer deserializer;
    
    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    
    @Deployment
    public static JavaArchive createDeployment() {
        
        return ShrinkWrap
                        .create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class)
                        .deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .deleteClass(datawave.query.metrics.ShardTableQueryMetricHandler.class)
                        .addAsManifestResource(
                                        new StringAsset("<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>"
                                                        + "</alternatives>"), "beans.xml");
    }
    
    @Before
    public void setup() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        
        eventQueryLogic.setFullTableScanEnabled(true);
        eventQueryLogic.setMaxDepthThreshold(6);
        tldEventQueryLogic.setFullTableScanEnabled(true);
        tldEventQueryLogic.setMaxDepthThreshold(6);
        deserializer = new KryoDocumentDeserializer();
    }
    
    protected abstract void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws Exception;
    
    protected abstract void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms,
                    ShardQueryLogic logic) throws Exception;
    
    protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms, Connector connector,
                    ShardQueryLogic logic) throws Exception {
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
        logic.setMaxEvaluationPipelines(1);
        
        GenericQueryConfiguration config = logic.initialize(connector, settings, authSet);
        logic.setupQuery(config);
        
        HashSet<String> expectedSet = new HashSet<>(expected);
        HashSet<String> resultSet;
        resultSet = new HashSet<>();
        Set<Document> docs = new HashSet<>();
        for (Entry<Key,Value> entry : logic) {
            Document d = deserializer.apply(entry).getValue();
            
            log.debug(entry.getKey() + " => " + d);
            
            Attribute<?> attr = d.get("UUID");
            if (attr == null) {
                attr = d.get("UUID.0");
            }
            
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
    
    @Test
    public void testMatchesAtLeastCountOf() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("testMatchesAtLeastCountOf");
        }
        // @formatter:off
        String[] queryStrings = {
                "UUID =~ '^[CS].*' AND filter:matchesAtLeastCountOf(3,NAM,'MICHAEL','VINCENT','FREDO','TONY')",
                "UUID =~ '^[CS].*' AND filter:matchesAtLeastCountOf(3,NAME,'MICHAEL','VINCENT','FRED','TONY')"
        };
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {
                Collections.singletonList("CORLEONE"),
                Collections.emptyList()
        };
        // @formatter:on
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
    
    @Test
    public void testMatchesAtLeastCountOfWithOptionsFunction() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        
        if (log.isDebugEnabled()) {
            log.debug("testMatchesAtLeastCountOf");
        }
        // @formatter:off
        String[] queryStrings = {
                "UUID =~ '^[CS].*' AND filter:matchesAtLeastCountOf(3,NAM,'MICHAEL','VINCENT','FREDO','TONY') "+
                        "AND f:options('include.grouping.context','true','hit.list','true')",

                "UUID =~ '^[CS].*' AND filter:matchesAtLeastCountOf(3,NAME,'MICHAEL','VINCENT','FRED','TONY') "+
                        "OR f:options('include.grouping.context','true','hit.list','true')"
        };

        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {
                Collections.singletonList("CORLEONE"),
                Collections.emptyList()
        };
        // @formatter:on
        
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
    
    @Test
    public void testDateDelta() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("testDateDelta");
        }
        // @formatter:off
        String[] queryStrings = {
                "UUID =~ '^[CS].*' AND filter:getMaxTime(DEATH_DATE) - filter:getMinTime(BIRTH_DATE) > 2522880000000L", // 80+ years
                "UUID =~ '^[CS].*' AND filter:getMaxTime(DEATH_DATE) - filter:getMinTime(BIRTH_DATE) > 1892160000000L", // 60+ years
                "UUID =~ '^[CS].*' AND filter:timeFunction(DEATH_DATE,BIRTH_DATE,'-','>',2522880000000L)", // 80+ years
                "(UUID:C* OR UUID:S*) AND #TIME_FUNCTION(DEATH_DATE,BIRTH_DATE,'-','>','2522880000000L')"
        };
        // timeFunction(Object time1, Object time2, String operatorString, String equalityString, long goal)
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Collections.singletonList("CAPONE"), Arrays.asList("CORLEONE", "CAPONE"),
                Collections.singletonList("CAPONE"), Collections.singletonList("CAPONE"),};
        for (int i = 0; i < queryStrings.length; i++) {
            if (i == 3) {
                eventQueryLogic.setParser(new LuceneToJexlQueryParser());
            }
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
    
    @Test
    public void testAgainstUnsupportedCompositeStructures() {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("makeSureTheyCannotDoAnythingCrazy");
        }
        // @formatter:off
        String[] queryStrings = {
                "UUID == 'CORLEONE' AND  filter:getAllMatches(NAME,'SANTINO').add('NAME:GROUCHO') == true",
                "UUID == 'CORLEONE' AND  filter:getAllMatches(NAME,'SANTINO').clear() == false"
        };
        
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Collections.emptyList(), Collections.emptyList()};
        for (int i = 0; i < queryStrings.length; i++) {
            try {
                runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
            } catch (Throwable t) {
                log.error(t);
                Assert.assertTrue(t instanceof DatawaveFatalQueryException);
            }
        }
    }
    
    @Test
    public void testWithIndexOnlyFieldsAndModelExpansion() {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("testWithIndexOnlyFieldsAndModelExpansion");
        }
        // @formatter:off
        String[] queryStrings = {
                "UUID =~ '^[CS].*' AND filter:includeRegex(LOCATION,'chicago')", // LOCATION is index-only
                "UUID =~ '^[CS].*' AND filter:includeRegex(LOC,'newyork')", // LOC model-maps to LOCATION and POSIZIONE, both are index-only
                "UUID =~ '^[CS].*' AND filter:includeRegex(LOC,'new.*')", // see above
                "UUID =~ '^[CS].*' AND filter:excludeRegex(LOC,'new.*')", // see above, but this will fail with the correct exception,because index-only fields
                                                                          // are mixed with expressions that cannot be run against the index
                "UUID =~ '^[CS].*' AND filter:excludeRegex(NAM,'A.*')", // this will expand to excludeRegex(NAME||NOME, 'A.*') which will become
                                                                        // !includeRegex(NAME||NOME, 'A.*')
        };
        
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Collections.singletonList("CAPONE"), Collections.singletonList("CORLEONE"),
                Arrays.asList("CORLEONE", "SOPRANO"), Collections.singletonList("CAPONE"), Collections.singletonList("CORLEONE"),};
        for (int i = 0; i < queryStrings.length; i++) {
            try {
                runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
            } catch (Throwable t) {
                log.error(t);
                Assert.assertTrue(t instanceof DatawaveFatalQueryException);
            }
        }
        
    }
    
    @Test
    public void testArithmetic() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("testArithmetic");
        }
        // @formatter:off
        String[] queryStrings = {
                "UUID =~ 'CORLEONE' AND 1 + 1 + 1 == 3",
                "UUID =~ 'CORLEONE' AND 1 * 2 * 3 == 6",
                "UUID =~ 'CORLEONE' AND 12 / 2 / 3 == 2",
                "UUID == 'CORLEONE' AND 1 + 1 + 1 == 4",
                "UUID == 'CORLEONE' AND 1 * 2 * 3 == 7",
                "UUID == 'CORLEONE' AND 12 / 2 / 3 == 3",
                "UUID == 'CORLEONE' AND filter:getAllMatches(NAM,'hubert').isEmpty() == true",
                "UUID == 'CORLEONE' AND filter:getAllMatches(NAM,'hubert').size() == 0"
        };
        
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Collections.singletonList("CORLEONE"), Collections.singletonList("CORLEONE"),
                Collections.singletonList("CORLEONE"), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
                Collections.singletonList("CORLEONE"), Collections.singletonList("CORLEONE"),};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
    
    @Test
    public void testNulls() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("testNulls");
        }
        // @formatter:off
        String[] queryStrings = {
                "UUID =~ '^[CS].*' AND filter:isNull(NULL1)", // no model expansion, NULL1 is not in the event(s)
                "UUID =~ '^[CS].*' AND filter:isNull(UUID)", // no model expansion, UUID is non-null in all events
                "UUID =~ '^[CS].*' AND filter:isNull(BOTH_NULL)", // expands to NULL1||NULL2, neither are in any events
                "filter:isNull(NULL2||NULL1)",
                "filter:isNull(BOTH_NULL)",
                // these 2 are equivalent:
                "filter:isNull(UUID||NULL1)",
                "filter:isNull(UUID) && filter:isNull(NULL1)",
                // these 2 are equivalent
                "filter:isNull(NULL1||NULL2)",
                "filter:isNull(NULL1) && filter:isNull(NULL2)",
                // these 3 are equivalent
                "UUID =~ '^[CS].*' AND filter:isNull(ONE_NULL)",
                "UUID =~ '^[CS].*' AND filter:isNull(UUID||NULL1)",
                "UUID =~ '^[CS].*' AND filter:isNull(UUID) && filter:isNull(NULL1)"
        };
        
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"), Collections.emptyList(),
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"), Arrays.asList("CORLEONE", "CAPONE", "SOPRANO", "ANDOLINI"), Arrays.asList("CORLEONE", "CAPONE", "SOPRANO", "ANDOLINI"),
                Collections.emptyList(), Collections.emptyList(),
                
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO", "ANDOLINI"), Arrays.asList("CORLEONE", "CAPONE", "SOPRANO", "ANDOLINI"), Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList(),};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
    
    @Test
    public void testNotNulls() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("testNotNulls");
        }
        // @formatter:off
        String[] queryStrings = {
                "filter:isNotNull(UUID)",
                "filter:isNotNull(NULL1)",
                
                // these are equivalent:
                "filter:isNotNull(NULL1||NULL2)",
                "filter:isNotNull(NULL1) || filter:isNotNull(NULL2)",
                "filter:isNotNull(BOTH_NULL)",
                
                // these are equivalent:
                "filter:isNotNull(UUID||NULL1)",
                "filter:isNotNull(UUID) || filter:isNotNull(NULL1)",
                "filter:isNotNull(ONE_NULL)",
                
                "UUID =~ '^[CS].*' AND filter:isNotNull(UUID)",
                "UUID =~ '^[CS].*' AND filter:isNotNull(NULL1)",
                "UUID =~ '^[CS].*' AND filter:isNotNull(NULL1||NULL2)",
                "UUID =~ '^[CS].*' AND filter:isNotNull(BOTH_NULL)",
                "UUID =~ '^[CS].*' AND filter:isNotNull(UUID||NULL1)",
                "UUID =~ '^[CS].*' AND filter:isNotNull(ONE_NULL)"
        };
        
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CORLEONE", "CAPONE", "SOPRANO", "ANDOLINI"), Collections.emptyList(), Collections.emptyList(),
                Collections.emptyList(), Collections.emptyList(), Arrays.asList("CORLEONE", "CAPONE", "SOPRANO", "ANDOLINI"),
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO", "ANDOLINI"), Arrays.asList("CORLEONE", "CAPONE", "SOPRANO", "ANDOLINI"),
                
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"), Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"),};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
    
    @Test
    public void composeFunctionsInsteadOfMatchesAtLeastCountOf() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("composeFunctionsInsteadOfMatchesAtLeastCountOf");
        }
        String[] queryStrings = {
                "UUID =~ '^[CS].*' AND filter:includeRegex(NAM,'MICHAEL').size() + filter:includeRegex(NAM,'VINCENT').size() + filter:includeRegex(NAM,'FREDO').size() + filter:includeRegex(NAM,'TONY').size() >= 3",
                "UUID =~ '^[CS].*' AND filter:includeRegex(NAM,'MICHAEL').size() + filter:includeRegex(NAM,'VINCENT').size() + filter:includeRegex(NAM,'FRED').size() + filter:includeRegex(NAM,'TONY').size() >= 3"};
        
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Collections.singletonList("CORLEONE"), Collections.emptyList()};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
    
    @Test
    public void testCompositeFunctions() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("testCompositeFunctions");
        }
        // @formatter:off
        String[] queryStrings = {
                "UUID == 'SOPRANO' AND  1 + 1 == 2",
                "UUID == 'SOPRANO' AND  1 * 1 == 1",
                "filter:getAllMatches(NAM,'MICHAEL').size() + filter:getAllMatches(NAM,'SANTINO').size() >= 1  AND UUID =~ '^[CS].*'",
                "UUID =~ '^[CS].*' AND filter:getAllMatches(NAM,'MICHAEL').size() > 0",
                "UUID =~ '^[CS].*' AND filter:includeRegex(NAM,'MICHAEL').size() == 1",
                "UUID == 'CORLEONE' AND filter:getAllMatches(NAM,'SANTINO').size() == 1",
                "UUID =~ '^[CS].*' AND filter:getAllMatches(NAM,'MICHAEL').size() > 0 AND filter:getAllMatches(NAM,'MICHAEL').size() < 2",
                "UUID == 'SOPRANO' AND  filter:getAllMatches(NAM,'MICHAEL').contains('foo') == false",
                "UUID == 'SOPRANO' AND  filter:getAllMatches(NAM,'ANTHONY').contains('NAME.0:ANTHONY') == true",
                "UUID =~ '^[CS].*' AND  filter:getAllMatches(NAM,'.*O').contains('NOME.0:SANTINO') == true"
        };
        
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {
                Collections.singletonList("SOPRANO"), // family name starts with C or S
                Collections.singletonList("SOPRANO"), // family name starts with C or S
                Arrays.asList("CORLEONE", "CAPONE"), Arrays.asList("CORLEONE", "CAPONE"), Arrays.asList("CORLEONE", "CAPONE"),
                Collections.singletonList("CORLEONE"), Arrays.asList("CORLEONE", "CAPONE"), Collections.singletonList("SOPRANO"),
                Collections.singletonList("SOPRANO"), Collections.singletonList("CORLEONE")};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
    
    @Test
    public void testMatchesAtLeastCountOfWithLucene() throws Exception {
        eventQueryLogic.setParser(new LuceneToJexlQueryParser());
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("testMatchesAtLeastCountOfWithLucene");
        }
        String[] queryStrings = {"(UUID:C* OR UUID:S*) AND #MATCHES_AT_LEAST_COUNT_OF('3',NAM,'MICHAEL','VINCENT','FREDO','TONY')",};
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Collections.singletonList("CORLEONE"),};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }

    @Test
    public void testWithLucene() throws Exception {
        eventQueryLogic.setParser(new LuceneToJexlQueryParser());
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        if (log.isDebugEnabled()) {
            log.debug("testWithLucene");
        }
        // @formatter:off
        String[] queryStrings = {
                "UUID:C*", // family name starts with 'C'
                "UUID:SOPRANO", // family name is SOPRANO
                "UUID:C* OR UUID:S* ", // family name starts with C or S
                "(UUID:C* OR UUID:S*) AND #INCLUDE(NAM, 'CONSTANZIA') ", // family has child CONSTANZIA
                "(UUID:C* OR UUID:S*) AND #INCLUDE(NAM, 'MICHAEL') ", // family has child MICHAEL
                "#JEXL(\"$UUID == 'CORLEONE' AND filter:getAllMatches(NAM,'SANTINO').size() == 1\")", // test LUCENE function to deliver jexl
                "UUID:CORLEONE AND #JEXL(\"filter:getAllMatches(NAM,'SANTINO').size() == 1\")"
        };
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CAPONE", "CORLEONE"), // family name starts with 'C'
                Collections.singletonList("SOPRANO"), // family name is SOPRANO
                Arrays.asList("SOPRANO", "CORLEONE", "CAPONE"), // family name starts with C or S
                Collections.singletonList("CORLEONE"), // family has child CONSTANZIA
                Arrays.asList("CORLEONE", "CAPONE"), // family has child MICHAEL
                Collections.singletonList("CORLEONE"), Collections.singletonList("CORLEONE")};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }

    @Test
    public void testTLDWithLuceneAndIdentifierToLiteralLTJexl() throws Exception {
        tldEventQueryLogic.setParser(new LuceneToJexlQueryParser());
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        if (log.isDebugEnabled()) {
            log.debug("testWithLucene");
        }
        // @formatter:off
        String[] queryStrings = {
                "UUID:ANDOLINI AND #JEXL(ETA < 15)" // family name is ANDOLINI
        };
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Collections.singletonList("CORLEONE")};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters, tldEventQueryLogic);
        }
    }

    @Test
    public void testTLDWithLuceneAndIdentifierToLiteralEQJexl() throws Exception {
        tldEventQueryLogic.setParser(new LuceneToJexlQueryParser());
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        if (log.isDebugEnabled()) {
            log.debug("testWithLucene");
        }
        // @formatter:off
        String[] queryStrings = {
                "UUID:ANDOLINI AND #JEXL(ETA == 12)"
        };
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Collections.singletonList("CORLEONE")};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters, tldEventQueryLogic);
        }
    }

    @Test
    public void testTLDWithLuceneAndIdentifierToIdentifierJexl() throws Exception {
        tldEventQueryLogic.setParser(new LuceneToJexlQueryParser());
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");

        if (log.isDebugEnabled()) {
            log.debug("testWithLucene");
        }
        // @formatter:off
        String[] queryStrings = {
                "UUID:ANDOLINI AND #JEXL(ETA < MAGIC)"
        };
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Collections.singletonList("CORLEONE")};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters, tldEventQueryLogic);
        }
    }

    @Test
    public void testWithLuceneAndOptionsFunction() throws Exception {
        eventQueryLogic.setParser(new LuceneToJexlQueryParser());
        Map<String,String> extraParameters = new HashMap<>();
        
        if (log.isDebugEnabled()) {
            log.debug("testWithLucene");
        }
        // @formatter:off
        String[] queryStrings = {
                "UUID:C* AND #OPTIONS('include.grouping.context', 'true')", // family name starts with 'C'
                "UUID:SOPRANO AND #OPTIONS('include.grouping.context', 'true')", // family name is SOPRANO
                "UUID:C* OR UUID:S*  AND #OPTIONS('include.grouping.context', 'true')", // family name starts with C or S
                "(UUID:C* OR UUID:S*) AND #INCLUDE(NAM, 'CONSTANZIA')  AND #OPTIONS('include.grouping.context', 'true')", // family has child CONSTANZIA
                "(UUID:C* OR UUID:S*) AND #INCLUDE(NAM, 'MICHAEL')  AND #OPTIONS('include.grouping.context', 'true')", // family has child MICHAEL
                "#JEXL(\"$UUID == 'CORLEONE' AND filter:getAllMatches(NAM,'SANTINO').size() == 1\") AND #OPTIONS('include.grouping.context', 'true')", // test LUCENE function to deliver jexl
                "UUID:CORLEONE AND #JEXL(\"filter:getAllMatches(NAM,'SANTINO').size() == 1\") AND #OPTIONS('include.grouping.context', 'true')"
        };
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {
                Arrays.asList("CAPONE", "CORLEONE"), // family name starts with 'C'
                Collections.singletonList("SOPRANO"), // family name is SOPRANO
                Arrays.asList("SOPRANO", "CORLEONE", "CAPONE"), // family name starts with C or S
                Collections.singletonList("CORLEONE"), // family has child CONSTANZIA
                Arrays.asList("CORLEONE", "CAPONE"), // family has child MICHAEL
                Collections.singletonList("CORLEONE"),
                Collections.singletonList("CORLEONE")
        };
        // @formatter:on
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
    
}
