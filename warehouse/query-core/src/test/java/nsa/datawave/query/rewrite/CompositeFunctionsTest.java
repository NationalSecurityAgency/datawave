package nsa.datawave.query.rewrite;

import nsa.datawave.configuration.spring.SpringBean;
import nsa.datawave.helpers.PrintUtility;
import nsa.datawave.ingest.data.TypeRegistry;
import nsa.datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import nsa.datawave.query.rewrite.attributes.Attribute;
import nsa.datawave.query.rewrite.attributes.Document;
import nsa.datawave.query.rewrite.attributes.PreNormalizedAttribute;
import nsa.datawave.query.rewrite.attributes.TypeAttribute;
import nsa.datawave.query.rewrite.exceptions.DatawaveFatalQueryException;
import nsa.datawave.query.rewrite.function.deserializer.KryoDocumentDeserializer;
import nsa.datawave.query.rewrite.jexl.functions.EvaluationPhaseFilterFunctions;
import nsa.datawave.query.rewrite.tables.RefactoredShardQueryLogic;
import nsa.datawave.query.rewrite.util.WiseGuysIngest;
import nsa.datawave.webservice.edgedictionary.TestDatawaveEdgeDictionaryImpl;
import nsa.datawave.webservice.query.QueryImpl;
import nsa.datawave.webservice.query.configuration.GenericQueryConfiguration;
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
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import static nsa.datawave.query.rewrite.QueryTestTableHelper.METADATA_TABLE_NAME;
import static nsa.datawave.query.rewrite.QueryTestTableHelper.MODEL_TABLE_NAME;
import static nsa.datawave.query.rewrite.QueryTestTableHelper.SHARD_INDEX_TABLE_NAME;
import static nsa.datawave.query.rewrite.QueryTestTableHelper.SHARD_TABLE_NAME;

/**
 * Tests the composite functions, the #JEXL lucene function, and the matchesAtLeastCountOf function
 * 
 */
public abstract class CompositeFunctionsTest {
    
    @RunWith(Arquillian.class)
    public static class ShardRange extends CompositeFunctionsTest {
        protected static Connector connector = null;
        
        @BeforeClass
        public static void setUp() throws Exception {
            
            QueryTestTableHelper qtth = new QueryTestTableHelper(CompositeFunctionsTest.ShardRange.class.toString(), log);
            connector = qtth.connector;
            
            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.SHARD);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, SHARD_TABLE_NAME);
            PrintUtility.printTable(connector, auths, SHARD_INDEX_TABLE_NAME);
            PrintUtility.printTable(connector, auths, METADATA_TABLE_NAME);
            PrintUtility.printTable(connector, auths, MODEL_TABLE_NAME);
        }
        
        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws ParseException,
                        Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, connector);
        }
    }
    
    @RunWith(Arquillian.class)
    public static class DocumentRange extends CompositeFunctionsTest {
        protected static Connector connector = null;
        
        @BeforeClass
        public static void setUp() throws Exception {
            
            QueryTestTableHelper qtth = new QueryTestTableHelper(CompositeFunctionsTest.DocumentRange.class.toString(), log);
            connector = qtth.connector;
            
            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.DOCUMENT);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, SHARD_TABLE_NAME);
            PrintUtility.printTable(connector, auths, SHARD_INDEX_TABLE_NAME);
            PrintUtility.printTable(connector, auths, METADATA_TABLE_NAME);
            PrintUtility.printTable(connector, auths, MODEL_TABLE_NAME);
        }
        
        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws ParseException,
                        Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, connector);
        }
    }
    
    private static final Logger log = Logger.getLogger(CompositeFunctionsTest.class);
    
    protected Authorizations auths = new Authorizations("ALL");
    
    protected Set<Authorizations> authSet = Collections.singleton(auths);
    
    @Inject
    @SpringBean(name = "EventQuery")
    protected RefactoredShardQueryLogic logic;
    
    protected KryoDocumentDeserializer deserializer;
    
    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    
    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        
        return ShrinkWrap
                        .create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "nsa.datawave.query", "org.jboss.logging",
                                        "nsa.datawave.webservice.query.result.event")
                        .addClass(TestDatawaveEdgeDictionaryImpl.class)
                        .deleteClass(nsa.datawave.query.metrics.QueryMetricQueryLogic.class)
                        .deleteClass(nsa.datawave.query.metrics.ShardTableQueryMetricHandler.class)
                        .addAsManifestResource(
                                        new StringAsset("<alternatives>" + "<stereotype>nsa.datawave.query.tables.edge.MockAlternative</stereotype>"
                                                        + "</alternatives>"), "beans.xml");
    }
    
    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }
    
    @Before
    public void setup() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        
        logic.setFullTableScanEnabled(true);
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
        for (Entry<Key,Value> entry : logic) {
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
    
    @Test
    public void testMatchesAtLeastCountOf() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("testMatchesAtLeastCountOf");
        }
        String[] queryStrings = {"UUID =~ '^[CS].*' AND filter:matchesAtLeastCountOf(3,NAM,'MICHAEL','VINCENT','FREDO','TONY')",
                "UUID =~ '^[CS].*' AND filter:matchesAtLeastCountOf(3,NAME,'MICHAEL','VINCENT','FRED','TONY')"};
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CORLEONE"), Arrays.asList()};
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
        String[] queryStrings = {"UUID =~ '^[CS].*' AND filter:getMaxTime(DEATH_DATE) - filter:getMinTime(BIRTH_DATE) > 2522880000000L", // 80+ years
                "UUID =~ '^[CS].*' AND filter:getMaxTime(DEATH_DATE) - filter:getMinTime(BIRTH_DATE) > 1892160000000L", // 60+ years
                "UUID =~ '^[CS].*' AND filter:timeFunction(DEATH_DATE,BIRTH_DATE,'-','>',2522880000000L)", // 80+ years
                "(UUID:C* OR UUID:S*) AND #TIME_FUNCTION(DEATH_DATE,BIRTH_DATE,'-','>','2522880000000L')",};
        // timeFunction(Object time1, Object time2, String operatorString, String equalityString, long goal)
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CAPONE"), Arrays.asList("CORLEONE", "CAPONE"), Arrays.asList("CAPONE"),
                Arrays.asList("CAPONE"),};
        for (int i = 0; i < queryStrings.length; i++) {
            if (i == 3) {
                logic.setParser(new LuceneToJexlQueryParser());
            }
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
    
    @Test
    public void makeSureTheyCannotDoAnythingCrazy() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("makeSureTheyCannotDoAnythingCrazy");
        }
        String[] queryStrings = {"UUID == 'CORLEONE' AND  filter:getAllMatches(NAME,'SANTINO').add('NAME:GROUCHO') == true",
                "UUID == 'CORLEONE' AND  filter:getAllMatches(NAME,'SANTINO').clear() == false",};
        
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList(), Arrays.asList()};
        for (int i = 0; i < queryStrings.length; i++) {
            try {
                runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
            } catch (Throwable t) {
                t.printStackTrace();
                Assert.assertTrue(t instanceof DatawaveFatalQueryException);
            }
        }
    }
    
    @Test
    public void testWithIndexOnlyFieldsAndModelExpansion() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("testWithIndexOnlyFieldsAndModelExpansion");
        }
        String[] queryStrings = {"UUID =~ '^[CS].*' AND filter:includeRegex(LOCATION,'chicago')", // LOCATION is index-only
                "UUID =~ '^[CS].*' AND filter:includeRegex(LOC,'newyork')", // LOC model-maps to LOCATION and POSIZIONE, both are index-only
                "UUID =~ '^[CS].*' AND filter:includeRegex(LOC,'new.*')", // see above
                "UUID =~ '^[CS].*' AND filter:excludeRegex(LOC,'new.*')", // see above, but this will fail with the correct exception,because index-only fields
                                                                          // are mixed with expressions that cannot be run against the index
                "UUID =~ '^[CS].*' AND filter:excludeRegex(NAM,'A.*')", // this will expand to excludeRegex(NAME||NOME, 'A.*') which will become
                                                                        // !includeRegex(NAME||NOME, 'A.*')
        };
        
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CAPONE"), Arrays.asList("CORLEONE"), Arrays.asList("CORLEONE", "SOPRANO"),
                Arrays.asList("CAPONE"), Arrays.asList("CORLEONE"),};
        for (int i = 0; i < queryStrings.length; i++) {
            try {
                runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
            } catch (Throwable t) {
                t.printStackTrace();
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
        String[] queryStrings = {"UUID =~ 'CORLEONE' AND 1 + 1 + 1 == 3", "UUID =~ 'CORLEONE' AND 1 * 2 * 3 == 6", "UUID =~ 'CORLEONE' AND 12 / 2 / 3 == 2",
                "UUID == 'CORLEONE' AND 1 + 1 + 1 == 4", "UUID == 'CORLEONE' AND 1 * 2 * 3 == 7", "UUID == 'CORLEONE' AND 12 / 2 / 3 == 3",
                "UUID == 'CORLEONE' AND filter:getAllMatches(NAM,'hubert').isEmpty() == true",
                "UUID == 'CORLEONE' AND filter:getAllMatches(NAM,'hubert').size() == 0",};
        
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CORLEONE"), Arrays.asList("CORLEONE"), Arrays.asList("CORLEONE"), Arrays.asList(),
                Arrays.asList(), Arrays.asList(), Arrays.asList("CORLEONE"), Arrays.asList("CORLEONE"),};
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
        String[] queryStrings = {
                "UUID =~ '^[CS].*' AND filter:isNull(NULL1)", // no model expansion, NULL1 is not in the event(s)
                "UUID =~ '^[CS].*' AND filter:isNull(UUID)", // no model expansion, UUID is non-null in all events
                "UUID =~ '^[CS].*' AND filter:isNull(BOTH_NULL)", // expands to NULL1||NULL2, neither are in any events
                "filter:isNull(NULL2||NULL1)", "filter:isNull(BOTH_NULL)",
                // these 2 are equivalent:
                "filter:isNull(UUID||NULL1)", "filter:isNull(UUID) && filter:isNull(NULL1)",
                // these 2 are equivalent
                "filter:isNull(NULL1||NULL2)", "filter:isNull(NULL1) && filter:isNull(NULL2)",
                // these 3 are equivalent
                "UUID =~ '^[CS].*' AND filter:isNull(ONE_NULL)", "UUID =~ '^[CS].*' AND filter:isNull(UUID||NULL1)",
                "UUID =~ '^[CS].*' AND filter:isNull(UUID) && filter:isNull(NULL1)"};
        
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"), Arrays.asList(),
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"), Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"), Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"),
                Arrays.asList(), Arrays.asList(),
                
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"), Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"), Arrays.asList(), Arrays.asList(),
                Arrays.asList(),};
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
        String[] queryStrings = {"filter:isNotNull(UUID)",
                "filter:isNotNull(NULL1)",
                
                // these are equivalent:
                "filter:isNotNull(NULL1||NULL2)", "filter:isNotNull(NULL1) || filter:isNotNull(NULL2)",
                "filter:isNotNull(BOTH_NULL)",
                
                // these are equivalent:
                "filter:isNotNull(UUID||NULL1)", "filter:isNotNull(UUID) || filter:isNotNull(NULL1)", "filter:isNotNull(ONE_NULL)",
                
                "UUID =~ '^[CS].*' AND filter:isNotNull(UUID)", "UUID =~ '^[CS].*' AND filter:isNotNull(NULL1)",
                "UUID =~ '^[CS].*' AND filter:isNotNull(NULL1||NULL2)", "UUID =~ '^[CS].*' AND filter:isNotNull(BOTH_NULL)",
                "UUID =~ '^[CS].*' AND filter:isNotNull(UUID||NULL1)", "UUID =~ '^[CS].*' AND filter:isNotNull(ONE_NULL)",};
        
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"), Arrays.asList(), Arrays.asList(), Arrays.asList(),
                Arrays.asList(), Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"), Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"),
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"),
                
                Arrays.asList("CORLEONE", "CAPONE", "SOPRANO"), Arrays.asList(), Arrays.asList(), Arrays.asList(),
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
        List<String>[] expectedLists = new List[] {Arrays.asList("CORLEONE"), Arrays.asList()};
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
        String[] queryStrings = {"UUID == 'SOPRANO' AND  1 + 1 == 2", "UUID == 'SOPRANO' AND  1 * 1 == 1",
                "filter:getAllMatches(NAM,'MICHAEL').size() + filter:getAllMatches(NAM,'SANTINO').size() >= 1  AND UUID =~ '^[CS].*'",
                "UUID =~ '^[CS].*' AND filter:getAllMatches(NAM,'MICHAEL').size() > 0", "UUID =~ '^[CS].*' AND filter:includeRegex(NAM,'MICHAEL').size() == 1",
                "UUID == 'CORLEONE' AND filter:getAllMatches(NAM,'SANTINO').size() == 1",
                "UUID =~ '^[CS].*' AND filter:getAllMatches(NAM,'MICHAEL').size() > 0 AND filter:getAllMatches(NAM,'MICHAEL').size() < 2",
                "UUID == 'SOPRANO' AND  filter:getAllMatches(NAM,'MICHAEL').contains('foo') == false",
                "UUID == 'SOPRANO' AND  filter:getAllMatches(NAM,'ANTHONY').contains('NAME.0:ANTHONY') == true",
                "UUID =~ '^[CS].*' AND  filter:getAllMatches(NAM,'.*O').contains('NOME.0:SANTINO') == true",};
        
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {
                Arrays.asList("SOPRANO"), // family name starts with C or S
                Arrays.asList("SOPRANO"), // family name starts with C or S
                Arrays.asList("CORLEONE", "CAPONE"), Arrays.asList("CORLEONE", "CAPONE"), Arrays.asList("CORLEONE", "CAPONE"), Arrays.asList("CORLEONE"),
                Arrays.asList("CORLEONE", "CAPONE"), Arrays.asList("SOPRANO"), Arrays.asList("SOPRANO"), Arrays.asList("CORLEONE")};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
    
    @Test
    public void testMatchesAtLeastCountOfWithLucene() throws Exception {
        logic.setParser(new LuceneToJexlQueryParser());
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("testMatchesAtLeastCountOfWithLucene");
        }
        String[] queryStrings = {"(UUID:C* OR UUID:S*) AND #MATCHES_AT_LEAST_COUNT_OF('3',NAM,'MICHAEL','VINCENT','FREDO','TONY')",};
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CORLEONE"),};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
    
    @Test
    public void testWithLucene() throws Exception {
        logic.setParser(new LuceneToJexlQueryParser());
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("testWithLucene");
        }
        String[] queryStrings = {"UUID:C*", // family name starts with 'C'
                "UUID:SOPRANO",// family name is SOPRANO
                "UUID:C* OR UUID:S* ",// family name starts with C or S
                "(UUID:C* OR UUID:S*) AND #INCLUDE(NAM, 'CONSTANZIA') ",// family has child CONSTANZIA
                "(UUID:C* OR UUID:S*) AND #INCLUDE(NAM, 'MICHAEL') ", // family has child MICHAEL
                "#JEXL(\"$UUID == 'CORLEONE' AND filter:getAllMatches(NAM,'SANTINO').size() == 1\")",// test LUCENE function to deliver jexl
                "UUID:CORLEONE AND #JEXL(\"filter:getAllMatches(NAM,'SANTINO').size() == 1\")"};
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("CAPONE", "CORLEONE"), // family name starts with 'C'
                Arrays.asList("SOPRANO"), // family name is SOPRANO
                Arrays.asList("SOPRANO", "CORLEONE", "CAPONE"), // family name starts with C or S
                Arrays.asList("CORLEONE"), // family has child CONSTANZIA
                Arrays.asList("CORLEONE", "CAPONE"), // family has child MICHAEL
                Arrays.asList("CORLEONE"), Arrays.asList("CORLEONE")};
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
    
    @Test
    public void testRightOf() throws Exception {
        String[] inputs = {"NAME.grandparent_0.parent_0.child_0", "NAME.grandparent_0.parent_0.child_0",
                "NAME.gggparent.ggparent.grandparent_0.parent_0.child_0",};
        String[] expected = {"child_0", "parent_0.child_0", "ggparent.grandparent_0.parent_0.child_0",};
        int[] groupNumber = new int[] {0, 1, 3};
        
        for (int i = 0; i < inputs.length; i++) {
            String noFieldName = inputs[i].substring(inputs[i].indexOf('.') + 1);
            String match = EvaluationPhaseFilterFunctions.getMatchToRightOfPeriod(noFieldName, groupNumber[i]);
            Assert.assertEquals(match, expected[i]);
        }
    }
}
