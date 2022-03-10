package datawave.query;

import datawave.configuration.spring.SpringBean;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.attributes.TypeAttribute;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
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
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.inject.Inject;
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
 * Loads some data in a mock accumulo table and then issues queries against the table using the shard query table.
 * 
 */
public abstract class FunctionalSetTest {
    
    @RunWith(Arquillian.class)
    public static class ShardRange extends FunctionalSetTest {
        protected static Connector connector = null;
        
        @BeforeClass
        public static void setUp() throws Exception {
            QueryTestTableHelper qtth = new QueryTestTableHelper(FunctionalSetTest.ShardRange.class.toString(), log);
            connector = qtth.connector;
            
            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.SHARD);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }
        
        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, connector);
        }
    }
    
    @RunWith(Arquillian.class)
    public static class DocumentRange extends FunctionalSetTest {
        protected static Connector connector = null;
        
        @BeforeClass
        public static void setUp() throws Exception {
            QueryTestTableHelper qtth = new QueryTestTableHelper(FunctionalSetTest.DocumentRange.class.toString(), log);
            connector = qtth.connector;
            
            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.DOCUMENT);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }
        
        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, connector);
        }
    }
    
    private static final Logger log = Logger.getLogger(FunctionalSetTest.class);
    
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
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class)
                        .deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
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
        
        logic.setFullTableScanEnabled(true);
        
        deserializer = new KryoDocumentDeserializer();
    }
    
    protected abstract void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws Exception;
    
    protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms, Connector connector)
                    throws Exception {
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
        logic.setMaxDepthThreshold(6);
        
        GenericQueryConfiguration config = logic.initialize(connector, settings, authSet);
        logic.setupQuery(config);
        HashSet<String> expectedSet = new HashSet<>(expected);
        HashSet<String> resultSet;
        resultSet = new HashSet<>();
        Set<Document> docs = new HashSet<>();
        for (Entry<Key,Value> entry : logic) {
            Document d = deserializer.apply(entry).getValue();
            
            log.debug(entry.getKey() + " => " + d);
            
            Attribute<?> attr = d.get("UUID.0");
            
            Assert.assertNotNull("Result Document did not contain a 'UUID'", attr);
            Assert.assertTrue("Expected result to be an instance of DatwawaveTypeAttribute, was: " + attr.getClass().getName(), attr instanceof TypeAttribute
                            || attr instanceof PreNormalizedAttribute);
            
            TypeAttribute<?> UUIDAttr = (TypeAttribute<?>) attr;
            
            String UUID = UUIDAttr.getType().getDelegate().toString();
            Assert.assertTrue("Received unexpected UUID for query:" + querystr + "  " + UUID, expected.contains(UUID));
            
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
        Assert.assertEquals("Unexpected number of records for query:" + querystr, expected.size(), resultSet.size());
    }
    
    @Test
    public void testMethodAsArgumentToMethod() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("testMethodAsArgumentToMethod");
        }
        // @formatter:off
        String[] queryStrings = {
                // this makes sure that the JexlStringBuildingVisitor will parse the method as a method argument
                "AG.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAM, 'MEADOW', GEN, 'FEMALE')).isEmpty() == false && "
                        + "AG.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAM, 'MEADOW', GEN, 'FEMALE')).containsAll(AG.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAM, 'MEADOW', GEN, 'FEMALE'))) == true"
        };
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {
                Arrays.asList("SOPRANO")
        };
        // @formatter:on
        
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
    
    @Test
    public void testMinMax() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("testMinMax");
        }
        // @formatter:off
        String[] queryStrings = {
                "AG.min() > 10", // model expands to AGE.min() > 10 || ETA.min() > 10
                "AG.max() == 40",
                "AG.max() >= 40",
                "AG.min() < 10",
                
                "AG.greaterThan(39).size() >= 1",
                "AG.compareWith(40,'==').size() == 1",
                
                "BIRTH_DATE.min() < '1920-12-28T00:00:05.000Z'",
                "DEATH_DATE.max() - BIRTH_DATE.min() > 1000*60*60*24", // one day
                "DEATH_DATE.max() - BIRTH_DATE.min() > 1000*60*60*24*5 + 1000*60*60*24*7", // 5 plus 7 days for the calculator-deprived
                "DEATH_DATE.min() < '20160301120000'",
                
                "AG.size() > 0", // model expands to AGE.size() > 0 || ETA.size() > 0
                "ETA.size() > 0",
                "AGE.size() > 0"
        };
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {
                Arrays.asList("ANDOLINI", "SOPRANO", "CORLEONE", "CAPONE"),
                Arrays.asList("CORLEONE", "CAPONE"),
                Arrays.asList("CORLEONE", "CAPONE"),
                Arrays.asList(),
                
                Arrays.asList("CORLEONE", "CAPONE"),
                Arrays.asList("CORLEONE", "CAPONE"),
                
                Arrays.asList("CAPONE"),
                Arrays.asList("SOPRANO", "CORLEONE", "CAPONE", "ANDOLINI"),
                Arrays.asList("SOPRANO", "CORLEONE", "CAPONE", "ANDOLINI"),
                Arrays.asList("SOPRANO", "CORLEONE", "CAPONE", "ANDOLINI"),
                
                Arrays.asList("SOPRANO", "CORLEONE", "CAPONE", "ANDOLINI"),
                Arrays.asList("CORLEONE", "ANDOLINI"),
                Arrays.asList("SOPRANO", "CAPONE"),};
        // @formatter:on
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
    
    @Test
    public void testFunctionsAsArguments() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        if (log.isDebugEnabled()) {
            log.debug("testFunctionsAsArguments");
        }
        // @formatter:off
        String[] queryStrings = {
                
                "10 <= AG && AG <= 18",
                "AG <= 18 && AG >= 10",
                "18 >= AG && 10 <= AG",
                // "AG <= 18",
                // "18 >= AG",
                "AG == 18",
                "18 == AG",
                "GEN == 'FEMALE'", // this succeeds because the literal 'FEMALE' is normalized to 'female' based on the type
                                   // (LcNoDiacritics) of the GENDER
                                   // field
                "GEN == 'female'", // this succeeds for the same reason as above. normalization was a no-op.
                "'female' == GEN", // this succeeds because no normalization is necessary
                "'FEMALE' == GEN",

                // the next one matches Meadow Soprano, age 18, because the 'MAGIC' value is 18 (we don't know/care what the actual value
                // of MAGIC is, only that whatever it is, it matches AGE in the same group as the other matches)
                "AG > 10 && AG < 100 && AG.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAM, 'MEADOW', GEN, 'FEMALE')) == MAGIC",

                // the next one matches Meadow Soprano, GENDER female, age 18 but not Constanza Corleone, Gender female, age 18
                // the < part of this is what is special. Other comparison operators should work the same way
                "AG > 10 && AG < 100 && AG.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAM, 'MEADOW', GEN, 'FEMALE')) < 19",

                // the next 2 queries are equivalent. the reason for the functional query stuff is for when we
                // want to query with an operator other than '=='
                "AG > 10 && AG < 100 && AG.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAM, 'ALPHONSE', GEN, 'MALE')) == 30",
                "AG > 10 && AG < 100 && grouping:matchesInGroup(NAM, 'ALPHONSE', GEN, 'MALE', AG, 30)",
                
                "AG > 10 && AG < 100 && filter:occurrence(AG, '==', filter:getAllMatches(AG, '16').size() + filter:getAllMatches(AG, '18').size())", // will
                                                                                                                                                     // match
                                                                                                                                                     // only the
                                                                                                                                                     // sopranos
                "AG > 10 && AG < 100 && filter:occurrence(AG, '==', filter:getAllMatches(AG, '19').size() + filter:getAllMatches(AG, '18').size())" // will
                                                                                                                                                    // match
                                                                                                                                                    // none
        };
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {
        
                Arrays.asList("SOPRANO", "CORLEONE", "ANDOLINI"), // "10 <= AG && AG <= 18"
                Arrays.asList("SOPRANO", "CORLEONE", "ANDOLINI"), // "10 <= AG && AG <= 18",
                Arrays.asList("SOPRANO", "CORLEONE", "ANDOLINI"), // "18 >= AG && 10 <= AG",
                Arrays.asList("SOPRANO", "CORLEONE"), // "AGE == 18"
                Arrays.asList("SOPRANO", "CORLEONE"), // "18 == AGE"
                Arrays.asList("SOPRANO", "CORLEONE"), // "GENDER == 'FEMALE'"
                Arrays.asList("SOPRANO", "CORLEONE"), // "GENDER == 'female'"
                Arrays.asList("SOPRANO", "CORLEONE"), // "'female' == GENDER"
                Arrays.asList("SOPRANO", "CORLEONE"), // "'FEMALE' == GENDER"

                Arrays.asList("SOPRANO"), // "AGE.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAME, 'MEADOW', GENDER, 'FEMALE')) == MAGIC",
                Arrays.asList("SOPRANO"), // "AGE.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAME, 'MEADOW', GENDER, 'FEMALE')) < 19"
                Arrays.asList("CAPONE"), // "AGE.getValuesForGroups(grouping:getGroupsForMatchesInGroup(NAME, 'ALPHONSE', GENDER, 'MALE')) == 30"
                Arrays.asList("CAPONE"), // "grouping:matchesInGroup(NAME, 'ALPHONSE', GENDER, 'MALE', AGE, 30)"
                
                Arrays.asList("SOPRANO"), // "grouping:matchesInGroup(NAME, 'ALPHONSE', GENDER, 'MALE', AGE, 30)"
                Arrays.asList() // "grouping:matchesInGroup(NAME, 'ALPHONSE', GENDER, 'MALE', AGE, 30)"
        };
        // @formatter:on
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
    
    @Test
    public void testConcatMethods() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("testConcatMethods");
        }
        // @formatter:off
        String[] queryStrings = {
                "UUID == 'SOPRANO' && NAM.min().hashCode() != 0",
        };
        List<String>[] expectedLists = new List[] {
                Arrays.asList("SOPRANO"),
        };
        // @formatter:on
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
}
