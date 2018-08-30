package datawave.query;

import datawave.configuration.spring.SpringBean;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.transformer.DocumentTransformer;
import datawave.query.util.WiseGuysIngest;
import datawave.webservice.edgedictionary.TestDatawaveEdgeDictionaryImpl;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.iterator.DatawaveTransformIterator;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;
import datawave.webservice.result.EventQueryResponseBase;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import static datawave.query.QueryTestTableHelper.*;

/**
 * Applies grouping to queries
 * 
 */
public abstract class GroupingTest {
    
    private static final Logger log = Logger.getLogger(GroupingTest.class);
    
    @RunWith(Arquillian.class)
    public static class ShardRange extends GroupingTest {
        protected static Connector connector = null;
        private static Authorizations auths = new Authorizations("ALL");
        
        @BeforeClass
        public static void setUp() throws Exception {
            
            QueryTestTableHelper qtth = new QueryTestTableHelper(GroupingTest.ShardRange.class.toString(), log);
            connector = qtth.connector;
            
            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.SHARD);
            PrintUtility.printTable(connector, auths, SHARD_TABLE_NAME);
            PrintUtility.printTable(connector, auths, SHARD_INDEX_TABLE_NAME);
            PrintUtility.printTable(connector, auths, MODEL_TABLE_NAME);
        }
        
        @Override
        protected void runTestQueryWithGrouping(Map<String,Integer> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms)
                        throws Exception {
            super.runTestQueryWithGrouping(expected, querystr, startDate, endDate, extraParms, connector);
        }
    }
    
    @RunWith(Arquillian.class)
    public static class DocumentRange extends GroupingTest {
        protected static Connector connector = null;
        private static Authorizations auths = new Authorizations("ALL");
        
        @BeforeClass
        public static void setUp() throws Exception {
            
            QueryTestTableHelper qtth = new QueryTestTableHelper(DocumentRange.class.toString(), log);
            connector = qtth.connector;
            
            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.DOCUMENT);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, SHARD_TABLE_NAME);
            PrintUtility.printTable(connector, auths, SHARD_INDEX_TABLE_NAME);
            PrintUtility.printTable(connector, auths, MODEL_TABLE_NAME);
        }
        
        @Override
        protected void runTestQueryWithGrouping(Map<String,Integer> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms)
                        throws Exception {
            super.runTestQueryWithGrouping(expected, querystr, startDate, endDate, extraParms, connector);
        }
    }
    
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
                        .addClass(TestDatawaveEdgeDictionaryImpl.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .deleteClass(datawave.query.metrics.ShardTableQueryMetricHandler.class)
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
    
    protected abstract void runTestQueryWithGrouping(Map<String,Integer> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms)
                    throws Exception;
    
    protected void runTestQueryWithGrouping(Map<String,Integer> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms,
                    Connector connector) throws Exception {
        log.debug("runTestQueryWithGrouping");
        
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
        
        DocumentTransformer transformer = (DocumentTransformer) (logic.getTransformer(settings));
        TransformIterator iter = new DatawaveTransformIterator(logic.iterator(), transformer);
        List<Object> eventList = new ArrayList<>();
        while (iter.hasNext()) {
            eventList.add(iter.next());
        }
        
        BaseQueryResponse response = transformer.createResponse(eventList);
        
        // un-comment to look at the json output
        // ObjectMapper mapper = new ObjectMapper();
        // mapper.enable(MapperFeature.USE_WRAPPER_NAME_AS_PROPERTY_NAME);
        // mapper.writeValue(new File("/tmp/grouped2.json"), response);
        
        Assert.assertTrue(response instanceof DefaultEventQueryResponse);
        DefaultEventQueryResponse eventQueryResponse = (DefaultEventQueryResponse) response;
        
        Assert.assertEquals("Got the wrong number of events", 8, (long) eventQueryResponse.getReturnedEvents());
        
        for (EventBase event : eventQueryResponse.getEvents()) {
            
            String genderKey = "";
            String ageKey = "";
            Integer value = null;
            for (Object field : event.getFields()) {
                FieldBase fieldBase = (FieldBase) field;
                if (fieldBase.getName().equals("COUNT")) {
                    value = Integer.valueOf(fieldBase.getValueString());
                } else if (fieldBase.getName().equals("GEN")) {
                    genderKey = fieldBase.getValueString();
                } else if (fieldBase.getName().equals("AG")) {
                    ageKey = fieldBase.getValueString();
                }
            }
            
            log.debug("mapping is " + genderKey + "-" + ageKey + " count:" + value);
            Assert.assertEquals(expected.get(genderKey + "-" + ageKey), value);
        }
        
    }
    
    @Test
    public void testGrouping() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID =~ '^[CS].*'";
        
        Map<String,Integer> expectedMap = new HashMap<>();
        expectedMap.put("FEMALE-18", 2);
        expectedMap.put("MALE-30", 1);
        expectedMap.put("MALE-34", 1);
        expectedMap.put("MALE-16", 1);
        expectedMap.put("MALE-40", 2);
        expectedMap.put("MALE-20", 2);
        expectedMap.put("MALE-24", 1);
        expectedMap.put("MALE-22", 2);
        
        extraParameters.put("group.fields", "AG,GEN");
        
        runTestQueryWithGrouping(expectedMap, queryString, startDate, endDate, extraParameters);
    }
    
    @Test
    public void testGroupingUsingFunction() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID =~ '^[CS].*' && f:groupby('AG','GEN')";
        
        Map<String,Integer> expectedMap = new HashMap<>();
        expectedMap.put("FEMALE-18", 2);
        expectedMap.put("MALE-30", 1);
        expectedMap.put("MALE-34", 1);
        expectedMap.put("MALE-16", 1);
        expectedMap.put("MALE-40", 2);
        expectedMap.put("MALE-20", 2);
        expectedMap.put("MALE-24", 1);
        expectedMap.put("MALE-22", 2);
        
        runTestQueryWithGrouping(expectedMap, queryString, startDate, endDate, extraParameters);
    }
    
}
