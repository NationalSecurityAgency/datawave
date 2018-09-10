package datawave.query;

import com.google.common.collect.ImmutableMap;
import datawave.configuration.spring.SpringBean;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.language.parser.jexl.JexlControlledQueryParser;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
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
import org.apache.accumulo.core.client.Connector;
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
    public static JavaArchive createDeployment() {
        
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
        logic.setMaxEvaluationPipelines(1);
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
        // logic.setMaxEvaluationPipelines(1);
        
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
        
        Assert.assertEquals("Got the wrong number of events", expected.size(), (long) eventQueryResponse.getReturnedEvents());
        
        for (EventBase event : eventQueryResponse.getEvents()) {
            
            String genderKey = "";
            String ageKey = "";
            Integer value = null;
            for (Object field : event.getFields()) {
                FieldBase fieldBase = (FieldBase) field;
                switch (fieldBase.getName()) {
                    case "COUNT":
                        value = Integer.valueOf(fieldBase.getValueString());
                        break;
                    case "GEN":
                        genderKey = fieldBase.getValueString();
                        break;
                    case "AG":
                        ageKey = fieldBase.getValueString();
                        break;
                }
            }
            
            log.debug("mapping is " + genderKey + "-" + ageKey + " count:" + value);
            String key;
            if (genderKey.length() > 0 && ageKey.length() > 0) {
                key = genderKey + "-" + ageKey;
            } else if (genderKey.length() > 0) {
                key = genderKey;
            } else {
                key = ageKey;
            }
            Assert.assertEquals(expected.get(key), value);
        }
        
    }
    
    @Test
    public void testGrouping() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID =~ '^[CS].*' && f:options('group.fields.batch.size','6')";
        
        // @formatter:off
        Map<String,Integer> expectedMap = ImmutableMap.<String,Integer> builder()
                .put("FEMALE-18", 2)
                .put("MALE-30", 1)
                .put("MALE-34", 1)
                .put("MALE-16", 1)
                .put("MALE-40", 2)
                .put("MALE-20", 2)
                .put("MALE-24", 1)
                .put("MALE-22", 2)
                .build();
        // @formatter:on
        
        extraParameters.put("group.fields", "AG,GEN");
        
        runTestQueryWithGrouping(expectedMap, queryString, startDate, endDate, extraParameters);
    }
    
    @Test
    public void testGrouping2() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID =~ '^[CS].*'";
        
        // @formatter:off
        Map<String,Integer> expectedMap = ImmutableMap.<String,Integer> builder()
                .put("18", 2)
                .put("30", 1)
                .put("34", 1)
                .put("16", 1)
                .put("40", 2)
                .put("20", 2)
                .put("24", 1)
                .put("22", 2)
                .build();
        // @formatter:on
        extraParameters.put("group.fields", "AG");
        
        runTestQueryWithGrouping(expectedMap, queryString, startDate, endDate, extraParameters);
    }
    
    @Test
    public void testGrouping3() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID =~ '^[CS].*'";
        
        Map<String,Integer> expectedMap = ImmutableMap.of("MALE", 10, "FEMALE", 2);
        
        extraParameters.put("group.fields", "GEN");
        
        runTestQueryWithGrouping(expectedMap, queryString, startDate, endDate, extraParameters);
    }
    
    @Test
    public void testGrouping4() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID =~ '^[CS].*'";
        
        Map<String,Integer> expectedMap = ImmutableMap.of("MALE", 10, "FEMALE", 2);
        
        extraParameters.put("group.fields", "GEN");
        
        runTestQueryWithGrouping(expectedMap, queryString, startDate, endDate, extraParameters);
    }
    
    @Test
    public void testGroupingUsingFunction() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID =~ '^[CS].*' && f:groupby('AG','GEN')";
        
        // @formatter:off
        Map<String,Integer> expectedMap = ImmutableMap.<String,Integer> builder()
                .put("FEMALE-18", 2)
                .put("MALE-30", 1)
                .put("MALE-34", 1)
                .put("MALE-16", 1)
                .put("MALE-40", 2)
                .put("MALE-20", 2)
                .put("MALE-24", 1)
                .put("MALE-22", 2)
                .build();
        // @formatter:on
        
        runTestQueryWithGrouping(expectedMap, queryString, startDate, endDate, extraParameters);
    }
    
    @Test
    public void testGroupingUsingLuceneFunction() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "(UUID:C* or UUID:S* ) and #GROUPBY('AG','GEN')";
        
        // @formatter:off
        Map<String,Integer> expectedMap = ImmutableMap.<String,Integer> builder()
                .put("FEMALE-18", 2)
                .put("MALE-30", 1)
                .put("MALE-34", 1)
                .put("MALE-16", 1)
                .put("MALE-40", 2)
                .put("MALE-20", 2)
                .put("MALE-24", 1)
                .put("MALE-22", 2)
                .build();
        // @formatter:on
        logic.setParser(new LuceneToJexlQueryParser());
        runTestQueryWithGrouping(expectedMap, queryString, startDate, endDate, extraParameters);
        logic.setParser(new JexlControlledQueryParser());
    }
    
}
