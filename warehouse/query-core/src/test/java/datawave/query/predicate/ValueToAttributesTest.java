package datawave.query.predicate;

import datawave.configuration.spring.SpringBean;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.marking.MarkingFunctions;
import datawave.query.QueryTestTableHelper;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.attributes.TypeAttribute;
import datawave.query.composite.CompositeMetadata;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.language.parser.ParseException;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.BaseEdgeQueryTest;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.CompositeTestingIngest;
import datawave.query.util.TypeMetadata;
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
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

/**
  */
public abstract class ValueToAttributesTest {
    
    @RunWith(Arquillian.class)
    public static class ShardRange extends ValueToAttributesTest {
        protected static Connector connector = null;
        
        @BeforeClass
        public static void setUp() throws Exception {
            
            QueryTestTableHelper qtth = new QueryTestTableHelper(ShardRange.class.toString(), log);
            connector = qtth.connector;
            
            CompositeTestingIngest.writeItAll(connector, CompositeTestingIngest.WhatKindaRange.SHARD);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, BaseEdgeQueryTest.MODEL_TABLE_NAME);
        }
        
        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, connector);
        }
    }
    
    @RunWith(Arquillian.class)
    public static class DocumentRange extends ValueToAttributesTest {
        protected static Connector connector = null;
        
        @BeforeClass
        public static void setUp() throws Exception {
            
            QueryTestTableHelper qtth = new QueryTestTableHelper(DocumentRange.class.toString(), log);
            connector = qtth.connector;
            
            CompositeTestingIngest.writeItAll(connector, CompositeTestingIngest.WhatKindaRange.DOCUMENT);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, BaseEdgeQueryTest.MODEL_TABLE_NAME);
        }
        
        @Override
        protected void runTestQuery(List<String> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms) throws ParseException,
                        Exception {
            super.runTestQuery(expected, querystr, startDate, endDate, extraParms, connector);
        }
    }
    
    private static final Logger log = Logger.getLogger(ValueToAttributesTest.class);
    
    protected Authorizations auths = new Authorizations("ALL");
    
    protected Set<Authorizations> authSet = Collections.singleton(auths);
    
    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;
    
    protected KryoDocumentDeserializer deserializer;
    
    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    
    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        System.setProperty("cdi.bean.context", "queryBeanRefContext.xml");
        
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
        
        GenericQueryConfiguration config = logic.initialize(connector, settings, authSet);
        logic.setupQuery(config);
        
        String plannedScript = logic.getQueryPlanner().getPlannedScript();
        Assert.assertTrue("CompositeTerm was not substituted into query:" + plannedScript, plannedScript.contains("MAKE_COLOR"));
        
        HashSet<String> expectedSet = new HashSet<>(expected);
        HashSet<String> resultSet;
        resultSet = new HashSet<>();
        Set<Document> docs = new HashSet<>();
        for (Map.Entry<Key,Value> entry : logic) {
            Document d = deserializer.apply(entry).getValue();
            
            log.trace(entry.getKey() + " => " + d);
            
            Attribute<?> attr = d.get("UUID");
            if (attr == null)
                attr = d.get("UUID.0");
            
            Assert.assertNotNull("Result Document did not contain a 'UUID'", attr);
            Assert.assertTrue("Expected result to be an instance of DatwawaveTypeAttribute, was: " + attr.getClass().getName(), attr instanceof TypeAttribute
                            || attr instanceof PreNormalizedAttribute);
            
            TypeAttribute<?> uuidAttr = (TypeAttribute<?>) attr;
            
            String uuid = uuidAttr.getType().getDelegate().toString();
            Assert.assertTrue("Received unexpected UUID: " + uuid, expected.contains(uuid));
            
            resultSet.add(uuid);
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
    public void testCompositeFunctions() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        
        if (log.isDebugEnabled()) {
            log.debug("testCompositeFunctions");
        }
        String[] queryStrings = { //
        "COLOR == 'RED' && MAKE == 'FORD'", //
                "COLOR == 'BLUE' && MAKE == 'CHEVY'", //
                "COLOR == 'BLUE' && MAKE == 'FORD'", //
        };
        
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] { //
        Arrays.asList("One"), //
                Arrays.asList("One"), //
                Arrays.asList("One")//
        };
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
    
    @Test
    public void testComposites() {
        CompositeMetadata compositeMetadata = new CompositeMetadata();
        for (String ingestType : new String[] {"test", "pilot", "work", "beep", "tw"}) {
            compositeMetadata.setCompositeFieldMappingByType(ingestType, "MAKE_COLOR", Arrays.asList("MAKE", "COLOR"));
            compositeMetadata.setCompositeFieldMappingByType(ingestType, "COLOR_WHEELS", Arrays.asList("MAKE", "COLOR"));
        }
        
        TypeMetadata typeMetadata = new TypeMetadata(
                        "MAKE:[beep:datawave.data.type.LcNoDiacriticsType];MAKE_COLOR:[beep:datawave.data.type.NoOpType];START_DATE:[beep:datawave.data.type.DateType];TYPE_NOEVAL:[beep:datawave.data.type.LcNoDiacriticsType];IP_ADDR:[beep:datawave.data.type.IpAddressType];WHEELS:[beep:datawave.data.type.LcNoDiacriticsType,datawave.data.type.NumberType];COLOR:[beep:datawave.data.type.LcNoDiacriticsType];COLOR_WHEELS:[beep:datawave.data.type.NoOpType];TYPE:[beep:datawave.data.type.LcNoDiacriticsType]");
        MarkingFunctions markingFunctions = new MarkingFunctions.Default();
        ValueToAttributes valueToAttributes = new ValueToAttributes(compositeMetadata, typeMetadata, null, markingFunctions);
    }
}
