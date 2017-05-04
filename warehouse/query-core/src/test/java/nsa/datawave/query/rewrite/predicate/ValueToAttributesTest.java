package nsa.datawave.query.rewrite.predicate;

import static nsa.datawave.query.rewrite.QueryTestTableHelper.METADATA_TABLE_NAME;
import static nsa.datawave.query.rewrite.QueryTestTableHelper.SHARD_INDEX_TABLE_NAME;
import static nsa.datawave.query.rewrite.QueryTestTableHelper.SHARD_TABLE_NAME;
import static nsa.datawave.query.tables.edge.BaseEdgeQueryTest.MODEL_TABLE_NAME;

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

import javax.inject.Inject;

import nsa.datawave.configuration.spring.SpringBean;
import nsa.datawave.helpers.PrintUtility;
import nsa.datawave.ingest.data.TypeRegistry;
import nsa.datawave.marking.MarkingFunctions;
import nsa.datawave.query.language.parser.ParseException;
import nsa.datawave.query.rewrite.QueryTestTableHelper;
import nsa.datawave.query.rewrite.attributes.Attribute;
import nsa.datawave.query.rewrite.attributes.Document;
import nsa.datawave.query.rewrite.attributes.PreNormalizedAttribute;
import nsa.datawave.query.rewrite.attributes.TypeAttribute;
import nsa.datawave.query.rewrite.function.deserializer.KryoDocumentDeserializer;
import nsa.datawave.query.rewrite.tables.RefactoredShardQueryLogic;
import nsa.datawave.query.rewrite.util.CompositeTestingIngest;
import nsa.datawave.query.util.CompositeMetadata;
import nsa.datawave.query.util.TypeMetadata;
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
    public static class DocumentRange extends ValueToAttributesTest {
        protected static Connector connector = null;
        
        @BeforeClass
        public static void setUp() throws Exception {
            
            QueryTestTableHelper qtth = new QueryTestTableHelper(DocumentRange.class.toString(), log);
            connector = qtth.connector;
            
            CompositeTestingIngest.writeItAll(connector, CompositeTestingIngest.WhatKindaRange.DOCUMENT);
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
    
    private static final Logger log = Logger.getLogger(ValueToAttributesTest.class);
    
    protected Authorizations auths = new Authorizations("ALL");
    
    protected Set<Authorizations> authSet = Collections.singleton(auths);
    
    @Inject
    @SpringBean(name = "EventQuery")
    protected RefactoredShardQueryLogic logic;
    
    protected KryoDocumentDeserializer deserializer;
    
    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    
    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        System.setProperty("cdi.bean.context", "refactoredQueryBeanRefContext.xml");
        
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
        "COLOR == 'RED' && MAKE == 'FORD'"};
        
        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] {Arrays.asList("One"), // family name starts with C or S
        };
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], format.parse("20091231"), format.parse("20150101"), extraParameters);
        }
    }
    
    @Test
    public void testComposites() {
        CompositeMetadata compositeMetadata = new CompositeMetadata(
                        "test:[MAKE:MAKE_COLOR[0];WHEELS:COLOR_WHEELS[1];COLOR:COLOR_WHEELS[0],MAKE_COLOR[1]];pilot:[MAKE:MAKE_COLOR[0];WHEELS:COLOR_WHEELS[1];COLOR:COLOR_WHEELS[0],MAKE_COLOR[1]];work:[MAKE:MAKE_COLOR[0];WHEELS:COLOR_WHEELS[1];COLOR:COLOR_WHEELS[0],MAKE_COLOR[1]];beep:[MAKE:MAKE_COLOR[0];WHEELS:COLOR_WHEELS[1];COLOR:COLOR_WHEELS[0],MAKE_COLOR[1]];tw:[MAKE:MAKE_COLOR[0];WHEELS:COLOR_WHEELS[1];COLOR:COLOR_WHEELS[0],MAKE_COLOR[1]]");
        
        TypeMetadata typeMetadata = new TypeMetadata(
                        "MAKE:[beep:nsa.datawave.data.type.LcNoDiacriticsType];MAKE_COLOR:[beep:nsa.datawave.data.type.NoOpType];START_DATE:[beep:nsa.datawave.data.type.DateType];TYPE_NOEVAL:[beep:nsa.datawave.data.type.LcNoDiacriticsType];IP_ADDR:[beep:nsa.datawave.data.type.IpAddressType];WHEELS:[beep:nsa.datawave.data.type.LcNoDiacriticsType,nsa.datawave.data.type.NumberType];COLOR:[beep:nsa.datawave.data.type.LcNoDiacriticsType];COLOR_WHEELS:[beep:nsa.datawave.data.type.NoOpType];TYPE:[beep:nsa.datawave.data.type.LcNoDiacriticsType]");
        MarkingFunctions markingFunctions = new MarkingFunctions.NoOp();
        ValueToAttributes valueToAttributes = new ValueToAttributes(compositeMetadata, typeMetadata, null, markingFunctions);
    }
}
