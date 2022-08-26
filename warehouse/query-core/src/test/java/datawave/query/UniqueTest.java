package datawave.query;

import com.google.common.collect.Sets;
import datawave.configuration.spring.SpringBean;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.exceptions.InvalidQueryException;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.transformer.DocumentTransformer;
import datawave.query.util.WiseGuysIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.iterator.DatawaveTransformIterator;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit5.ArquillianExtension;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.inject.Inject;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Applies uniqueness to queries
 *
 */
public abstract class UniqueTest {
    
    private static final Logger log = Logger.getLogger(UniqueTest.class);
    
    @Disabled
    @ExtendWith(ArquillianExtension.class)
    public static class ShardRange extends UniqueTest {
        protected static Connector connector = null;
        private static final Authorizations auths = new Authorizations("ALL");
        
        @BeforeAll
        public static void setUp() throws Exception {
            
            // testing tear downs but without consistency, because when we tear it down then we loose the ongoing bloom filter and subsequently the rebuild will
            // start returning
            // different keys.
            QueryTestTableHelper qtth = new QueryTestTableHelper(ShardRange.class.toString(), log,
                            RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER_SANS_CONSISTENCY, RebuildingScannerTestHelper.INTERRUPT.EVERY_OTHER);
            connector = qtth.connector;
            
            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }
        
        @Override
        protected void runTestQueryWithUniqueness(Set<Set<String>> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms)
                        throws Exception {
            super.runTestQueryWithUniqueness(expected, querystr, startDate, endDate, extraParms, connector);
        }
    }
    
    @Disabled
    @ExtendWith(ArquillianExtension.class)
    public static class DocumentRange extends UniqueTest {
        protected static Connector connector = null;
        private static final Authorizations auths = new Authorizations("ALL");
        
        @BeforeAll
        public static void setUp() throws Exception {
            
            // testing tear downs but without consistency, because when we tear it down then we loose the ongoing bloom filter and subsequently the rebuild will
            // start returning
            // different keys.
            QueryTestTableHelper qtth = new QueryTestTableHelper(DocumentRange.class.toString(), log,
                            RebuildingScannerTestHelper.TEARDOWN.EVERY_OTHER_SANS_CONSISTENCY, RebuildingScannerTestHelper.INTERRUPT.EVERY_OTHER);
            connector = qtth.connector;
            
            WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.DOCUMENT);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }
        
        @Override
        protected void runTestQueryWithUniqueness(Set<Set<String>> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms)
                        throws Exception {
            super.runTestQueryWithUniqueness(expected, querystr, startDate, endDate, extraParms, connector);
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
                        .deleteClass(DefaultEdgeEventQueryLogic.class)
                        .deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .deleteClass(datawave.query.metrics.ShardTableQueryMetricHandler.class)
                        .addAsManifestResource(
                                        new StringAsset("<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>"
                                                        + "</alternatives>"), "beans.xml");
    }
    
    @AfterAll
    public static void teardown() {
        TypeRegistry.reset();
    }
    
    @BeforeEach
    public void setup() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        
        logic.setFullTableScanEnabled(true);
        deserializer = new KryoDocumentDeserializer();
    }
    
    protected abstract void runTestQueryWithUniqueness(Set<Set<String>> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms)
                    throws Exception;
    
    protected void runTestQueryWithUniqueness(Set<Set<String>> expected, String querystr, Date startDate, Date endDate, Map<String,String> extraParms,
                    Connector connector) throws Exception {
        log.debug("runTestQueryWithUniqueness");
        
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
        
        Assertions.assertTrue(response instanceof DefaultEventQueryResponse);
        DefaultEventQueryResponse eventQueryResponse = (DefaultEventQueryResponse) response;
        
        for (EventBase event : eventQueryResponse.getEvents()) {
            boolean found = false;
            for (Iterator<Set<String>> it = expected.iterator(); it.hasNext();) {
                Set<String> expectedSet = it.next();
                
                if (expectedSet.contains(event.getMetadata().getInternalId())) {
                    it.remove();
                    found = true;
                    break;
                }
            }
            Assertions.assertTrue(found);
        }
        Assertions.assertTrue(expected.isEmpty());
    }
    
    @Test
    public void testUniqueness() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID =~ '^[CS].*'";
        
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID, WiseGuysIngest.corleoneUID, WiseGuysIngest.caponeUID));
        extraParameters.put("unique.fields", "DEATH_DATE,$MAGIC");
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);
        
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.corleoneUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.caponeUID));
        extraParameters.put("unique.fields", "$DEATH_DATE,BIRTH_DATE");
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);
        
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.corleoneUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.caponeUID));
        extraParameters.put("unique.fields", "death_date,birth_date");
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);
    }
    
    @Test
    public void testUniquenessWithMissingField() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID =~ '^[CS].*'";
        
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID, WiseGuysIngest.corleoneUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.caponeUID));
        extraParameters.put("unique.fields", "NUMBER");
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);
        
    }
    
    @Test
    public void testUniquenessUsingFunction() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID =~ '^[CS].*' && f:unique($DEATH_DATE,MAGIC)";
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID, WiseGuysIngest.corleoneUID, WiseGuysIngest.caponeUID));
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);
        
        queryString = "UUID =~ '^[CS].*' && f:unique('DEATH_DATE','$BIRTH_DATE')";
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.corleoneUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.caponeUID));
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);
        
        queryString = "UUID =~ '^[CS].*' && f:unique('death_date','$birth_date')";
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.corleoneUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.caponeUID));
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);
    }
    
    @Test
    public void testUniquenessUsingLuceneFunction() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("query.syntax", "LUCENE");
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID:/^[CS].*/ AND #UNIQUE(DEATH_DATE,$MAGIC)";
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID, WiseGuysIngest.corleoneUID, WiseGuysIngest.caponeUID));
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);
        
        queryString = "UUID:/^[CS].*/ AND #UNIQUE(DEATH_DATE,$BIRTH_DATE)";
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.corleoneUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.caponeUID));
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);
        
        queryString = "UUID:/^[CS].*/ AND #UNIQUE(death_date,birth_date)";
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.corleoneUID));
        expected.add(Sets.newHashSet(WiseGuysIngest.caponeUID));
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);
    }
    
    @Test
    public void testUniquenessWithBadField() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("query.syntax", "LUCENE");
        
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID:/^[CS].*/ AND #UNIQUE(FOO_BAR,$MAGIC)";
        runTestQueryWithUniqueness(new HashSet(), queryString, startDate, endDate, extraParameters);
        
        String queryString2 = "UUID:/^[CS].*/ AND #UNIQUE(foo_bar,$magic)";
        Assertions.assertThrows(InvalidQueryException.class, () -> {
            runTestQueryWithUniqueness(new HashSet(), queryString2, startDate, endDate, extraParameters);
        });
    }
    
    @Test
    public void testUniquenessWithHitTermField() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("query.syntax", "LUCENE");
        
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID, WiseGuysIngest.corleoneUID, WiseGuysIngest.caponeUID));
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID:/^[CS].*/ AND #UNIQUE(HIT_TERM)";
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);
    }
    
    @Test
    public void testUniquenessWithModelAliases() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("query.syntax", "LUCENE");
        
        Set<Set<String>> expected = new HashSet<>();
        expected.add(Sets.newHashSet(WiseGuysIngest.sopranoUID, WiseGuysIngest.corleoneUID, WiseGuysIngest.caponeUID));
        Date startDate = format.parse("20091231");
        Date endDate = format.parse("20150101");
        
        String queryString = "UUID:/^[CS].*/ AND #UNIQUE(BOTH_NULL)";
        runTestQueryWithUniqueness(expected, queryString, startDate, endDate, extraParameters);
    }
}
