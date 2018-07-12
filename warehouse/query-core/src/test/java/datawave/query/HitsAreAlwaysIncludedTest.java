package datawave.query;

import com.google.common.collect.Sets;
import datawave.configuration.spring.SpringBean;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.function.LimitFields;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.LimitFieldsTestingIngest;
import datawave.webservice.edgedictionary.TestDatawaveEdgeDictionaryImpl;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static datawave.query.QueryTestTableHelper.*;

/**
 * Tests the limit.fields feature to ensure that hit terms are always included and that associated fields at the same grouping context are included along with
 * the field that hit on the query
 * 
 */
public abstract class HitsAreAlwaysIncludedTest {
    
    @RunWith(Arquillian.class)
    public static class ShardRange extends HitsAreAlwaysIncludedTest {
        protected static Connector connector = null;
        
        @BeforeClass
        public static void setUp() throws Exception {
            
            QueryTestTableHelper qtth = new QueryTestTableHelper(ShardRange.class.toString(), log);
            connector = qtth.connector;
            
            LimitFieldsTestingIngest.writeItAll(connector, LimitFieldsTestingIngest.WhatKindaRange.SHARD);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, SHARD_TABLE_NAME);
            PrintUtility.printTable(connector, auths, SHARD_INDEX_TABLE_NAME);
            PrintUtility.printTable(connector, auths, MODEL_TABLE_NAME);
        }
        
        @Override
        protected void runTestQuery(String queryString, Date startDate, Date endDate, Map<String,String> extraParms, Collection<String> goodResults)
                        throws Exception {
            super.runTestQuery(connector, queryString, startDate, endDate, extraParms, goodResults);
        }
    }
    
    @RunWith(Arquillian.class)
    public static class DocumentRange extends HitsAreAlwaysIncludedTest {
        protected static Connector connector = null;
        
        @BeforeClass
        public static void setUp() throws Exception {
            
            QueryTestTableHelper qtth = new QueryTestTableHelper(DocumentRange.class.toString(), log);
            connector = qtth.connector;
            
            LimitFieldsTestingIngest.writeItAll(connector, LimitFieldsTestingIngest.WhatKindaRange.DOCUMENT);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(connector, auths, SHARD_TABLE_NAME);
            PrintUtility.printTable(connector, auths, SHARD_INDEX_TABLE_NAME);
            PrintUtility.printTable(connector, auths, MODEL_TABLE_NAME);
        }
        
        @Override
        protected void runTestQuery(String queryString, Date startDate, Date endDate, Map<String,String> extraParms, Collection<String> goodResults)
                        throws Exception {
            super.runTestQuery(connector, queryString, startDate, endDate, extraParms, goodResults);
        }
    }
    
    private static final Logger log = Logger.getLogger(HitsAreAlwaysIncludedTest.class);
    
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
    
    protected abstract void runTestQuery(String queryString, Date startDate, Date endDate, Map<String,String> extraParms, Collection<String> goodResults)
                    throws Exception;
    
    protected void runTestQuery(Connector connector, String queryString, Date startDate, Date endDate, Map<String,String> extraParms,
                    Collection<String> goodResults) throws Exception {
        
        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(startDate);
        settings.setEndDate(endDate);
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(queryString);
        settings.setParameters(extraParms);
        settings.setId(UUID.randomUUID());
        
        log.debug("query: " + settings.getQuery());
        log.debug("logic: " + settings.getQueryLogicName());
        
        GenericQueryConfiguration config = logic.initialize(connector, settings, authSet);
        logic.setupQuery(config);
        
        Set<Document> docs = new HashSet<>();
        for (Entry<Key,Value> entry : logic) {
            Document d = deserializer.apply(entry).getValue();
            log.trace(entry.getKey() + " => " + d);
            docs.add(d);
            
            Attribute hitAttribute = d.get("HIT_TERM");
            
            if (hitAttribute instanceof Attributes) {
                Attributes attributes = (Attributes) hitAttribute;
                for (Attribute attr : attributes.getAttributes()) {
                    if (attr instanceof Content) {
                        Content content = (Content) attr;
                        Assert.assertTrue(goodResults.contains(content.getContent()));
                    }
                }
            } else if (hitAttribute instanceof Content) {
                Content content = (Content) hitAttribute;
                Assert.assertTrue(goodResults.contains(content.getContent()));
            }
            
            // remove from goodResults as we find the expected return fields
            log.debug("goodResults: " + goodResults);
            Map<String,Attribute<? extends Comparable<?>>> dictionary = d.getDictionary();
            log.debug("dictionary:" + dictionary);
            for (Entry<String,Attribute<? extends Comparable<?>>> dictionaryEntry : dictionary.entrySet()) {
                
                Attribute<? extends Comparable<?>> attribute = dictionaryEntry.getValue();
                if (attribute instanceof Attributes) {
                    for (Attribute attr : ((Attributes) attribute).getAttributes()) {
                        String toFind = dictionaryEntry.getKey() + ":" + attr;
                        boolean found = goodResults.remove(toFind);
                        if (found)
                            log.debug("removed " + toFind);
                        else
                            log.debug("Did not remove " + toFind);
                    }
                } else {
                    
                    String toFind = dictionaryEntry.getKey() + ":" + dictionaryEntry.getValue();
                    
                    boolean found = goodResults.remove(toFind);
                    if (found)
                        log.debug("removed " + toFind);
                    else
                        log.debug("Did not remove " + toFind);
                }
                
            }
            
            Assert.assertTrue(goodResults + " was not empty", goodResults.isEmpty());
        }
        Assert.assertTrue("No docs were returned!", docs.size() > 0);
    }
    
    @Test
    public void checkThePattern() throws Exception {
        Pattern pattern = LimitFields.PREFIX_SUFFIX_PATTERN;
        Matcher matcher = pattern.matcher("FOO_3.FOO.3.3");
        Assert.assertTrue(matcher.matches());
        Assert.assertEquals(matcher.group(1), "FOO_");
        Assert.assertEquals(matcher.group(2), "3");
        matcher = pattern.matcher("FOO_3");
        Assert.assertFalse(matcher.matches());
        matcher = pattern.matcher("FOO_3_BAR.FOO.3");
        Assert.assertTrue(matcher.matches());
        Assert.assertEquals(matcher.group(1), "FOO_");
        Assert.assertEquals(matcher.group(2), "3");
    }
    
    @Test
    public void testHitForIndexedQueryTerm() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3");
        
        String queryString = "FOO_3_BAR == 'defg<cat>'";
        
        Set<String> goodResults = Sets.newHashSet("FOO_1_BAR.FOO.3:good<cat>", "FOO_3_BAR.FOO.3:defg<cat>", "FOO_3.FOO.3.3:defg", "FOO_4.FOO.4.3:yes",
                        "FOO_1.FOO.1.3:good");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }
    
    @Test
    public void testHitForIndexedQueryOnUnrealmed() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3");
        
        String queryString = "FOO_3 == 'defg'";
        
        Set<String> goodResults = Sets.newHashSet("FOO_1_BAR.FOO.3:good<cat>", "FOO_3_BAR.FOO.3:defg<cat>", "FOO_3.FOO.3.3:defg", "FOO_4.FOO.4.3:yes",
                        "FOO_1.FOO.1.3:good");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }
    
    @Test
    public void testHitForIndexedQueryAndAnyfieldLimit() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "_ANYFIELD_=2");
        
        String queryString = "FOO_3_BAR == 'defg<cat>'";
        
        Set<String> goodResults = Sets.newHashSet("FOO_1_BAR.FOO.3:good<cat>", "FOO_3_BAR.FOO.3:defg<cat>", "FOO_3.FOO.3.3:defg", "FOO_4.FOO.4.3:yes",
                        "FOO_1.FOO.1.3:good");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }
    
    @Test
    public void testHitForIndexedAndUnindexedQueryAndAnyfieldLimit() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3");
        
        String queryString = "FOO_3_BAR == 'defg<cat>' and FOO_1 == 'good'";
        
        Set<String> goodResults = Sets.newHashSet("FOO_1_BAR.FOO.3:good<cat>", "FOO_3_BAR.FOO.3:defg<cat>", "FOO_3.FOO.3.3:defg", "FOO_4.FOO.4.3:yes",
                        "FOO_1.FOO.1.3:good");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }
    
    @Test
    public void testHitWithoutGroupingContext() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "false");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3");
        
        String queryString = "FOO_3_BAR == 'defg<cat>'";
        
        // there is no grouping context so i can expect only the original term, not the related ones (in the same group)
        Set<String> goodResults = Sets.newHashSet("FOO_3_BAR:defg<cat>");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, goodResults);
    }
    
}
