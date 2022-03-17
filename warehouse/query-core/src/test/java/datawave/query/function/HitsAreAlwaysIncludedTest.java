package datawave.query.function;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import datawave.configuration.spring.SpringBean;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.QueryTestTableHelper;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.LimitFieldsTestingIngest;
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
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
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
import java.util.stream.Collectors;

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
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }
        
        @Override
        protected void runTestQuery(String queryString, Date startDate, Date endDate, Map<String,String> extraParms, Collection<String> expectedHits,
                        Collection<String> goodResults) throws Exception {
            super.runTestQuery(connector, queryString, startDate, endDate, extraParms, expectedHits, goodResults);
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
            PrintUtility.printTable(connector, auths, TableName.SHARD);
            PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }
        
        @Override
        protected void runTestQuery(String queryString, Date startDate, Date endDate, Map<String,String> extraParms, Collection<String> expectedHits,
                        Collection<String> goodResults) throws Exception {
            super.runTestQuery(connector, queryString, startDate, endDate, extraParms, expectedHits, goodResults);
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
    
    protected abstract void runTestQuery(String queryString, Date startDate, Date endDate, Map<String,String> extraParms, Collection<String> expectedHits,
                    Collection<String> goodResults) throws Exception;
    
    protected void runTestQuery(Connector connector, String queryString, Date startDate, Date endDate, Map<String,String> extraParms,
                    Collection<String> expectedHits, Collection<String> goodResults) throws Exception {
        
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
            
            Attribute hitAttribute = d.get(JexlEvaluation.HIT_TERM_FIELD);
            
            if (hitAttribute instanceof Attributes) {
                Attributes attributes = (Attributes) hitAttribute;
                for (Attribute attr : attributes.getAttributes()) {
                    if (attr instanceof Content) {
                        Content content = (Content) attr;
                        Assert.assertTrue(expectedHits.remove(content.getContent()));
                    }
                }
            } else if (hitAttribute instanceof Content) {
                Content content = (Content) hitAttribute;
                Assert.assertTrue(content.getContent() + " is not an expected hit", expectedHits.remove(content.getContent()));
            } else {
                Assert.fail("Did not find hit term field");
            }
            
            Assert.assertTrue(expectedHits + " expected hits was not empty", expectedHits.isEmpty());
            
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
            
            Assert.assertTrue(goodResults + " good results was not empty", goodResults.isEmpty());
        }
        Assert.assertTrue("No docs were returned!", !docs.isEmpty());
    }
    
    @Test
    public void checkThePattern() {
        String[] tokens = LimitFields.getCommonalityAndGroupingContext("FOO_3.FOO.3.3");
        Assert.assertEquals(2, tokens.length);
        Assert.assertEquals(tokens[0], "FOO");
        Assert.assertEquals(tokens[1], "3");
        
        tokens = LimitFields.getCommonalityAndGroupingContext("FOO_3");
        Assert.assertNull(tokens);
        
        tokens = LimitFields.getCommonalityAndGroupingContext("FOO_3_BAR.FOO.3");
        Assert.assertEquals(2, tokens.length);
        Assert.assertEquals(tokens[0], "FOO");
        Assert.assertEquals(tokens[1], "3");
    }
    
    @Test
    public void testHitForIndexedQueryTerm() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4");
        
        String queryString = "FOO_3_BAR == 'defg<cat>'";
        
        Set<String> goodResults = Sets.newHashSet("FOO_1_BAR.FOO.3:good<cat>", "FOO_3_BAR.FOO.3:defg<cat>", "FOO_3.FOO.3.3:defg", "FOO_4.FOO.4.3:yes",
                        "FOO_1.FOO.1.3:good");
        Set<String> expectedHits = Sets.newHashSet("FOO_3_BAR.FOO.3:defg<cat>");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testHitForIndexedQueryTermWithOptionsInQueryFunction() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        
        String queryString = "FOO_3_BAR == 'defg<cat>' and f:options('include.grouping.context', 'true', "
                        + "'hit.list', 'true', 'limit.fields', 'FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4')";
        
        Set<String> goodResults = Sets.newHashSet("FOO_1_BAR.FOO.3:good<cat>", "FOO_3_BAR.FOO.3:defg<cat>", "FOO_3.FOO.3.3:defg", "FOO_4.FOO.4.3:yes",
                        "FOO_1.FOO.1.3:good");
        Set<String> expectedHits = Sets.newHashSet("FOO_3_BAR.FOO.3:defg<cat>");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testHitForIndexedQueryOnUnrealmed() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4");
        
        String queryString = "FOO_3 == 'defg'";
        
        Set<String> goodResults = Sets.newHashSet("FOO_1_BAR.FOO.3:good<cat>", "FOO_3_BAR.FOO.3:defg<cat>", "FOO_3.FOO.3.3:defg", "FOO_4.FOO.4.3:yes",
                        "FOO_1.FOO.1.3:good");
        Set<String> expectedHits = Sets.newHashSet("FOO_3.FOO.3.3:defg");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
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
        Set<String> expectedHits = Sets.newHashSet("FOO_3_BAR.FOO.3:defg<cat>");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testHitForIndexedAndUnindexedQueryAndAnyfieldLimit() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4");
        
        String queryString = "FOO_3_BAR == 'defg<cat>' and FOO_1 == 'good'";
        
        Set<String> goodResults = Sets.newHashSet("FOO_1_BAR.FOO.3:good<cat>", "FOO_3_BAR.FOO.3:defg<cat>", "FOO_3.FOO.3.3:defg", "FOO_4.FOO.4.3:yes",
                        "FOO_1.FOO.1.3:good");
        Set<String> expectedHits = Sets.newHashSet("FOO_3_BAR.FOO.3:defg<cat>", "FOO_1.FOO.1.3:good");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testHitWithoutGroupingContext() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "false");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4");
        
        String queryString = "FOO_3_BAR == 'defg<cat>'";
        
        // there is no grouping context so i can expect only the original term, not the related ones (in the same group)
        Set<String> goodResults = Sets.newHashSet("FOO_3_BAR:defg<cat>");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, new HashSet<>(goodResults), goodResults);
    }
    
    @Test
    public void testHitWithRange() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "false");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4");
        
        String queryString = "((_Bounded_ = true) && (FOO_1_BAR_1 >= '2021-03-01 00:00:00' && FOO_1_BAR_1 <= '2021-04-01 00:00:00'))";
        
        // there is no grouping context so i can expect only the original term, not the related ones (in the same group)
        Set<String> expectedHits = Sets.newHashSet("FOO_1_BAR_1:Wed Mar 24 16:00:00 GMT 2021");
        Set<String> goodResults = Sets.newHashSet("FOO_1_BAR_1:2021-03-24T16:00:00.000Z");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testHitWithDate() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "false");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4");
        
        String queryString = "FOO_1_BAR_1 == '2021-03-24T16:00:00.000Z'";
        
        // there is no grouping context so i can expect only the original term, not the related ones (in the same group)
        Set<String> expectedHits = Sets.newHashSet("FOO_1_BAR_1:Wed Mar 24 16:00:00 GMT 2021");
        Set<String> goodResults = Sets.newHashSet("FOO_1_BAR_1:2021-03-24T16:00:00.000Z");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testHitWithExceededOrThreshold() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "false");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=4,FOO_4=3,FOO_1_BAR_1=4");
        logic.setMaxOrExpansionThreshold(1);
        ivaratorConfig();
        
        String queryString = "FOO_3_BAR == 'defg<cat>' || FOO_3_BAR == 'abcd<cat>'";
        
        // there is no grouping context so i can expect only the original term, not the related ones (in the same group)
        Set<String> goodResults = Sets.newHashSet("FOO_3_BAR:defg<cat>", "FOO_3_BAR:abcd<cat>");
        Set<String> expectedHits = Sets.newHashSet("FOO_3_BAR:defg<cat>", "FOO_3_BAR:abcd<cat>");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testHitWithFunction() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "false");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=4,FOO_4=3,FOO_1_BAR_1=4");
        logic.setMaxOrExpansionThreshold(1);
        ivaratorConfig();
        
        String queryString = "FOO_3_BAR == 'defg<cat>' || FOO_3_BAR == 'abcd<cat>'";
        
        // there is no grouping context so i can expect only the original term, not the related ones (in the same group)
        Set<String> goodResults = Sets.newHashSet("FOO_3_BAR:defg<cat>", "FOO_3_BAR:abcd<cat>");
        Set<String> expectedHits = Sets.newHashSet("FOO_3_BAR:defg<cat>", "FOO_3_BAR:abcd<cat>");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    protected void ivaratorConfig() throws IOException {
        final URL hdfsConfig = this.getClass().getResource("/testhadoop.config");
        Assert.assertNotNull(hdfsConfig);
        this.logic.setHdfsSiteConfigURLs(hdfsConfig.toExternalForm());
        
        final List<String> dirs = new ArrayList<>();
        final List<String> fstDirs = new ArrayList<>();
        Path ivCache = Paths.get(Files.createTempDir().toURI());
        dirs.add(ivCache.toUri().toString());
        String uriList = String.join(",", dirs);
        log.info("hdfs dirs(" + uriList + ")");
        this.logic.setIvaratorCacheDirConfigs(dirs.stream().map(IvaratorCacheDirConfig::new).collect(Collectors.toList()));
    }
    
}
