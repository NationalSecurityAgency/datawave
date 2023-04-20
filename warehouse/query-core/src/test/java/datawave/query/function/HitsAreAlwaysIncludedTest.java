package datawave.query.function;

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
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
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

/**
 * Tests the {@code limit.fields} feature to ensure that hit terms are always included and that associated fields at the same grouping context are included
 * along with the field that hit on the query
 * 
 * @see LimitFields
 */
public abstract class HitsAreAlwaysIncludedTest {
    
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    
    @RunWith(Arquillian.class)
    public static class ShardRange extends HitsAreAlwaysIncludedTest {
        protected static AccumuloClient client = null;
        
        @BeforeClass
        public static void setUp() throws Exception {
            
            QueryTestTableHelper qtth = new QueryTestTableHelper(ShardRange.class.toString(), log);
            client = qtth.client;
            
            LimitFieldsTestingIngest.writeItAll(client, LimitFieldsTestingIngest.WhatKindaRange.SHARD);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(client, auths, TableName.SHARD);
            PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }
        
        @Override
        protected void runTestQuery(String queryString, Date startDate, Date endDate, Map<String,String> extraParms, Collection<String> expectedHits,
                        Collection<String> goodResults) throws Exception {
            super.runTestQuery(client, queryString, startDate, endDate, extraParms, expectedHits, goodResults);
        }
    }
    
    @RunWith(Arquillian.class)
    public static class DocumentRange extends HitsAreAlwaysIncludedTest {
        protected static AccumuloClient client = null;
        
        @BeforeClass
        public static void setUp() throws Exception {
            
            QueryTestTableHelper qtth = new QueryTestTableHelper(DocumentRange.class.toString(), log);
            client = qtth.client;
            
            LimitFieldsTestingIngest.writeItAll(client, LimitFieldsTestingIngest.WhatKindaRange.DOCUMENT);
            Authorizations auths = new Authorizations("ALL");
            PrintUtility.printTable(client, auths, TableName.SHARD);
            PrintUtility.printTable(client, auths, TableName.SHARD_INDEX);
            PrintUtility.printTable(client, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
        }
        
        @Override
        protected void runTestQuery(String queryString, Date startDate, Date endDate, Map<String,String> extraParms, Collection<String> expectedHits,
                        Collection<String> goodResults) throws Exception {
            super.runTestQuery(client, queryString, startDate, endDate, extraParms, expectedHits, goodResults);
        }
    }
    
    private static final Logger log = Logger.getLogger(HitsAreAlwaysIncludedTest.class);
    
    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;
    
    private final Authorizations auths = new Authorizations("ALL");
    private final Set<Authorizations> authSet = Collections.singleton(auths);
    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    private KryoDocumentDeserializer deserializer;
    
    private String query;
    private Date startDate;
    private Date endDate;
    private final Map<String,String> queryParameters = new HashMap<>();
    private final Collection<String> expectedHits = new HashSet<>();
    private final Collection<String> expectedResults = new HashSet<>();
    
    /**
     * This should be implemented by {@link ShardRange} and {@link DocumentRange}.
     * 
     * @return the connector to use when running tests
     */
    public abstract Connector getConnector();
    
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
    
    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }
    
    @Before
    public void setup() throws ParseException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        logic.setFullTableScanEnabled(true);
        deserializer = new KryoDocumentDeserializer();
        startDate = format.parse("20091231");
        endDate = format.parse("20150101");
    }
    
    @After
    public void tearDown() {
        query = null;
        startDate = null;
        endDate = null;
        queryParameters.clear();
        expectedResults.clear();
        expectedHits.clear();
    }
    
    @Test
    public void testHitForIndexedQueryTerm() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>'");
        
        givenQueryParameter("include.grouping.context", "true");
        givenQueryParameter("hit.list", "true");
        givenQueryParameter("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4");
        
        givenExpectedResult("FOO_1_BAR.FOO.3:good<cat>");
        givenExpectedResult("FOO_3_BAR.FOO.3:defg<cat>");
        givenExpectedResult("FOO_3.FOO.3.3:defg");
        givenExpectedResult("FOO_4.FOO.4.3:yes");
        givenExpectedResult("FOO_1.FOO.1.3:good");
        
        givenExpectedHit("FOO_3_BAR.FOO.3:defg<cat>");
        
        runTest();
    }
    
    @Test
    public void testHitForIndexedQueryTermWithOptionsInQueryFunction() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>' and f:options('include.grouping.context', 'true', 'hit.list', 'true', "
                        + "'limit.fields', 'FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4')");
        
        givenExpectedResult("FOO_1_BAR.FOO.3:good<cat>");
        givenExpectedResult("FOO_3_BAR.FOO.3:defg<cat>");
        givenExpectedResult("FOO_3.FOO.3.3:defg");
        givenExpectedResult("FOO_4.FOO.4.3:yes");
        givenExpectedResult("FOO_1.FOO.1.3:good");
        
        givenExpectedHit("FOO_3_BAR.FOO.3:defg<cat>");
        
        runTest();
    }
    
    @Test
    public void testHitForIndexedQueryOnUnrealmed() throws Exception {
        givenQuery("FOO_3 == 'defg'");
        
        givenQueryParameter("hit.list", "true");
        givenQueryParameter("include.grouping.context", "true");
        givenQueryParameter("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4");
        
        givenExpectedResult("FOO_1_BAR.FOO.3:good<cat>");
        givenExpectedResult("FOO_3_BAR.FOO.3:defg<cat>");
        givenExpectedResult("FOO_3.FOO.3.3:defg");
        givenExpectedResult("FOO_4.FOO.4.3:yes");
        givenExpectedResult("FOO_1.FOO.1.3:good");
        
        givenExpectedHit("FOO_3.FOO.3.3:defg");
        
        runTest();
    }
    
    @Test
    public void testHitForIndexedQueryAndAnyfieldLimit() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>'");
        
        givenQueryParameter("include.grouping.context", "true");
        givenQueryParameter("hit.list", "true");
        givenQueryParameter("limit.fields", "_ANYFIELD_=2");
        
        givenExpectedResult("FOO_1_BAR.FOO.3:good<cat>");
        givenExpectedResult("FOO_3_BAR.FOO.3:defg<cat>");
        givenExpectedResult("FOO_3.FOO.3.3:defg");
        givenExpectedResult("FOO_4.FOO.4.3:yes");
        givenExpectedResult("FOO_1.FOO.1.3:good");
        
        givenExpectedHit("FOO_3_BAR.FOO.3:defg<cat>");
        
        runTest();
    }
    
    @Test
    public void testHitForIndexedAndUnindexedQueryAndAnyfieldLimit() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>' and FOO_1 == 'good'");
        
        givenQueryParameter("include.grouping.context", "true");
        givenQueryParameter("hit.list", "true");
        givenQueryParameter("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4");
        
        givenExpectedResult("FOO_1_BAR.FOO.3:good<cat>");
        givenExpectedResult("FOO_3_BAR.FOO.3:defg<cat>");
        givenExpectedResult("FOO_3.FOO.3.3:defg");
        givenExpectedResult("FOO_4.FOO.4.3:yes");
        givenExpectedResult("FOO_1.FOO.1.3:good");
        
        givenExpectedHit("FOO_3_BAR.FOO.3:defg<cat>");
        givenExpectedHit("FOO_1.FOO.1.3:good");
        
        runTest();
    }
    
    @Test
    public void testHitWithoutGroupingContext() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>'");
        
        givenQueryParameter("include.grouping.context", "false");
        givenQueryParameter("hit.list", "true");
        givenQueryParameter("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4");
        
        // There is no grouping context, expect only the original term, not the related ones (in the same group).
        givenExpectedResult("FOO_3_BAR:defg<cat>");
        
        givenExpectedHit("FOO_3_BAR:defg<cat>");
        
        runTest();
    }
    
    @Test
    public void testHitWithRange() throws Exception {
        givenQuery("((_Bounded_ = true) && (FOO_1_BAR_1 >= '2021-03-01 00:00:00' && FOO_1_BAR_1 <= '2021-04-01 00:00:00'))");
        
        givenQueryParameter("include.grouping.context", "false");
        givenQueryParameter("hit.list", "true");
        givenQueryParameter("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4");
        
        // There is no grouping context, expect only the original term, not the related ones (in the same group).
        givenExpectedResult("FOO_1_BAR_1:2021-03-24T16:00:00.000Z");
        
        givenExpectedHit("FOO_1_BAR_1:Wed Mar 24 16:00:00 GMT 2021");
        
        runTest();
    }
    
    @Test
    public void testHitWithDate() throws Exception {
        givenQuery("FOO_1_BAR_1 == '2021-03-24T16:00:00.000Z'");
        
        givenQueryParameter("include.grouping.context", "false");
        givenQueryParameter("hit.list", "true");
        givenQueryParameter("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4");
        
        // There is no grouping context, expect only the original term, not the related ones (in the same group).
        givenExpectedHit("FOO_1_BAR_1:Wed Mar 24 16:00:00 GMT 2021");
        
        givenExpectedResult("FOO_1_BAR_1:2021-03-24T16:00:00.000Z");
        
        runTest();
    }
    
    @Test
    public void testHitWithExceededOrThreshold() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>' || FOO_3_BAR == 'abcd<cat>'");
        
        givenQueryParameter("include.grouping.context", "false");
        givenQueryParameter("hit.list", "true");
        givenQueryParameter("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=4,FOO_4=3,FOO_1_BAR_1=4");
        
        logic.setMaxOrExpansionThreshold(1);
        configIvarator();
        
        // There is no grouping context, expect only the original term, not the related ones (in the same group).
        givenExpectedResult("FOO_3_BAR:defg<cat>");
        givenExpectedResult("FOO_3_BAR:abcd<cat>");
        
        givenExpectedHit("FOO_3_BAR:defg<cat>");
        givenExpectedHit("FOO_3_BAR:abcd<cat>");
        
        runTest();
    }
    
    @Test
    public void testHitWithFunction() throws Exception {
        givenQuery("FOO_3_BAR == 'defg<cat>' || FOO_3_BAR == 'abcd<cat>'");
        
        givenQueryParameter("include.grouping.context", "false");
        givenQueryParameter("hit.list", "true");
        givenQueryParameter("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=4,FOO_4=3,FOO_1_BAR_1=4");
        
        logic.setMaxOrExpansionThreshold(1);
        configIvarator();
        
        // There is no grouping context, expect only the original term, not the related ones (in the same group).
        givenExpectedResult("FOO_3_BAR:defg<cat>");
        givenExpectedResult("FOO_3_BAR:abcd<cat>");
        
        givenExpectedHit("FOO_3_BAR:defg<cat>");
        givenExpectedHit("FOO_3_BAR:abcd<cat>");
        
        runTest();
    }
    
    protected void configIvarator() throws IOException {
        URL hdfsConfig = this.getClass().getResource("/testhadoop.config");
        Assert.assertNotNull(hdfsConfig);
        this.logic.setHdfsSiteConfigURLs(hdfsConfig.toExternalForm());
        File tmpDir = temporaryFolder.newFolder();
        log.info("hdfs dirs(" + tmpDir.toURI() + ")");
        IvaratorCacheDirConfig config = new IvaratorCacheDirConfig(tmpDir.toURI().toString());
        this.logic.setIvaratorCacheDirConfigs(Collections.singletonList(config));
    }
    
    private void givenQuery(String query) {
        this.query = query;
    }
    
    private void givenQueryParameter(String key, String value) {
        this.queryParameters.put(key, value);
    }
    
    private void givenExpectedHit(String expectedHit) {
        this.expectedHits.add(expectedHit);
    }
    
    private void givenExpectedResult(String expectedResult) {
        this.expectedResults.add(expectedResult);
    }
    
    private void runTest() throws Exception {
        QueryImpl settings = new QueryImpl();
        settings.setBeginDate(startDate);
        settings.setEndDate(endDate);
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(query);
        settings.setParameters(queryParameters);
        settings.setId(UUID.randomUUID());
        
        log.debug("query: " + settings.getQuery());
        log.debug("logic: " + settings.getQueryLogicName());
        
        GenericQueryConfiguration config = logic.initialize(getConnector(), settings, authSet);
        logic.setupQuery(config);
        
        Set<Document> docs = new HashSet<>();
        for (Entry<Key,Value> entry : logic) {
            Document d = deserializer.apply(entry).getValue();
            log.trace(entry.getKey() + " => " + d);
            docs.add(d);
            
            Attribute<?> hitAttribute = d.get(JexlEvaluation.HIT_TERM_FIELD);
            
            if (hitAttribute instanceof Attributes) {
                Attributes attributes = (Attributes) hitAttribute;
                for (Attribute<?> attr : attributes.getAttributes()) {
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
            log.debug("goodResults: " + expectedResults);
            Map<String,Attribute<? extends Comparable<?>>> dictionary = d.getDictionary();
            log.debug("dictionary:" + dictionary);
            for (Entry<String,Attribute<? extends Comparable<?>>> dictionaryEntry : dictionary.entrySet()) {
                
                Attribute<? extends Comparable<?>> attribute = dictionaryEntry.getValue();
                if (attribute instanceof Attributes) {
                    for (Attribute<?> attr : ((Attributes) attribute).getAttributes()) {
                        String toFind = dictionaryEntry.getKey() + ":" + attr;
                        boolean found = expectedResults.remove(toFind);
                        if (found)
                            log.debug("removed " + toFind);
                        else
                            log.debug("Did not remove " + toFind);
                    }
                } else {
                    
                    String toFind = dictionaryEntry.getKey() + ":" + dictionaryEntry.getValue();
                    
                    boolean found = expectedResults.remove(toFind);
                    if (found)
                        log.debug("removed " + toFind);
                    else
                        log.debug("Did not remove " + toFind);
                }
                
            }
            
            Assert.assertTrue(expectedHits + " good results was not empty", expectedResults.isEmpty());
        }
        Assert.assertFalse("No docs were returned!", docs.isEmpty());
    }
}
