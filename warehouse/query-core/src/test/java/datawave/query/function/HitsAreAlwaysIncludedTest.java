package datawave.query.function;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import datawave.configuration.spring.SpringBean;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.QueryTestTableHelper;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.util.LimitFieldsTestingIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.QueryImpl;
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
    
    protected Authorizations auths = new Authorizations("ALL");
    
    protected Set<Authorizations> authSet = Collections.singleton(auths);
    
    @Inject
    @SpringBean(name = "EventQuery")
    protected ShardQueryLogic logic;
    
    protected KryoDocumentDeserializer deserializer;
    
    private final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    
    @Deployment
    public static JavaArchive createDeployment() throws Exception {
        
        return ShrinkWrap.create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event", "datawave.core.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class).deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .addAsManifestResource(new StringAsset(
                                        "<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" + "</alternatives>"),
                                        "beans.xml");
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
    
    protected void runTestQuery(AccumuloClient client, String queryString, Date startDate, Date endDate, Map<String,String> extraParms,
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
        
        GenericQueryConfiguration config = logic.initialize(client, settings, authSet);
        logic.setupQuery(config);
        
        List<String> extraValues = new ArrayList<>();
        
        Set<Document> docs = new HashSet<>();
        for (Entry<Key,Value> entry : logic) {
            Document d = deserializer.apply(entry).getValue();
            
            log.trace(entry.getKey() + " => " + d);
            docs.add(d);
            
            Attribute hitAttribute = d.get(JexlEvaluation.HIT_TERM_FIELD);
            Attribute recordId = d.get(Document.DOCKEY_FIELD_NAME);
            
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
                if (attribute == hitAttribute || attribute == recordId) {
                    continue;
                }
                
                if (attribute instanceof Attributes) {
                    for (Attribute attr : ((Attributes) attribute).getAttributes()) {
                        String toFind = dictionaryEntry.getKey() + ":" + attr;
                        boolean found = goodResults.remove(toFind);
                        if (found) {
                            log.debug("removed " + toFind);
                        } else if (toFind.contains(LimitFields.ORIGINAL_COUNT_SUFFIX)) {
                            log.debug("Ignoring original count field " + toFind);
                        } else {
                            extraValues.add('"' + toFind + '"');
                        }
                    }
                } else {
                    
                    String toFind = dictionaryEntry.getKey() + ":" + dictionaryEntry.getValue();
                    
                    boolean found = goodResults.remove(toFind);
                    if (found) {
                        log.debug("removed " + toFind);
                    } else if (toFind.contains(LimitFields.ORIGINAL_COUNT_SUFFIX)) {
                        log.debug("Ignoring original count field " + toFind);
                    } else {
                        extraValues.add('"' + toFind + '"');
                    }
                }
                
            }
        }
        
        Assert.assertTrue(goodResults + " good results was not empty", goodResults.isEmpty());
        Assert.assertTrue(extraValues + " extra values was not empty", extraValues.isEmpty());
        Assert.assertTrue("No docs were returned!", !docs.isEmpty());
        Assert.assertEquals("Expected exactly one document", 1, docs.size());
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
        extraParameters.put("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0");
        
        String queryString = "FOO_3_BAR == 'defg<cat>'";
        
        //@formatter:off
        Set<String> goodResults = Sets.newHashSet(
                // the hit and associated fields in the same group
                "FOO_1_BAR.FOO.3:good<cat>",
                "FOO_3_BAR.FOO.3:defg<cat>",
                "FOO_3.FOO.3.3:defg",
                "FOO_4.FOO.4.3:yes",
                "FOO_1.FOO.1.3:good",
                // the additional values included per the limits
                "FOO_1.FOO.1.0:yawn",
                "FOO_1_BAR.FOO.0:yawn<cat>",
                "FOO_1_BAR.FOO.1:yawn<cat>",
                "FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z",
                "FOO_3.FOO.3.0:abcd",
                "FOO_3_BAR.FOO.0:abcd<cat>",
                "FOO_4.FOO.4.0:purr",
                "FOO_4.FOO.4.1:purr");
        //@formatter:on
        Set<String> expectedHits = Sets.newHashSet("FOO_3_BAR.FOO.3:defg<cat>");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testHitForIndexedQueryTermWithOptionsInQueryFunction() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        
        String queryString = "FOO_3_BAR == 'defg<cat>' and f:options('include.grouping.context', 'true', "
                        + "'hit.list', 'true', 'limit.fields', 'FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0')";
        
        //@formatter:off
        Set<String> goodResults = Sets.newHashSet(
                // the hit and associated fields in the same group
                "FOO_1_BAR.FOO.3:good<cat>",
                "FOO_3_BAR.FOO.3:defg<cat>",
                "FOO_3.FOO.3.3:defg",
                "FOO_4.FOO.4.3:yes",
                "FOO_1.FOO.1.3:good",
                // the additional values included per the limits
                "FOO_1.FOO.1.0:yawn",
                "FOO_1_BAR.FOO.0:yawn<cat>",
                "FOO_1_BAR.FOO.1:yawn<cat>",
                "FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z",
                "FOO_3.FOO.3.0:abcd",
                "FOO_3_BAR.FOO.0:abcd<cat>",
                "FOO_4.FOO.4.0:purr",
                "FOO_4.FOO.4.1:purr");
        //@formatter:on
        Set<String> expectedHits = Sets.newHashSet("FOO_3_BAR.FOO.3:defg<cat>");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testHitForIndexedQueryOnUnrealmed() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0");
        
        String queryString = "FOO_3 == 'defg'";
        
        //@formatter:off
        Set<String> goodResults = Sets.newHashSet(
                // the hit and associated fields in the same group
                "FOO_1_BAR.FOO.3:good<cat>",
                "FOO_3_BAR.FOO.3:defg<cat>",
                "FOO_3.FOO.3.3:defg",
                "FOO_4.FOO.4.3:yes",
                "FOO_1.FOO.1.3:good",
                // the additional values included per the limits
                "FOO_1.FOO.1.0:yawn",
                "FOO_1_BAR.FOO.0:yawn<cat>",
                "FOO_1_BAR.FOO.1:yawn<cat>",
                "FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z",
                "FOO_3.FOO.3.0:abcd",
                "FOO_3_BAR.FOO.0:abcd<cat>",
                "FOO_4.FOO.4.0:purr",
                "FOO_4.FOO.4.1:purr");
        //@formatter:on
        Set<String> expectedHits = Sets.newHashSet("FOO_3.FOO.3.3:defg");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testHitForIndexedQueryAndAnyfieldLimit() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "_ANYFIELD_=2,BAR_1=0,BAR_2=0,BAR_3=0");
        
        String queryString = "FOO_3_BAR == 'defg<cat>'";
        
        //@formatter:off
        Set<String> goodResults = Sets.newHashSet(
                // the hit and associated fields in the same group
                "FOO_1_BAR.FOO.3:good<cat>",
                "FOO_3_BAR.FOO.3:defg<cat>",
                "FOO_3.FOO.3.3:defg",
                "FOO_4.FOO.4.3:yes",
                "FOO_1.FOO.1.3:good",
                // the additional values included per the limits
                "FOO_1.FOO.1.0:yawn",
                "FOO_1_BAR.FOO.0:yawn<cat>",
                "FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z",
                "FOO_3.FOO.3.0:abcd",
                "FOO_3_BAR.FOO.0:abcd<cat>",
                "FOO_4.FOO.4.0:purr");
        //@formatter:on
        Set<String> expectedHits = Sets.newHashSet("FOO_3_BAR.FOO.3:defg<cat>");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testHitForIndexedAndUnindexedQueryAndAnyfieldLimit() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "_ANYFIELD_=2,BAR_1=0,BAR_2=0,BAR_3=0");
        
        String queryString = "FOO_3_BAR == 'defg<cat>' and FOO_1 == 'good'";
        //@formatter:off
        Set<String> goodResults = Sets.newHashSet(
                // the hit and associated fields in the same group
                "FOO_1_BAR.FOO.3:good<cat>",
                "FOO_3_BAR.FOO.3:defg<cat>",
                "FOO_3.FOO.3.3:defg",
                "FOO_4.FOO.4.3:yes",
                // the additional values included per the limits
                "FOO_1.FOO.1.0:yawn",
                "FOO_1.FOO.1.3:good",
                "FOO_1_BAR.FOO.0:yawn<cat>",
                "FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z",
                "FOO_3.FOO.3.0:abcd",
                "FOO_3_BAR.FOO.0:abcd<cat>",
                "FOO_4.FOO.4.0:purr");
        //@formatter:on
        Set<String> expectedHits = Sets.newHashSet("FOO_3_BAR.FOO.3:defg<cat>", "FOO_1.FOO.1.3:good");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testHitWithoutGroupingContext() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "false");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0");
        
        String queryString = "FOO_3_BAR == 'defg<cat>'";
        
        // there is no grouping context so I can expect only the original term, not the related ones (in the same group)
        //@formatter:off
        Set<String> goodResults = Sets.newHashSet(
                // the hit
                "FOO_3_BAR:defg<cat>",
                // the additional values included per the limits
                "FOO_1:yawn",
                "FOO_1:good",
                "FOO_1_BAR:yawn<cat>",
                "FOO_1_BAR:good<cat>",
                "FOO_1_BAR_1:2021-03-24T16:00:00.000Z",
                "FOO_3:abcd",
                "FOO_3:bcde",
                "FOO_3_BAR:abcd<cat>",
                "FOO_4:purr",
                "FOO_4:yes");
        //@formatter:on
        Set<String> expectedHits = Sets.newHashSet("FOO_3_BAR:defg<cat>");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testHitWithRange() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "false");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0");
        
        String queryString = "((_Bounded_ = true) && (FOO_1_BAR_1 >= '2021-03-01 00:00:00' && FOO_1_BAR_1 <= '2021-04-01 00:00:00'))";
        
        // there is no grouping context so I can expect only the original term, not the related ones (in the same group)
        Set<String> expectedHits = Sets.newHashSet("FOO_1_BAR_1:Wed Mar 24 16:00:00 GMT 2021");
        //@formatter:off
        Set<String> goodResults = Sets.newHashSet(
                // the hit
                "FOO_1_BAR_1:2021-03-24T16:00:00.000Z",
                // the additional values included per the limits
                "FOO_1:yawn",
                "FOO_1:good",
                "FOO_1_BAR:yawn<cat>",
                "FOO_1_BAR:good<cat>",
                "FOO_3:abcd",
                "FOO_3:bcde",
                "FOO_3_BAR:abcd<cat>",
                "FOO_3_BAR:bcde<cat>",
                "FOO_4:purr",
                "FOO_4:yes");
        //@formatter:on
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testHitWithDate() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "false");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=2,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0");
        
        String queryString = "FOO_1_BAR_1 == '2021-03-24T16:00:00.000Z'";
        
        // there is no grouping context so I can expect only the original term, not the related ones (in the same group)
        Set<String> expectedHits = Sets.newHashSet("FOO_1_BAR_1:Wed Mar 24 16:00:00 GMT 2021");
        //@formatter:off
        Set<String> goodResults = Sets.newHashSet(
                // the hit
                "FOO_1_BAR_1:2021-03-24T16:00:00.000Z",
                // the additional values included per the limits
                "FOO_1:yawn",
                "FOO_1:good",
                "FOO_1_BAR:yawn<cat>",
                "FOO_1_BAR:good<cat>",
                "FOO_3:abcd",
                "FOO_3:bcde",
                "FOO_3_BAR:abcd<cat>",
                "FOO_3_BAR:bcde<cat>",
                "FOO_4:purr",
                "FOO_4:yes");
        //@formatter:on
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testHitWithExceededOrThreshold() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "false");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=3,FOO_1=2,FOO_3=2,FOO_3_BAR=1,FOO_4=3,FOO_1_BAR_1=4,BAR_1=0,BAR_2=0,BAR_3=0");
        logic.setMaxOrExpansionThreshold(1);
        ivaratorConfig();
        
        String queryString = "FOO_3_BAR == 'defg<cat>' || FOO_3_BAR == 'abcd<cat>'";
        
        // there is no grouping context so I can expect only the original term, not the related ones (in the same group)
        //@formatter:off
        Set<String> goodResults = Sets.newHashSet(
                // the hits
                "FOO_3_BAR:defg<cat>",
                "FOO_3_BAR:abcd<cat>",
                // the additional values included per the limits
                "FOO_1:yawn",
                "FOO_1:good",
                "FOO_1_BAR:yawn<cat>",
                "FOO_1_BAR:good<cat>",
                "FOO_1_BAR_1:2021-03-24T16:00:00.000Z",
                "FOO_3:abcd",
                "FOO_3:bcde",
                "FOO_4:purr",
                "FOO_4:yes");
        //@formatter:on
        Set<String> expectedHits = Sets.newHashSet("FOO_3_BAR:defg<cat>", "FOO_3_BAR:abcd<cat>");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testHitsOnly() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "false");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=-1,FOO_1=-1,FOO_3=-1,FOO_3_BAR=-1,FOO_4=-1,FOO_1_BAR_1=-1,BAR_1=0,BAR_2=0,BAR_3=0");
        logic.setMaxOrExpansionThreshold(1);
        ivaratorConfig();
        
        String queryString = "FOO_3_BAR == 'defg<cat>' || FOO_3_BAR == 'abcd<cat>'";
        
        // there is no grouping context so I can expect only the original term, not the related ones (in the same group)
        //@formatter:off
        Set<String> goodResults = Sets.newHashSet("FOO_3_BAR:defg<cat>", "FOO_3_BAR:abcd<cat>");
        //@formatter:on
        Set<String> expectedHits = Sets.newHashSet("FOO_3_BAR:defg<cat>", "FOO_3_BAR:abcd<cat>");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testGroupedHitsOnly() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=-1,FOO_1=-1,FOO_3=-1,FOO_3_BAR=-1,FOO_4=-1,FOO_1_BAR_1=-1,BAR_1=0,BAR_2=0,BAR_3=0");
        logic.setMaxOrExpansionThreshold(1);
        ivaratorConfig();
        
        String queryString = "FOO_3_BAR == 'defg<cat>' || FOO_3_BAR == 'abcd<cat>'";
        //@formatter:off
        Set<String> goodResults = Sets.newHashSet(
                // the hit and associated fields in the same group
                "FOO_1_BAR.FOO.3:good<cat>",
                "FOO_3_BAR.FOO.3:defg<cat>",
                "FOO_3.FOO.3.3:defg",
                "FOO_4.FOO.4.3:yes",
                // the additional values included per the limits
                "FOO_1.FOO.1.3:good",
                "FOO_1.FOO.1.0:yawn",
                "FOO_4.FOO.4.0:purr",
                "FOO_3.FOO.3.0:abcd",
                "FOO_3_BAR.FOO.0:abcd<cat>",
                "FOO_1_BAR.FOO.0:yawn<cat>",
                "FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z");
        //@formatter:on
        Set<String> expectedHits = Sets.newHashSet("FOO_3_BAR.FOO.3:defg<cat>", "FOO_3_BAR.FOO.0:abcd<cat>");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testGroupedHitsWithMatchingField() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=-1,FOO_1=-1,FOO_3=-1,FOO_3_BAR=-1,FOO_4=-1,FOO_1_BAR_1=-1,BAR_1=0,BAR_2=0,BAR_3=0");
        extraParameters.put("matching.field.sets", "FOO_4");
        logic.setMaxOrExpansionThreshold(1);
        ivaratorConfig();
        
        String queryString = "FOO_3_BAR == 'abcd<cat>'";
        
        //@formatter:off
        Set<String> goodResults = Sets.newHashSet(
                // the hit and associated fields in the same group
                "FOO_3_BAR.FOO.0:abcd<cat>",
                "FOO_1.FOO.1.0:yawn",
                "FOO_4.FOO.4.0:purr",
                "FOO_3.FOO.3.0:abcd",
                "FOO_1_BAR.FOO.0:yawn<cat>",
                "FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z",
                // the additional values included per the matching field sets
                "FOO_1.FOO.1.1:yawn",
                "FOO_4.FOO.4.1:purr",
                "FOO_3.FOO.3.1:bcde",
                "FOO_3_BAR.FOO.1:bcde<cat>",
                "FOO_1_BAR.FOO.1:yawn<cat>",
                "FOO_1.FOO.1.2:yawn",
                "FOO_4.FOO.4.2:purr",
                "FOO_3.FOO.3.2:cdef",
                "FOO_3_BAR.FOO.2:cdef<cat>",
                "FOO_1_BAR.FOO.2:yawn<cat>");
        //@formatter:on
        Set<String> expectedHits = Sets.newHashSet("FOO_3_BAR.FOO.0:abcd<cat>");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testGroupedHitsWithMatchingFields() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=-1,FOO_1=-1,FOO_3=-1,FOO_3_BAR=-1,FOO_4=-1,FOO_1_BAR_1=-1,BAR_1=0,BAR_2=0,BAR_3=0");
        extraParameters.put("matching.field.sets", "FOO_4=BAR_1");
        logic.setMaxOrExpansionThreshold(1);
        ivaratorConfig();
        
        String queryString = "FOO_3_BAR == 'abcd<cat>'";
        
        //@formatter:off
        Set<String> goodResults = Sets.newHashSet(
                // the hit and associated fields in the same group
                "FOO_3_BAR.FOO.0:abcd<cat>",
                "FOO_1.FOO.1.0:yawn",
                "FOO_4.FOO.4.0:purr",
                "FOO_3.FOO.3.0:abcd",
                "FOO_1_BAR.FOO.0:yawn<cat>",
                "FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z",
                // the additional values included per the matching field sets
                "FOO_1.FOO.1.1:yawn",
                "FOO_4.FOO.4.1:purr",
                "FOO_3.FOO.3.1:bcde",
                "FOO_3_BAR.FOO.1:bcde<cat>",
                "FOO_1_BAR.FOO.1:yawn<cat>",
                "FOO_1.FOO.1.2:yawn",
                "FOO_4.FOO.4.2:purr",
                "FOO_3.FOO.3.2:cdef",
                "FOO_3_BAR.FOO.2:cdef<cat>",
                "FOO_1_BAR.FOO.2:yawn<cat>",
                "BAR_1.BAR.1.3:purr",
                "BAR_2.BAR.2.3:tiger",
                "BAR_3.BAR.3.3:spotted");
        //@formatter:on
        Set<String> expectedHits = Sets.newHashSet("FOO_3_BAR.FOO.0:abcd<cat>");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testGroupedHitsWithMoreMatchingFields() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=-1,FOO_1=-1,FOO_3=-1,FOO_3_BAR=-1,FOO_4=-1,FOO_1_BAR_1=-1,BAR_1=0,BAR_2=0,BAR_3=0");
        extraParameters.put("matching.field.sets", "FOO_4=BAR_1=FOO_1");
        logic.setMaxOrExpansionThreshold(1);
        ivaratorConfig();
        
        String queryString = "FOO_3_BAR == 'abcd<cat>'";
        
        //@formatter:off
        Set<String> goodResults = Sets.newHashSet(
                // the hit and associated fields in the same group
                "FOO_3_BAR.FOO.0:abcd<cat>",
                "FOO_1.FOO.1.0:yawn",
                "FOO_4.FOO.4.0:purr",
                "FOO_3.FOO.3.0:abcd",
                "FOO_1_BAR.FOO.0:yawn<cat>",
                "FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z",
                // the additional values included per the matching field sets
                "FOO_1.FOO.1.1:yawn",
                "FOO_4.FOO.4.1:purr",
                "FOO_3.FOO.3.1:bcde",
                "FOO_3_BAR.FOO.1:bcde<cat>",
                "FOO_1_BAR.FOO.1:yawn<cat>",
                "FOO_1.FOO.1.2:yawn",
                "FOO_4.FOO.4.2:purr",
                "FOO_3.FOO.3.2:cdef",
                "FOO_3_BAR.FOO.2:cdef<cat>",
                "FOO_1_BAR.FOO.2:yawn<cat>",
                "BAR_1.BAR.1.2:yawn",
                "BAR_2.BAR.2.2:siberian",
                "BAR_3.BAR.3.2:pink",
                "BAR_1.BAR.1.3:purr",
                "BAR_2.BAR.2.3:tiger",
                "BAR_3.BAR.3.3:spotted");
        //@formatter:on
        Set<String> expectedHits = Sets.newHashSet("FOO_3_BAR.FOO.0:abcd<cat>");
        
        runTestQuery(queryString, format.parse("20091231"), format.parse("20150101"), extraParameters, expectedHits, goodResults);
    }
    
    @Test
    public void testGroupedHitsWithMatchingFieldSets() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");
        extraParameters.put("limit.fields", "FOO_1_BAR=-1,FOO_1=-1,FOO_3=-1,FOO_3_BAR=-1,FOO_4=-1,FOO_1_BAR_1=-1,BAR_1=0,BAR_2=0,BAR_3=0");
        extraParameters.put("matching.field.sets", "FOO_4=BAR_1,FOO_1=BAR_1");
        logic.setMaxOrExpansionThreshold(1);
        ivaratorConfig();
        
        String queryString = "FOO_3_BAR == 'abcd<cat>'";
        
        //@formatter:off
        Set<String> goodResults = Sets.newHashSet(
                // the hit and associated fields in the same group
                "FOO_3_BAR.FOO.0:abcd<cat>",
                "FOO_1.FOO.1.0:yawn",
                "FOO_4.FOO.4.0:purr",
                "FOO_3.FOO.3.0:abcd",
                "FOO_1_BAR.FOO.0:yawn<cat>",
                "FOO_1_BAR_1.FOO.0:2021-03-24T16:00:00.000Z",
                // the additional values included per the matching field sets
                "FOO_1.FOO.1.1:yawn",
                "FOO_4.FOO.4.1:purr",
                "FOO_3.FOO.3.1:bcde",
                "FOO_3_BAR.FOO.1:bcde<cat>",
                "FOO_1_BAR.FOO.1:yawn<cat>",
                "FOO_1.FOO.1.2:yawn",
                "FOO_4.FOO.4.2:purr",
                "FOO_3.FOO.3.2:cdef",
                "FOO_3_BAR.FOO.2:cdef<cat>",
                "FOO_1_BAR.FOO.2:yawn<cat>",
                "BAR_1.BAR.1.2:yawn",
                "BAR_2.BAR.2.2:siberian",
                "BAR_3.BAR.3.2:pink",
                "BAR_1.BAR.1.3:purr",
                "BAR_2.BAR.2.3:tiger",
                "BAR_3.BAR.3.3:spotted");
        //@formatter:on
        Set<String> expectedHits = Sets.newHashSet("FOO_3_BAR.FOO.0:abcd<cat>");
        
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
