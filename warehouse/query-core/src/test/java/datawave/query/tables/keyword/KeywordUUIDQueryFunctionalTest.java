package datawave.query.tables.keyword;

import static datawave.query.util.WiseGuysIngest.caponeUID;
import static datawave.query.util.WiseGuysIngest.corleoneUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import datawave.configuration.spring.SpringBean;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.helpers.PrintUtility;
import datawave.microservice.query.QueryImpl;
import datawave.query.ExcerptTest;
import datawave.query.QueryTestTableHelper;
import datawave.query.tables.ResponseQueryDriver;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.tables.keyword.extractor.FieldedTagCloudInputExtractor;
import datawave.query.tables.keyword.transform.KeywordResultsTransformer;
import datawave.query.util.WiseGuysIngest;
import datawave.util.TableName;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.keyword.DefaultTagCloud;
import datawave.webservice.result.keyword.DefaultTagCloudEntry;
import datawave.webservice.result.keyword.DefaultTagCloudResponse;
import datawave.webservice.result.keyword.TagCloudBase;

@RunWith(Arquillian.class)
public class KeywordUUIDQueryFunctionalTest {
    protected static AccumuloClient connector = null;

    private static final Logger log = Logger.getLogger(KeywordUUIDQueryFunctionalTest.class);
    protected Authorizations auths = new Authorizations("ALL");
    protected Set<Authorizations> authSet = Set.of(auths);

    @Inject
    @SpringBean(name = "KeywordUUIDQuery")
    protected KeywordChainedUUIDQueryLogic logic;

    private QueryDriver driver;

    @Deployment
    public static JavaArchive createDeployment() throws Exception {

        return ShrinkWrap.create(JavaArchive.class)
                        .addPackages(true, "org.apache.deltaspike", "io.astefanutti.metrics.cdi", "datawave.query", "org.jboss.logging",
                                        "datawave.webservice.query.result.event")
                        .deleteClass(DefaultEdgeEventQueryLogic.class).deleteClass(RemoteEdgeDictionary.class)
                        .deleteClass(datawave.query.metrics.QueryMetricQueryLogic.class)
                        .addAsManifestResource(new StringAsset(
                                        "<alternatives>" + "<stereotype>datawave.query.tables.edge.MockAlternative</stereotype>" + "</alternatives>"),
                                        "beans.xml");
    }

    @BeforeClass
    public static void setUp() throws Exception {
        QueryTestTableHelper qtth = new QueryTestTableHelper(ExcerptTest.DocumentRangeTest.class.toString(), log);
        connector = qtth.client;

        Logger.getLogger(PrintUtility.class).setLevel(Level.DEBUG);

        WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.DOCUMENT);
        Authorizations auths = new Authorizations("ALL");
        PrintUtility.printTable(connector, auths, TableName.SHARD);
        PrintUtility.printTable(connector, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(connector, auths, QueryTestTableHelper.MODEL_TABLE_NAME);
    }

    @Before
    public void setup() throws ParseException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        log.setLevel(Level.TRACE);
        driver = new QueryDriver();
        driver.withLogic(logic).withAuths(auths);
    }

    @Test
    public void injectionTest() {
        assertNotNull(logic);
    }

    @Test
    public void emptyQueryTest() throws Exception {
        driver.withQuery("UUID:ABC").run();
    }

    @Test
    public void keywordHighScoresLookupTest() throws Exception {
        driver.withQuery("UUID:CORLEONE").run();
    }

    @Test
    public void keywordAdjustedMaxScoreLookupV1Test() throws Exception {
        KeywordQueryLogic keywordQueryLogic = (KeywordQueryLogic) logic.getLogic2();
        keywordQueryLogic.setMaxScore(1);

        driver.withQuery("UUID:CORLEONE").withExpectedTagCloud(getCorleoneKeywordCloud("UUID:CORLEONE", "1")).run();
    }

    @Test
    public void keywordAdjustedMaxScoreLookupV2Test() throws Exception {
        KeywordQueryLogic keywordQueryLogic = (KeywordQueryLogic) logic.getLogic2();
        keywordQueryLogic.setMaxScore(1);

        driver.withQuery("UUID:CORLEONE").withExpectedTagCloud(getCorleoneKeywordCloud("UUID:CORLEONE", "2"))
                        .withExtraParameters(Map.of("tag.cloud.version", "2")).run();
    }

    @Test
    public void keywordLanguageLookupV1Test() throws Exception {
        KeywordQueryLogic keywordQueryLogic = (KeywordQueryLogic) logic.getLogic2();
        keywordQueryLogic.setMaxScore(1);

        // add language to the data so it will be pulled back with the document
        BatchWriter bw = connector.createBatchWriter("shard");
        Mutation m = new Mutation("20130101_0");
        m.put("test" + '\u0000' + corleoneUID, "LANGUAGE" + '\u0000' + "english", new Value());
        bw.addMutation(m);
        bw.flush();

        DefaultTagCloud expectedTagCloud = getCorleoneKeywordCloud("UUID:CORLEONE", "1");
        expectedTagCloud.setLanguage("english");
        driver.withQuery("UUID:CORLEONE").withExpectedTagCloud(expectedTagCloud).withExtraParameters(Map.of("tag.cloud.language", "true")).run();
    }

    @Test
    public void keywordLanguageLookupV2Test() throws Exception {
        KeywordQueryLogic keywordQueryLogic = (KeywordQueryLogic) logic.getLogic2();
        keywordQueryLogic.setMaxScore(1);

        // add language to the data so it will be pulled back with the document
        BatchWriter bw = connector.createBatchWriter("shard");
        Mutation m = new Mutation("20130101_0");
        m.put("test" + '\u0000' + corleoneUID, "LANGUAGE" + '\u0000' + "english", new Value());
        bw.addMutation(m);
        bw.flush();

        DefaultTagCloud expectedTagCloud = getCorleoneKeywordCloud("UUID:CORLEONE", "2");
        expectedTagCloud.setMetadata(Map.of("view", "CONTENT", "type", KeywordResultsTransformer.LABEL, "language", "english"));
        driver.withQuery("UUID:CORLEONE").withExpectedTagCloud(expectedTagCloud)
                        .withExtraParameters(Map.of("tag.cloud.language", "true", "tag.cloud.version", "2")).run();
    }

    @Test
    public void keywordMultiLanguageLookupV1Test() throws Exception {
        KeywordQueryLogic keywordQueryLogic = (KeywordQueryLogic) logic.getLogic2();
        keywordQueryLogic.setMaxScore(1);

        // add language to the data so it will be pulled back with the document
        BatchWriter bw = connector.createBatchWriter("shard");
        Mutation m = new Mutation("20130101_0");

        m.put("test" + '\u0000' + corleoneUID, "LANGUAGE" + '\u0000' + "english", new Value());
        m.put("test" + '\u0000' + caponeUID, "LANGUAGE" + '\u0000' + "italian", new Value());
        bw.addMutation(m);
        bw.flush();

        DefaultTagCloud expectedTagCloud = getCorleoneKeywordCloud("UUID:CORLEONE", "1");
        expectedTagCloud.setLanguage("english");
        driver.withExpectedTagCloud(expectedTagCloud);
        expectedTagCloud = createCloud(getCaponeItalianKeywordEntries("UUID:CAPONE"), "1");
        expectedTagCloud.setLanguage("italian");
        driver.withExpectedTagCloud(expectedTagCloud);
        driver.withQuery("UUID:CORLEONE OR UUID:CAPONE").withExtraParameters(Map.of("tag.cloud.language", "true")).run();
    }

    @Test
    public void keywordMultiLanguageLookupV2Test() throws Exception {
        KeywordQueryLogic keywordQueryLogic = (KeywordQueryLogic) logic.getLogic2();
        keywordQueryLogic.setMaxScore(1);

        // add language to the data so it will be pulled back with the document
        BatchWriter bw = connector.createBatchWriter("shard");
        Mutation m = new Mutation("20130101_0");

        m.put("test" + '\u0000' + corleoneUID, "LANGUAGE" + '\u0000' + "english", new Value());
        m.put("test" + '\u0000' + caponeUID, "LANGUAGE" + '\u0000' + "italian", new Value());
        bw.addMutation(m);
        bw.flush();

        DefaultTagCloud expectedTagCloud = getCorleoneKeywordCloud("UUID:CORLEONE", "2");
        expectedTagCloud.setMetadata(Map.of("view", "CONTENT", "type", KeywordResultsTransformer.LABEL, "language", "english"));
        driver.withExpectedTagCloud(expectedTagCloud);
        expectedTagCloud = getCaponeKeywordCloud("UUID:CAPONE", "2");
        expectedTagCloud.setMetadata(Map.of("view", "CONTENT", "type", KeywordResultsTransformer.LABEL, "language", "italian"));
        driver.withExpectedTagCloud(expectedTagCloud);
        driver.withQuery("UUID:CORLEONE OR UUID:CAPONE").withExtraParameters(Map.of("tag.cloud.language", "true", "tag.cloud.version", "2")).run();
    }

    @Test
    public void keywordMultiDocLanguageLookupV1Test() throws Exception {
        KeywordQueryLogic keywordQueryLogic = (KeywordQueryLogic) logic.getLogic2();
        keywordQueryLogic.setMaxScore(1);

        // add language to the data so it will be pulled back with the document
        BatchWriter bw = connector.createBatchWriter("shard");
        Mutation m = new Mutation("20130101_0");

        m.put("test" + '\u0000' + corleoneUID, "LANGUAGE" + '\u0000' + "english", new Value());
        m.put("test" + '\u0000' + caponeUID, "LANGUAGE" + '\u0000' + "english", new Value());
        bw.addMutation(m);
        bw.flush();

        List<DefaultTagCloudEntry> entries = getCaponeKeywordEntries("UUID:CAPONE");
        entries.addAll(getCorleoneKeywordEntries("UUID:CORLEONE"));

        DefaultTagCloud expectedTagCloud = createCloud(entries, "1");
        expectedTagCloud.setLanguage("english");
        driver.withExpectedTagCloud(expectedTagCloud);
        driver.withQuery("UUID:CORLEONE OR UUID:CAPONE").withExtraParameters(Map.of("tag.cloud.language", "true")).run();
    }

    @Test
    public void keywordMultiDocLanguageLookupV2Test() throws Exception {
        KeywordQueryLogic keywordQueryLogic = (KeywordQueryLogic) logic.getLogic2();
        keywordQueryLogic.setMaxScore(1);

        // add language to the data so it will be pulled back with the document
        BatchWriter bw = connector.createBatchWriter("shard");
        Mutation m = new Mutation("20130101_0");

        m.put("test" + '\u0000' + corleoneUID, "LANGUAGE" + '\u0000' + "english", new Value());
        m.put("test" + '\u0000' + caponeUID, "LANGUAGE" + '\u0000' + "english", new Value());
        bw.addMutation(m);
        bw.flush();

        List<DefaultTagCloudEntry> entries = getCaponeKeywordEntries("UUID:CAPONE");
        entries.addAll(getCorleoneKeywordEntries("UUID:CORLEONE"));

        DefaultTagCloud expectedTagCloud = createCloud(entries, "2");
        expectedTagCloud.setMetadata(Map.of("view", "CONTENT", "type", KeywordResultsTransformer.LABEL, "language", "english"));
        driver.withExpectedTagCloud(expectedTagCloud);
        driver.withQuery("UUID:CORLEONE OR UUID:CAPONE").withExtraParameters(Map.of("tag.cloud.language", "true", "tag.cloud.version", "2")).run();
    }

    @Test
    public void extractorSingleFieldTest() throws Exception {
        // override the chain strategy to add in an extractor
        KeywordUUIDChainStrategy chainStrategy = (KeywordUUIDChainStrategy) logic.getChainStrategy();
        FieldedTagCloudInputExtractor extractor = new FieldedTagCloudInputExtractor();
        extractor.setCategory("test");
        extractor.setFields(List.of("NOME"));
        chainStrategy.setExtractors(List.of(extractor));
        logic.setExtractors(List.of(extractor));

        driver.withExpectedTagCloud(getExpectedCloud("1", Map.of(), List.of())).withQuery("UUID:CAPONE OR UUID:CORLEONE OR UUID:SOPRANO")
                        .withExtraParameters(Map.of("tag.cloud.category", "test")).run();
    }

    // TODO tests
    // single extractor
    // multi extractor
    // mixed langauge and extractor
    // extractor with subtags
    // extractor with subtags filtered
    // keyword filtered

    private DefaultTagCloud getExpectedCloud(String version, Map<String,String> metadata, List<DefaultTagCloudEntry> entries) {
        DefaultTagCloud expectedCloud = new DefaultTagCloud();
        if (version.equals("1")) {
            expectedCloud.setLanguage(metadata.get("language") != null ? metadata.get("language") : metadata.get("type"));
        } else {
            expectedCloud.setMetadata(metadata);
        }
        expectedCloud.setMarkings(Map.of("visibility", "[ALL]"));
        expectedCloud.setTags(entries);
        expectedCloud.setIntermediateResult(false);

        return expectedCloud;
    }

    private DefaultTagCloudEntry createTagCloudEntry(String term, double score, int frequency, List<String> sources) {
        DefaultTagCloudEntry entry = new DefaultTagCloudEntry();
        entry.setTerm(term);
        entry.setScore(score);
        entry.setFrequency(frequency);
        entry.setSources(sources);

        return entry;
    }

    private List<DefaultTagCloudEntry> getCaponeKeywordEntries(String src) {
        List<DefaultTagCloudEntry> entries = new ArrayList<>();
        entries.add(createTagCloudEntry("alone", 0.6092, 1, List.of(src)));
        entries.add(createTagCloudEntry("farther", 0.6092, 1, List.of(src)));
        entries.add(createTagCloudEntry("get much", 0.5903, 1, List.of(src)));
        entries.add(createTagCloudEntry("gun", 0.6092, 1, List.of(src)));
        entries.add(createTagCloudEntry("kind", 0.2546, 1, List.of(src)));
        entries.add(createTagCloudEntry("kind word", 0.2052, 1, List.of(src)));
        entries.add(createTagCloudEntry("kind word alone", 0.4375, 1, List.of(src)));
        entries.add(createTagCloudEntry("much farther", 0.5903, 1, List.of(src)));
        entries.add(createTagCloudEntry("word", 0.2857, 1, List.of(src)));
        entries.add(createTagCloudEntry("word alone", 0.534, 1, List.of(src)));

        return entries;
    }

    private List<DefaultTagCloudEntry> getCaponeItalianKeywordEntries(String src) {
        List<DefaultTagCloudEntry> entries = new ArrayList<>();
        entries.add(createTagCloudEntry("can", 0.3124, 1, List.of(src)));
        entries.add(createTagCloudEntry("can with", 0.3768, 1, List.of(src)));
        entries.add(createTagCloudEntry("farther with", 0.364, 1, List.of(src)));
        entries.add(createTagCloudEntry("kind", 0.2546, 1, List.of(src)));
        entries.add(createTagCloudEntry("kind word", 0.2052, 1, List.of(src)));
        entries.add(createTagCloudEntry("much farther with", 0.3869, 1, List.of(src)));
        entries.add(createTagCloudEntry("with", 0.219, 1, List.of(src)));
        entries.add(createTagCloudEntry("word", 0.2857, 1, List.of(src)));
        entries.add(createTagCloudEntry("you", 0.2996, 1, List.of(src)));
        entries.add(createTagCloudEntry("you can", 0.2991, 1, List.of(src)));

        return entries;
    }

    private DefaultTagCloud getCaponeKeywordCloud(String src, String version) {
        return createCloud(getCaponeItalianKeywordEntries(src), version);
    }

    private List<DefaultTagCloudEntry> getCorleoneKeywordEntries(String src) {
        List<DefaultTagCloudEntry> entries = new ArrayList<>();
        entries.add(createTagCloudEntry("im", 0.6041, 1, List.of(src)));
        entries.add(createTagCloudEntry("cant", 0.7494, 1, List.of(src)));
        entries.add(createTagCloudEntry("make", 0.7494, 1, List.of(src)));
        entries.add(createTagCloudEntry("offer", 0.7494, 1, List.of(src)));
        entries.add(createTagCloudEntry("refuse", 0.7494, 1, List.of(src)));
        entries.add(createTagCloudEntry("gonna", 0.8807, 1, List.of(src)));

        return entries;
    }

    private DefaultTagCloud getCorleoneKeywordCloud(String src, String version) {
        return createCloud(getCorleoneKeywordEntries(src), version);
    }

    private DefaultTagCloud createCloud(List<DefaultTagCloudEntry> entries, String version) {
        DefaultTagCloud expectedCloud = new DefaultTagCloud();
        expectedCloud.setMarkings(Map.of("visibility", "[ALL]"));
        expectedCloud.setTags(entries);
        if (version.equals("1")) {
            expectedCloud.setLanguage("keyword");
        } else {
            expectedCloud.setMetadata(Map.of("view", "CONTENT", "type", KeywordResultsTransformer.LABEL));
        }
        expectedCloud.setIntermediateResult(false);
        return expectedCloud;
    }

    private static class QueryDriver {
        private final List<DefaultTagCloud> expectedResults = new ArrayList<>();
        private KeywordChainedUUIDQueryLogic logic;
        private String query;
        private ResponseQueryDriver<Map.Entry<Key,Value>> queryDriver;
        private Authorizations auths;
        private final Map<String,String> extraParameters = new HashMap<>();

        public QueryDriver withExpectedTagCloud(DefaultTagCloud tagCloud) {
            expectedResults.add(tagCloud);
            return this;
        }

        public QueryDriver withLogic(KeywordChainedUUIDQueryLogic logic) {
            this.logic = logic;
            return this;
        }

        public QueryDriver withQuery(String query) {
            this.query = query;
            return this;
        }

        public QueryDriver withAuths(Authorizations auths) {
            this.auths = auths;
            return this;
        }

        public QueryDriver withExtraParameters(Map<String,String> extraParameters) {
            this.extraParameters.putAll(extraParameters);
            return this;
        }

        public void run() throws Exception {
            QueryImpl settings = new QueryImpl();
            settings.setPagesize(Integer.MAX_VALUE);
            settings.setQueryAuthorizations(auths.toString());
            settings.setQuery(this.query);
            Calendar calendar = Calendar.getInstance();
            calendar.set(Calendar.YEAR, 2000);
            settings.setBeginDate(calendar.getTime());
            settings.setEndDate(new Date());
            settings.setParameters(extraParameters);
            settings.setId(UUID.randomUUID());

            GenericQueryConfiguration config = logic.initialize(connector, settings, Set.of(auths));
            logic.setupQuery(config);

            queryDriver = new ResponseQueryDriver<>(logic);
            BaseQueryResponse response = queryDriver.drive(config);
            assertNotNull(response);
            assertTrue(response instanceof DefaultTagCloudResponse);
            DefaultTagCloudResponse tagCloudResponse = (DefaultTagCloudResponse) response;
            if (expectedResults.isEmpty()) {
                assertNull(tagCloudResponse.getTagClouds());
            } else {
                assertEquals(expectedResults.size(), tagCloudResponse.getTagClouds().size());
                List<TagCloudBase> found = new ArrayList<>();
                for (TagCloudBase tagCloud : tagCloudResponse.getTagClouds()) {
                    if (!expectedResults.contains(tagCloud)) {
                        fail("unexpected tag cloud: " + tagCloud);
                    }
                    found.add(tagCloud);
                }
                assertEquals(found.size(), expectedResults.size());
            }
        }
    }
}
