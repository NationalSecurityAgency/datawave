package datawave.query.tables.keyword;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import datawave.configuration.spring.SpringBean;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.microservice.query.QueryImpl;
import datawave.query.ExcerptTest;
import datawave.query.QueryTestTableHelper;
import datawave.query.tables.ResponseQueryDriver;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.tables.keyword.transform.KeywordResultsTransformer;
import datawave.query.tables.keyword.transform.TagCloudPartitionTransformer;
import datawave.query.util.WiseGuysIngest;
import datawave.util.TableName;
import datawave.util.keyword.TagCloudInput;
import datawave.util.keyword.TagCloudPartition;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.result.keyword.DefaultTagCloud;
import datawave.webservice.result.keyword.DefaultTagCloudEntry;
import datawave.webservice.result.keyword.DefaultTagCloudResponse;
import datawave.webservice.result.keyword.TagCloudBase;

@RunWith(Arquillian.class)
public class KeywordQueryLogicFunctionalTest {
    protected static AccumuloClient connector = null;

    private static final Logger log = Logger.getLogger(KeywordQueryLogicFunctionalTest.class);
    protected Authorizations auths = new Authorizations("ALL");
    protected Set<Authorizations> authSet = Set.of(auths);

    @Inject
    @SpringBean(name = "KeywordQuery")
    protected KeywordQueryLogic logic;

    private final TagCloudPartitionTransformer tagCloudPartitionTransformer = TagCloudPartitionTransformer.getInstance();

    private final Map<String,String> extraParameters = new HashMap<>();
    private final List<DefaultTagCloud> expectedResults = new ArrayList<>();

    private ResponseQueryDriver<Entry<Key,Value>> queryDriver;

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

        queryDriver = new ResponseQueryDriver<>(logic);
    }

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

    private DefaultTagCloud getKeywordCloud(String docId, String version) {
        List<DefaultTagCloudEntry> entries = new ArrayList<>();
        entries.add(createTagCloudEntry("kind word", 0.2052, 1, List.of(docId)));
        entries.add(createTagCloudEntry("kind", 0.2546, 1, List.of(docId)));
        entries.add(createTagCloudEntry("word", 0.2857, 1, List.of(docId)));
        entries.add(createTagCloudEntry("kind word alone", 0.4375, 1, List.of(docId)));
        entries.add(createTagCloudEntry("word alone", 0.534, 1, List.of(docId)));
        entries.add(createTagCloudEntry("get much", 0.5903, 1, List.of(docId)));
        entries.add(createTagCloudEntry("much farther", 0.5903, 1, List.of(docId)));

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

    @Test
    public void simpleV1Test() throws Exception {
        String docId = "20130101_0/test/-cvy0gj.tlf59s.-duxzua";
        String queryString = "DOCUMENT:" + docId;

        addExpectedTagCloud(getKeywordCloud(docId, "1"));
        runTestQuery(queryString);
    }

    @Test
    public void simpleV2Test() throws Exception {
        String docId = "20130101_0/test/-cvy0gj.tlf59s.-duxzua";
        String queryString = "DOCUMENT:" + docId;

        extraParameters.put("tag.cloud.version", "2");
        addExpectedTagCloud(getKeywordCloud(docId, "2"));
        runTestQuery(queryString);
    }

    @Test
    public void simpleWithOnlyExternalHitsV1Test() throws Exception {
        String queryString = "DOCUMENT:20130101_0/test/-cvy0gj.tlf59s.-duxzuab";

        TagCloudPartition externalPartition = new TagCloudPartition("FOO", "FOO", List
                        .of(new TagCloudInput("20130101_0/test/-cvy0gj.tlf59s.-duxzuab", "ALL", Map.of("x", .5d, "y", .8d, "z", 1d), Map.of("type", "demo"))));
        logic.setExternalData(List.of(tagCloudPartitionTransformer.encode(externalPartition)), Set.of(tagCloudPartitionTransformer));

        List<DefaultTagCloudEntry> entries = new ArrayList<>();
        entries.add(createTagCloudEntry("x", .5, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuab")));
        entries.add(createTagCloudEntry("y", .8, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuab")));
        entries.add(createTagCloudEntry("z", 1, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuab")));
        addExpectedTagCloud(getExpectedCloud("1", Map.of("type", "demo"), entries));

        runTestQuery(queryString);
    }

    @Test
    public void simpleWithOnlyExternalHitsV2Test() throws Exception {
        String queryString = "DOCUMENT:20130101_0/test/-cvy0gj.tlf59s.-duxzuab";

        TagCloudPartition externalPartition = new TagCloudPartition("FOO", "FOO", List
                        .of(new TagCloudInput("20130101_0/test/-cvy0gj.tlf59s.-duxzuab", "ALL", Map.of("x", .5d, "y", .8d, "z", 1d), Map.of("type", "demo"))));
        logic.setExternalData(List.of(tagCloudPartitionTransformer.encode(externalPartition)), Set.of(tagCloudPartitionTransformer));

        List<DefaultTagCloudEntry> entries = new ArrayList<>();
        entries.add(createTagCloudEntry("x", .5, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuab")));
        entries.add(createTagCloudEntry("y", .8, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuab")));
        entries.add(createTagCloudEntry("z", 1, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuab")));

        extraParameters.put("tag.cloud.version", "2");
        addExpectedTagCloud(getExpectedCloud("2", Map.of("type", "demo"), entries));

        runTestQuery(queryString);
    }

    @Test
    public void multipleExternalHitsV1Test() throws Exception {
        String queryString = "DOCUMENT:20130101_0/test/-cvy0gj.tlf59s.-duxzuab";

        TagCloudPartition externalPartition = new TagCloudPartition("FOO", "FOO", TagCloudPartition.ScoreType.HIGHER_IS_BETTER, List.of(
                        new TagCloudInput("20130101_0/test/-cvy0gj.tlf59s.-duxzuab", "ALL", Map.of("x", .5d, "y", .8d, "z", 1d), Map.of("type", "demo")),
                        new TagCloudInput("20130101_0/test/-cvy0gj.tlf59s.-duxzuabc", "ALL", Map.of("x", .3d, "y", .9d, "a", .7d), Map.of("type", "demo"))));
        logic.setExternalData(List.of(tagCloudPartitionTransformer.encode(externalPartition)), Set.of(tagCloudPartitionTransformer));

        List<DefaultTagCloudEntry> entries = new ArrayList<>();
        entries.add(createTagCloudEntry("x", .5, 2, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuabc", "20130101_0/test/-cvy0gj.tlf59s.-duxzuab")));
        entries.add(createTagCloudEntry("y", .9, 2, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuabc", "20130101_0/test/-cvy0gj.tlf59s.-duxzuab")));
        entries.add(createTagCloudEntry("z", 1, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuab")));
        entries.add(createTagCloudEntry("a", .7, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuabc")));

        addExpectedTagCloud(getExpectedCloud("1", Map.of("type", "demo"), entries));

        runTestQuery(queryString);
    }

    @Test
    public void multipleExternalHitsV2Test() throws Exception {
        String queryString = "DOCUMENT:20130101_0/test/-cvy0gj.tlf59s.-duxzuab";

        TagCloudPartition externalPartition = new TagCloudPartition("FOO", "FOO", TagCloudPartition.ScoreType.HIGHER_IS_BETTER, List.of(
                        new TagCloudInput("20130101_0/test/-cvy0gj.tlf59s.-duxzuab", "ALL", Map.of("x", .5d, "y", .8d, "z", 1d), Map.of("type", "demo")),
                        new TagCloudInput("20130101_0/test/-cvy0gj.tlf59s.-duxzuabc", "ALL", Map.of("x", .3d, "y", .9d, "a", .7d), Map.of("type", "demo"))));
        logic.setExternalData(List.of(tagCloudPartitionTransformer.encode(externalPartition)), Set.of(tagCloudPartitionTransformer));

        List<DefaultTagCloudEntry> entries = new ArrayList<>();
        entries.add(createTagCloudEntry("x", .5, 2, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuabc", "20130101_0/test/-cvy0gj.tlf59s.-duxzuab")));
        entries.add(createTagCloudEntry("y", .9, 2, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuabc", "20130101_0/test/-cvy0gj.tlf59s.-duxzuab")));
        entries.add(createTagCloudEntry("z", 1, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuab")));
        entries.add(createTagCloudEntry("a", .7, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuabc")));

        extraParameters.put("tag.cloud.version", "2");
        addExpectedTagCloud(getExpectedCloud("2", Map.of("type", "demo"), entries));

        runTestQuery(queryString);
    }

    @Test
    public void mixedHitV1Test() throws Exception {
        String docId = "20130101_0/test/-cvy0gj.tlf59s.-duxzua";
        String queryString = "DOCUMENT:" + docId;

        TagCloudPartition externalPartition = new TagCloudPartition("FOO", "FOO", TagCloudPartition.ScoreType.HIGHER_IS_BETTER, List.of(
                        new TagCloudInput("20130101_0/test/-cvy0gj.tlf59s.-duxzua", "ALL", Map.of("x", .5d, "y", .8d, "z", 1d), Map.of("type", "demo")),
                        new TagCloudInput("20130101_0/test/-cvy0gj.tlf59s.-duxzua", "ALL", Map.of("x", .3d, "y", .9d, "a", .7d), Map.of("type", "demo"))));
        logic.setExternalData(List.of(tagCloudPartitionTransformer.encode(externalPartition)), Set.of(tagCloudPartitionTransformer));

        List<DefaultTagCloudEntry> entries = new ArrayList<>();
        entries.add(createTagCloudEntry("x", .5, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzua")));
        entries.add(createTagCloudEntry("y", .9, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzua")));
        entries.add(createTagCloudEntry("z", 1, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzua")));
        entries.add(createTagCloudEntry("a", .7, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzua")));

        addExpectedTagCloud(getExpectedCloud("1", Map.of("type", "demo"), entries));
        addExpectedTagCloud(getKeywordCloud(docId, "1"));

        runTestQuery(queryString);
    }

    @Test
    public void mixedHitV2Test() throws Exception {
        String docId = "20130101_0/test/-cvy0gj.tlf59s.-duxzua";
        String queryString = "DOCUMENT:" + docId;

        TagCloudPartition externalPartition = new TagCloudPartition("FOO", "FOO", TagCloudPartition.ScoreType.HIGHER_IS_BETTER, List.of(
                        new TagCloudInput("20130101_0/test/-cvy0gj.tlf59s.-duxzua", "ALL", Map.of("x", .5d, "y", .8d, "z", 1d), Map.of("type", "demo")),
                        new TagCloudInput("20130101_0/test/-cvy0gj.tlf59s.-duxzua", "ALL", Map.of("x", .3d, "y", .9d, "a", .7d), Map.of("type", "demo"))));
        logic.setExternalData(List.of(tagCloudPartitionTransformer.encode(externalPartition)), Set.of(tagCloudPartitionTransformer));

        List<DefaultTagCloudEntry> entries = new ArrayList<>();
        entries.add(createTagCloudEntry("x", .5, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzua")));
        entries.add(createTagCloudEntry("y", .9, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzua")));
        entries.add(createTagCloudEntry("z", 1, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzua")));
        entries.add(createTagCloudEntry("a", .7, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzua")));

        extraParameters.put("tag.cloud.version", "2");
        addExpectedTagCloud(getExpectedCloud("2", Map.of("type", "demo"), entries));
        addExpectedTagCloud(getKeywordCloud(docId, "2"));

        runTestQuery(queryString);
    }

    @Test
    public void noHitTest() throws Exception {
        runTestQuery("");
    }

    private void addExpectedTagCloud(DefaultTagCloud expected) {
        expectedResults.add(expected);
    }

    private DefaultTagCloudEntry createTagCloudEntry(String term, double score, int frequency, List<String> sources) {
        DefaultTagCloudEntry entry = new DefaultTagCloudEntry();
        entry.setTerm(term);
        entry.setScore(score);
        entry.setFrequency(frequency);
        entry.setSources(sources);

        return entry;
    }

    protected void runTestQuery(String queryString) throws Exception {
        QueryImpl settings = new QueryImpl();
        settings.setPagesize(Integer.MAX_VALUE);
        settings.setQueryAuthorizations(auths.serialize());
        settings.setQuery(queryString);
        settings.setParameters(extraParameters);
        settings.setId(UUID.randomUUID());

        log.debug("query: " + settings.getQuery());
        log.debug("logic: " + settings.getQueryLogicName());

        GenericQueryConfiguration config = logic.initialize(connector, settings, authSet);
        logic.setupQuery(config);

        DefaultTagCloudResponse response = (DefaultTagCloudResponse) queryDriver.drive(config);
        // check the response clouds are expected
        List<TagCloudBase> found = new ArrayList<>();

        if (!expectedResults.isEmpty()) {
            assertEquals(expectedResults.size(), response.getTagClouds().size());
            for (TagCloudBase tagCloud : response.getTagClouds()) {
                if (!expectedResults.contains(tagCloud)) {
                    fail("unexpected tag cloud: " + tagCloud);
                }
                found.add(tagCloud);
            }
        } else {
            assertNull(response.getTagClouds());
        }

        // nothing still expected
        assertEquals(found.size(), expectedResults.size());
    }

    private boolean isExpectedTagCloud(DefaultTagCloud tagCloud) {
        DefaultTagCloud expected = null;
        for (DefaultTagCloud expectedCloud : expectedResults) {
            if (Objects.equals(expectedCloud.getMetadata(), tagCloud.getMetadata())) {
                List<DefaultTagCloudEntry> expectedTags = expectedCloud.getTags();
                if (expectedTags.containsAll(tagCloud.getTags()) && tagCloud.getTags().containsAll(expectedTags)) {
                    expected = expectedCloud;
                    break;
                }
            }
        }

        if (expected != null) {
            expectedResults.remove(expected);
            return true;
        }

        return false;
    }

    @AfterClass
    public static void teardown() {
        TypeRegistry.reset();
    }

}
