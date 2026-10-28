package datawave.query.tables.keyword;

import static datawave.query.tables.keyword.TagCloudTestUtil.CAPONE_SOURCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryTestTableHelper;
import datawave.query.tables.ResponseQueryDriver;
import datawave.query.tables.keyword.transform.TagCloudPartitionTransformer;
import datawave.query.util.WiseGuysIngest;
import datawave.util.keyword.TagCloudInput;
import datawave.util.keyword.TagCloudPartition;
import datawave.webservice.result.keyword.DefaultTagCloud;
import datawave.webservice.result.keyword.DefaultTagCloudEntry;
import datawave.webservice.result.keyword.DefaultTagCloudResponse;
import datawave.webservice.result.keyword.TagCloudBase;

@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = {"datawave.configuration.spring", "datawave.query"})
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:beanRefContext.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
public class KeywordQueryLogicFunctionalTest {

    private static final Logger log = Logger.getLogger(KeywordQueryLogicFunctionalTest.class);

    protected static AccumuloClient connector;

    protected Authorizations auths = new Authorizations("ALL");
    protected Set<Authorizations> authSet = Set.of(auths);

    @Autowired
    @Qualifier("KeywordQuery")
    protected KeywordQueryLogic logic;

    private final TagCloudPartitionTransformer tagCloudPartitionTransformer = TagCloudPartitionTransformer.getInstance();

    private final Map<String,String> extraParameters = new HashMap<>();
    private final List<DefaultTagCloud> expectedResults = new ArrayList<>();

    private ResponseQueryDriver<Entry<Key,Value>> queryDriver;
    private final TagCloudTestUtil tagCloudTestUtil = new TagCloudTestUtil();

    @BeforeAll
    public static void setUp() throws Exception {
        QueryTestTableHelper qtth = new QueryTestTableHelper(KeywordQueryLogicFunctionalTest.class.toString(), log);
        connector = qtth.client;
        WiseGuysIngest.writeItAll(connector, WiseGuysIngest.WhatKindaRange.DOCUMENT);
    }

    @BeforeEach
    public void setup() {
        queryDriver = new ResponseQueryDriver<>(logic);
    }

    @Test
    public void simpleV1Test() throws Exception {
        String docId = CAPONE_SOURCE;
        String queryString = "DOCUMENT:" + docId;

        addExpectedTagCloud(tagCloudTestUtil.getCaponeKeywordCloud(docId, "1"));
        runTestQuery(queryString);
    }

    @Test
    public void simpleV2Test() throws Exception {
        String docId = CAPONE_SOURCE;
        String queryString = "DOCUMENT:" + docId;

        extraParameters.put("tag.cloud.version", "2");
        addExpectedTagCloud(tagCloudTestUtil.getCaponeKeywordCloud(docId, "2"));
        runTestQuery(queryString);
    }

    @Test
    public void simpleWithOnlyExternalHitsV1Test() throws Exception {
        String queryString = "DOCUMENT:20130101_0/test/-cvy0gj.tlf59s.-duxzuab";

        TagCloudPartition externalPartition = new TagCloudPartition("FOO", "FOO", List
                        .of(new TagCloudInput("20130101_0/test/-cvy0gj.tlf59s.-duxzuab", "ALL", Map.of("x", .5d, "y", .8d, "z", 1d), Map.of("type", "demo"))));
        logic.setExternalData(List.of(tagCloudPartitionTransformer.encode(externalPartition)), Set.of(tagCloudPartitionTransformer));

        List<DefaultTagCloudEntry> entries = new ArrayList<>();
        entries.add(tagCloudTestUtil.createTagCloudEntry("x", .5, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuab")));
        entries.add(tagCloudTestUtil.createTagCloudEntry("y", .8, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuab")));
        entries.add(tagCloudTestUtil.createTagCloudEntry("z", 1, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuab")));
        addExpectedTagCloud(tagCloudTestUtil.getExpectedCloud("1", Map.of("type", "demo"), entries));

        runTestQuery(queryString);
    }

    @Test
    public void simpleWithOnlyExternalHitsV2Test() throws Exception {
        String queryString = "DOCUMENT:20130101_0/test/-cvy0gj.tlf59s.-duxzuab";

        TagCloudPartition externalPartition = new TagCloudPartition("FOO", "FOO",
                        List.of(new TagCloudInput(CAPONE_SOURCE, "ALL", Map.of("x", .5d, "y", .8d, "z", 1d), Map.of("type", "demo"))));
        logic.setExternalData(List.of(tagCloudPartitionTransformer.encode(externalPartition)), Set.of(tagCloudPartitionTransformer));

        List<DefaultTagCloudEntry> entries = new ArrayList<>();
        entries.add(tagCloudTestUtil.createTagCloudEntry("x", .5, 1, List.of(CAPONE_SOURCE)));
        entries.add(tagCloudTestUtil.createTagCloudEntry("y", .8, 1, List.of(CAPONE_SOURCE)));
        entries.add(tagCloudTestUtil.createTagCloudEntry("z", 1, 1, List.of(CAPONE_SOURCE)));

        extraParameters.put("tag.cloud.version", "2");
        addExpectedTagCloud(tagCloudTestUtil.getExpectedCloud("2", Map.of("type", "demo"), entries));

        runTestQuery(queryString);
    }

    @Test
    public void multipleExternalHitsV1Test() throws Exception {
        String queryString = "DOCUMENT:20130101_0/test/-cvy0gj.tlf59s.-duxzuab";

        TagCloudPartition externalPartition = new TagCloudPartition("FOO", "FOO", TagCloudPartition.ScoreType.HIGHER_IS_BETTER, List.of(
                        new TagCloudInput(CAPONE_SOURCE, "ALL", Map.of("x", .5d, "y", .8d, "z", 1d), Map.of("type", "demo")),
                        new TagCloudInput("20130101_0/test/-cvy0gj.tlf59s.-duxzuabc", "ALL", Map.of("x", .3d, "y", .9d, "a", .7d), Map.of("type", "demo"))));
        logic.setExternalData(List.of(tagCloudPartitionTransformer.encode(externalPartition)), Set.of(tagCloudPartitionTransformer));

        List<DefaultTagCloudEntry> entries = new ArrayList<>();
        entries.add(tagCloudTestUtil.createTagCloudEntry("x", .5, 2, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuabc", CAPONE_SOURCE)));
        entries.add(tagCloudTestUtil.createTagCloudEntry("y", .9, 2, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuabc", CAPONE_SOURCE)));
        entries.add(tagCloudTestUtil.createTagCloudEntry("z", 1, 1, List.of(CAPONE_SOURCE)));
        entries.add(tagCloudTestUtil.createTagCloudEntry("a", .7, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuabc")));

        addExpectedTagCloud(tagCloudTestUtil.getExpectedCloud("1", Map.of("type", "demo"), entries));

        runTestQuery(queryString);
    }

    @Test
    public void multipleExternalHitsV2Test() throws Exception {
        String queryString = "DOCUMENT:20130101_0/test/-cvy0gj.tlf59s.-duxzuab";

        TagCloudPartition externalPartition = new TagCloudPartition("FOO", "FOO", TagCloudPartition.ScoreType.HIGHER_IS_BETTER, List.of(
                        new TagCloudInput(CAPONE_SOURCE, "ALL", Map.of("x", .5d, "y", .8d, "z", 1d), Map.of("type", "demo")),
                        new TagCloudInput("20130101_0/test/-cvy0gj.tlf59s.-duxzuabc", "ALL", Map.of("x", .3d, "y", .9d, "a", .7d), Map.of("type", "demo"))));
        logic.setExternalData(List.of(tagCloudPartitionTransformer.encode(externalPartition)), Set.of(tagCloudPartitionTransformer));

        List<DefaultTagCloudEntry> entries = new ArrayList<>();
        entries.add(tagCloudTestUtil.createTagCloudEntry("x", .5, 2, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuabc", CAPONE_SOURCE)));
        entries.add(tagCloudTestUtil.createTagCloudEntry("y", .9, 2, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuabc", CAPONE_SOURCE)));
        entries.add(tagCloudTestUtil.createTagCloudEntry("z", 1, 1, List.of(CAPONE_SOURCE)));
        entries.add(tagCloudTestUtil.createTagCloudEntry("a", .7, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzuabc")));

        extraParameters.put("tag.cloud.version", "2");
        addExpectedTagCloud(tagCloudTestUtil.getExpectedCloud("2", Map.of("type", "demo"), entries));

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
        entries.add(tagCloudTestUtil.createTagCloudEntry("x", .5, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzua")));
        entries.add(tagCloudTestUtil.createTagCloudEntry("y", .9, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzua")));
        entries.add(tagCloudTestUtil.createTagCloudEntry("z", 1, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzua")));
        entries.add(tagCloudTestUtil.createTagCloudEntry("a", .7, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzua")));

        addExpectedTagCloud(tagCloudTestUtil.getExpectedCloud("1", Map.of("type", "demo"), entries));
        addExpectedTagCloud(tagCloudTestUtil.getCaponeKeywordCloud(docId, "1"));

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
        entries.add(tagCloudTestUtil.createTagCloudEntry("x", .5, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzua")));
        entries.add(tagCloudTestUtil.createTagCloudEntry("y", .9, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzua")));
        entries.add(tagCloudTestUtil.createTagCloudEntry("z", 1, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzua")));
        entries.add(tagCloudTestUtil.createTagCloudEntry("a", .7, 1, List.of("20130101_0/test/-cvy0gj.tlf59s.-duxzua")));

        extraParameters.put("tag.cloud.version", "2");
        addExpectedTagCloud(tagCloudTestUtil.getExpectedCloud("2", Map.of("type", "demo"), entries));
        addExpectedTagCloud(tagCloudTestUtil.getCaponeKeywordCloud(docId, "2"));

        runTestQuery(queryString);
    }

    @Test
    public void noHitTest() throws Exception {
        runTestQuery("");
    }

    private void addExpectedTagCloud(DefaultTagCloud expected) {
        expectedResults.add(expected);
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
}
