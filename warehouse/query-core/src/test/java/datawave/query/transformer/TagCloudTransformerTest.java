package datawave.query.transformer;

import static datawave.query.transformer.TagCloudTransformer.DEFAULT_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Before;
import org.junit.Test;

import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.tables.keyword.KeywordQueryState;
import datawave.query.tables.keyword.transform.KeywordResultsTransformer;
import datawave.query.tables.keyword.transform.TagCloudInputTransformer;
import datawave.query.tables.keyword.transform.TagCloudPartitionTransformer;
import datawave.util.keyword.DefaultTagCloudUtils;
import datawave.util.keyword.KeywordResults;
import datawave.util.keyword.TagCloudInput;
import datawave.util.keyword.TagCloudPartition;
import datawave.webservice.result.keyword.DefaultTagCloud;
import datawave.webservice.result.keyword.DefaultTagCloudResponse;

public class TagCloudTransformerTest {
    private Query query;
    private KeywordQueryState state;

    private static final Map<String,Double> ENTITY_MAP = Map.of("x", .8, "y", .3, "z", .1);

    @Before
    public void setup() {
        query = new QueryImpl();
        query.setQueryAuthorizations("A,B,C");

        state = new KeywordQueryState();
        state.setTagCloudUtils(new DefaultTagCloudUtils());
    }

    @Test
    public void nullVersionTest() {
        TagCloudTransformer transformer = new TagCloudTransformer(null, query, new KeywordQueryState(), new MarkingFunctions.Default(),
                        new DefaultResponseObjectFactory(), Set.of());
        assertEquals(DEFAULT_VERSION, transformer.getResponseVersion());
    }

    @Test
    public void version1Test() {
        TagCloudTransformer transformer = new TagCloudTransformer("1", query, new KeywordQueryState(), new MarkingFunctions.Default(),
                        new DefaultResponseObjectFactory(), Set.of());
        assertEquals(TagCloudTransformer.ResponseVersion.V1, transformer.getResponseVersion());
        transformer = new TagCloudTransformer("v1", query, new KeywordQueryState(), new MarkingFunctions.Default(), new DefaultResponseObjectFactory(),
                        Set.of());
        assertEquals(TagCloudTransformer.ResponseVersion.V1, transformer.getResponseVersion());
        transformer = new TagCloudTransformer("V1", query, new KeywordQueryState(), new MarkingFunctions.Default(), new DefaultResponseObjectFactory(),
                        Set.of());
        assertEquals(TagCloudTransformer.ResponseVersion.V1, transformer.getResponseVersion());
    }

    @Test
    public void version2Test() {
        TagCloudTransformer transformer = new TagCloudTransformer("2", query, new KeywordQueryState(), new MarkingFunctions.Default(),
                        new DefaultResponseObjectFactory(), Set.of());
        assertEquals(TagCloudTransformer.ResponseVersion.V2, transformer.getResponseVersion());
        transformer = new TagCloudTransformer("v2", query, new KeywordQueryState(), new MarkingFunctions.Default(), new DefaultResponseObjectFactory(),
                        Set.of());
        assertEquals(TagCloudTransformer.ResponseVersion.V2, transformer.getResponseVersion());
        transformer = new TagCloudTransformer("V2", query, new KeywordQueryState(), new MarkingFunctions.Default(), new DefaultResponseObjectFactory(),
                        Set.of());
        assertEquals(TagCloudTransformer.ResponseVersion.V2, transformer.getResponseVersion());
    }

    @Test
    public void defaultVersionTest() {
        TagCloudTransformer transformer = new TagCloudTransformer("3", query, new KeywordQueryState(), new MarkingFunctions.Default(),
                        new DefaultResponseObjectFactory(), Set.of());
        assertEquals(DEFAULT_VERSION, transformer.getResponseVersion());
        transformer = new TagCloudTransformer("v3", query, new KeywordQueryState(), new MarkingFunctions.Default(), new DefaultResponseObjectFactory(),
                        Set.of());
        assertEquals(DEFAULT_VERSION, transformer.getResponseVersion());
        transformer = new TagCloudTransformer("V3", query, new KeywordQueryState(), new MarkingFunctions.Default(), new DefaultResponseObjectFactory(),
                        Set.of());
        assertEquals(DEFAULT_VERSION, transformer.getResponseVersion());
    }

    @Test
    public void defaultIsV1Test() {
        assertEquals(DEFAULT_VERSION, TagCloudTransformer.ResponseVersion.V1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void transformNoTransformerTest() {
        TagCloudTransformer transformer = new TagCloudTransformer(null, query, new KeywordQueryState(), new MarkingFunctions.Default(),
                        new DefaultResponseObjectFactory(), Set.of());
        transformer.transform(Map.entry(new Key(), new Value()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void transformCantDecodeTest() {
        NeverDecodingTagCloudInputTransformer neverDecodingTagCloudInputTransformer = new NeverDecodingTagCloudInputTransformer();
        TagCloudTransformer transformer = new TagCloudTransformer(null, query, new KeywordQueryState(), new MarkingFunctions.Default(),
                        new DefaultResponseObjectFactory(), Set.of(neverDecodingTagCloudInputTransformer));
        transformer.transform(Map.entry(new Key(), new Value()));
    }

    @Test
    public void transformTest() {
        EmptyTagCloudInputTransformer emptyTagCloudInputTransformer = new EmptyTagCloudInputTransformer();
        TagCloudTransformer transformer = new TagCloudTransformer(null, query, new KeywordQueryState(), new MarkingFunctions.Default(),
                        new DefaultResponseObjectFactory(), Set.of(emptyTagCloudInputTransformer));
        TagCloudPartition partition = transformer.transform(Map.entry(new Key(), new Value()));
        assertNotNull(partition);
        assertEquals(new TagCloudPartition(), partition);
    }

    @Test
    public void transformMultipleTest() {
        NeverDecodingTagCloudInputTransformer neverDecodingTagCloudInputTransformer = new NeverDecodingTagCloudInputTransformer();
        EmptyTagCloudInputTransformer emptyTagCloudInputTransformer = new EmptyTagCloudInputTransformer();
        TagCloudTransformer transformer = new TagCloudTransformer(null, query, new KeywordQueryState(), new MarkingFunctions.Default(),
                        new DefaultResponseObjectFactory(), Set.of(neverDecodingTagCloudInputTransformer, emptyTagCloudInputTransformer));
        TagCloudPartition partition = transformer.transform(Map.entry(new Key(), new Value()));
        assertNotNull(partition);
        assertEquals(new TagCloudPartition(), partition);
    }

    @Test
    public void transformKeywordResultsTest() throws IOException {
        KeywordResults keywordResults = new KeywordResults("source", "view", "language", "vis", ENTITY_MAP);
        Key k = new Key();
        Value v = new Value(KeywordResults.serialize(keywordResults));

        KeywordResultsTransformer keywordResultsTransformer = new KeywordResultsTransformer();
        TagCloudTransformer transformer = new TagCloudTransformer(null, query, new KeywordQueryState(), new MarkingFunctions.Default(),
                        new DefaultResponseObjectFactory(), Set.of(keywordResultsTransformer));
        TagCloudPartition partition = transformer.transform(Map.entry(k, v));
        assertNotNull(partition);
        assertEquals("language", partition.getPartition());
        assertEquals(TagCloudPartition.ScoreType.LOWER_IS_BETTER, partition.getScoreType());
        assertEquals(1, partition.getInputs().size());
        assertEquals("vis", partition.getInputs().get(0).getVisibility());
        assertEquals("source", partition.getInputs().get(0).getSource());
        assertEquals(ENTITY_MAP, partition.getInputs().get(0).getEntities());
        assertEquals(Map.of("view", "view", "language", "language", "type", "keyword"), partition.getInputs().get(0).getMetadata());
    }

    @Test
    public void transformTagCloudPartitionTest() {
        Map<String,String> metadata = Map.of("type", "data");
        TagCloudPartitionTransformer tagCloudPartitionTransformer = TagCloudPartitionTransformer.getInstance();
        TagCloudPartition p = new TagCloudPartition("data");
        p.addInput(new TagCloudInput("source", "vis", ENTITY_MAP, metadata));
        Entry<Key,Value> entry = tagCloudPartitionTransformer.encode(p);

        TagCloudTransformer transformer = new TagCloudTransformer(null, query, new KeywordQueryState(), new MarkingFunctions.Default(),
                        new DefaultResponseObjectFactory(), Set.of(tagCloudPartitionTransformer));
        TagCloudPartition transformed = transformer.transform(entry);
        assertEquals(p, transformed);
    }

    @Test
    public void createResponseLanguageV1Test() {
        Map<String,String> metadata = Map.of("type", "data", "language", "english");
        TagCloudPartition p = new TagCloudPartition("data");
        p.addInput(new TagCloudInput("source", "vis", ENTITY_MAP, metadata));

        TagCloudTransformer transformer = new TagCloudTransformer(null, query, state, new MarkingFunctions.Default(), new DefaultResponseObjectFactory(),
                        Set.of());
        assertEquals(TagCloudTransformer.ResponseVersion.V1, transformer.getResponseVersion());

        Object o = transformer.createResponse(List.of(p));
        assertTrue(o instanceof DefaultTagCloudResponse);
        DefaultTagCloudResponse response = (DefaultTagCloudResponse) o;
        assertEquals(1, response.getTagClouds().size());
        assertTrue(response.getTagClouds().get(0) instanceof DefaultTagCloud);
        DefaultTagCloud tagCloud = (DefaultTagCloud) response.getTagClouds().get(0);
        assertEquals("english", tagCloud.getLanguage());
        assertNull(tagCloud.getMetadata());
        assertEquals(3, tagCloud.getTags().size());
    }

    @Test
    public void createResponseLanguageV2Test() {
        Map<String,String> metadata = Map.of("type", "data", "language", "english");
        TagCloudPartition p = new TagCloudPartition("data");
        p.addInput(new TagCloudInput("source", "vis", ENTITY_MAP, metadata));

        TagCloudTransformer transformer = new TagCloudTransformer("2", query, state, new MarkingFunctions.Default(), new DefaultResponseObjectFactory(),
                        Set.of());
        assertEquals(TagCloudTransformer.ResponseVersion.V2, transformer.getResponseVersion());

        Object o = transformer.createResponse(List.of(p));
        assertTrue(o instanceof DefaultTagCloudResponse);
        DefaultTagCloudResponse response = (DefaultTagCloudResponse) o;
        assertEquals(1, response.getTagClouds().size());
        assertTrue(response.getTagClouds().get(0) instanceof DefaultTagCloud);
        DefaultTagCloud tagCloud = (DefaultTagCloud) response.getTagClouds().get(0);
        assertEquals(metadata, tagCloud.getMetadata());
        assertNull(tagCloud.getLanguage());
        assertEquals(3, tagCloud.getTags().size());
    }

    @Test
    public void createResponseNoLanguageV1Test() {
        Map<String,String> metadata = Map.of("type", "data");
        TagCloudPartition p = new TagCloudPartition("data");
        p.addInput(new TagCloudInput("source", "vis", ENTITY_MAP, metadata));

        TagCloudTransformer transformer = new TagCloudTransformer(null, query, state, new MarkingFunctions.Default(), new DefaultResponseObjectFactory(),
                        Set.of());
        assertEquals(TagCloudTransformer.ResponseVersion.V1, transformer.getResponseVersion());

        Object o = transformer.createResponse(List.of(p));
        assertTrue(o instanceof DefaultTagCloudResponse);
        DefaultTagCloudResponse response = (DefaultTagCloudResponse) o;
        assertEquals(1, response.getTagClouds().size());
        assertTrue(response.getTagClouds().get(0) instanceof DefaultTagCloud);
        DefaultTagCloud tagCloud = (DefaultTagCloud) response.getTagClouds().get(0);
        assertEquals("data", tagCloud.getLanguage());
        assertNull(tagCloud.getMetadata());
        assertEquals(3, tagCloud.getTags().size());
    }

    @Test
    public void createResponseNoLanguageV2Test() {
        Map<String,String> metadata = Map.of("type", "data");
        TagCloudPartition p = new TagCloudPartition("data");
        p.addInput(new TagCloudInput("source", "vis", ENTITY_MAP, metadata));

        TagCloudTransformer transformer = new TagCloudTransformer("2", query, state, new MarkingFunctions.Default(), new DefaultResponseObjectFactory(),
                        Set.of());
        assertEquals(TagCloudTransformer.ResponseVersion.V2, transformer.getResponseVersion());

        Object o = transformer.createResponse(List.of(p));
        assertTrue(o instanceof DefaultTagCloudResponse);
        DefaultTagCloudResponse response = (DefaultTagCloudResponse) o;
        assertEquals(1, response.getTagClouds().size());
        assertTrue(response.getTagClouds().get(0) instanceof DefaultTagCloud);
        DefaultTagCloud tagCloud = (DefaultTagCloud) response.getTagClouds().get(0);
        assertEquals(metadata, tagCloud.getMetadata());
        assertNull(tagCloud.getLanguage());
        assertEquals(3, tagCloud.getTags().size());
    }

    private static class EmptyTagCloudInputTransformer implements TagCloudInputTransformer {

        @Override
        public Map.Entry<Key,Value> encode(Object input) {
            return null;
        }

        @Override
        public TagCloudPartition decode(Map.Entry input) {
            return new TagCloudPartition();
        }

        @Override
        public boolean canDecode(Map.Entry input) {
            return true;
        }
    }

    private static class NeverDecodingTagCloudInputTransformer implements TagCloudInputTransformer {

        @Override
        public Map.Entry<Key,Value> encode(Object input) {
            return null;
        }

        @Override
        public TagCloudPartition decode(Map.Entry input) {
            return null;
        }

        @Override
        public boolean canDecode(Map.Entry input) {
            return false;
        }
    }
}
