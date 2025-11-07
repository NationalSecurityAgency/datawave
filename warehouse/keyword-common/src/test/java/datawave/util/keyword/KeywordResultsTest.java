package datawave.util.keyword;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

public class KeywordResultsTest {

    static final String EXPECTED_SOURCE = "NEWS_SOURCE";
    static final String EXPECTED_LANGUAGE = "ENGLISH";
    static final String EXPECTED_VIEW = "NEWS_VIEW";
    static final String EXPECTED_VISIBILITY = "NEWS_VISIBILITY";
    static final String EXPECTED_TYPE = "NEWS_TYPE";

    static final Map<String,String> EXPECTED_METADATA = Map.of("view", EXPECTED_VIEW, "language", EXPECTED_LANGUAGE, "type", EXPECTED_TYPE);

    static final LinkedHashMap<String,Double> EXPECTED_NEWS_OUTPUT = new LinkedHashMap<>();
    static {
        LinkedHashMap<String,Double> m = EXPECTED_NEWS_OUTPUT;
        m.put("acquiring kaggle", 0.4602);
        m.put("cloud next", 0.3884);
        m.put("cloud next conference", 0.5552);
        m.put("san francisco", 0.3884);
    }

    @Test
    public void testConstructEmpty() {
        KeywordResults results = new KeywordResults();
        assertTrue(results.getKeywords().isEmpty());
        assertEquals(0, results.getKeywordCount());
        assertTrue(results.getSource().isEmpty());
        // TODO-crwill9 another behavior change here, can't direct getLanguage()
        assertTrue(results.getMetadata().isEmpty());
        assertTrue(results.getVisibility().isEmpty());

    }

    @Test
    public void testConstructFull() {
        KeywordResults results = new KeywordResults(EXPECTED_SOURCE, EXPECTED_VISIBILITY, EXPECTED_NEWS_OUTPUT, EXPECTED_METADATA);
        assertEquals(4, results.getKeywordCount());
        assertEquals(EXPECTED_NEWS_OUTPUT, results.getKeywords());
        assertEquals(EXPECTED_SOURCE, results.getSource());
        assertEquals(EXPECTED_VIEW, results.getMetadata().get("view"));
        assertEquals(EXPECTED_LANGUAGE, results.getMetadata().get("language"));
        assertEquals(EXPECTED_VISIBILITY, results.getVisibility());

    }

    @Test
    public void testSerializeDeserializeEmpty() throws IOException {
        KeywordResults results = new KeywordResults();
        byte[] serialized = KeywordResults.serialize(results);
        KeywordResults deserialized = KeywordResults.deserialize(serialized);
        assertEquals(results.getKeywordCount(), deserialized.getKeywordCount());
        assertEquals(results.getKeywords(), deserialized.getKeywords());
        assertEquals(results.getSource(), deserialized.getSource());
        assertEquals(results.getMetadata().get("language"), deserialized.getMetadata().get("language"));
        assertEquals(results.getVisibility(), deserialized.getVisibility());
    }

    @Test
    public void testSerializeDeserializePopulated() throws IOException {
        KeywordResults results = new KeywordResults(EXPECTED_SOURCE, EXPECTED_VISIBILITY, EXPECTED_NEWS_OUTPUT, EXPECTED_METADATA);
        byte[] serialized = KeywordResults.serialize(results);
        KeywordResults deserialized = KeywordResults.deserialize(serialized);
        assertEquals(results.getKeywordCount(), deserialized.getKeywordCount());
        assertEquals(results.getKeywords(), deserialized.getKeywords());
        assertEquals(results.getSource(), deserialized.getSource());
        assertEquals(results.getMetadata().get("view"), deserialized.getMetadata().get("view"));
        assertEquals(results.getMetadata().get("language"), deserialized.getMetadata().get("language"));
        assertEquals(results.getVisibility(), deserialized.getVisibility());
    }

    @Test
    public void testSerializeDeserializeJsonPopulated() throws IOException {
        KeywordResults results = new KeywordResults(EXPECTED_SOURCE, EXPECTED_VISIBILITY, EXPECTED_NEWS_OUTPUT, EXPECTED_METADATA);
        String resultsJson = TagCloudInput.toJson(results);
        KeywordResults deserialized = (KeywordResults) TagCloudInput.fromJson(resultsJson, KeywordResults.class);
        assertEquals(results.getKeywordCount(), deserialized.getKeywordCount());
        assertEquals(results.getKeywords(), deserialized.getKeywords());
        assertEquals(results.getSource(), deserialized.getSource());
        assertEquals(results.getMetadata().get("view"), deserialized.getMetadata().get("view"));
        assertEquals(results.getMetadata().get("language"), deserialized.getMetadata().get("language"));
        assertEquals(results.getVisibility(), deserialized.getVisibility());
    }
}
