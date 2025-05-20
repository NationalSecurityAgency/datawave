package datawave.query.util.keyword;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.LinkedHashMap;

import org.junit.Test;

public class KeywordResultsTest {

    static final String EXPECTED_SOURCE = "NEWS_VIEW";

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
        assertTrue(results.get().isEmpty());
        assertEquals(0, results.size());
        assertTrue(results.getSource().isEmpty());
    }

    @Test
    public void testConstructFull() {
        KeywordResults results = new KeywordResults(EXPECTED_SOURCE, EXPECTED_NEWS_OUTPUT);
        assertEquals(4, results.size());
        assertEquals(EXPECTED_NEWS_OUTPUT, results.get());
        assertEquals(EXPECTED_SOURCE, results.getSource());
    }

    @Test
    public void testSerializeDeserializeEmpty() throws IOException {
        KeywordResults results = new KeywordResults();
        byte[] serialized = KeywordResults.serialize(results);
        KeywordResults deserialized = KeywordResults.deserialize(serialized);
        assertEquals(results.size(), deserialized.size());
        assertEquals(results.get(), deserialized.get());
        assertEquals(results.getSource(), deserialized.getSource());
    }

    @Test
    public void testSerializeDeserializePopulated() throws IOException {
        KeywordResults results = new KeywordResults(EXPECTED_SOURCE, EXPECTED_NEWS_OUTPUT);
        byte[] serialized = KeywordResults.serialize(results);
        KeywordResults deserialized = KeywordResults.deserialize(serialized);
        assertEquals(results.size(), deserialized.size());
        assertEquals(results.get(), deserialized.get());
        assertEquals(results.getSource(), deserialized.getSource());
    }
}
