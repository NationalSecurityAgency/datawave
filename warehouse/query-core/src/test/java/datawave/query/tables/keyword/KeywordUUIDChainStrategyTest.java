package datawave.query.tables.keyword;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.easymock.Capture;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import datawave.data.type.NoOpType;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.config.KeywordQueryConfiguration;
import datawave.query.function.serializer.DocumentSerializer;
import datawave.util.keyword.KeywordResults;

public class KeywordUUIDChainStrategyTest extends EasyMockSupport {

    private AccumuloClient mockAccumulo;
    private KeywordQueryLogic mockLogic;
    private KeywordQueryConfiguration mockConfig;
    private KeywordQueryState mockState;

    private Query settings;

    @Before
    public void setup() throws Exception {
        mockAccumulo = createMock(AccumuloClient.class);
        mockLogic = createMock(KeywordQueryLogic.class);
        mockConfig = createMock(KeywordQueryConfiguration.class);
        mockState = createMock(KeywordQueryState.class);

        expect(mockLogic.getLogicName()).andReturn("secondLogic").anyTimes();

        settings = new QueryImpl();
    }

    public Entry<Key,Value> createDocument(String shard, String dt, String uid, String language, String identifier) {
        String colf = dt + "\0" + uid;
        Key documentKey = new Key(shard, colf);

        Document d = new Document();

        d.put("LANGUAGE", new TypeAttribute<>(new NoOpType(language), documentKey, true));
        d.put("HIT_TERM", new Content(identifier, documentKey, true));

        Entry<Key,Document> entry = Map.entry(documentKey, d);

        DocumentSerializer serializer = DocumentSerialization.getDocumentSerializer(DocumentSerialization.DEFAULT_RETURN_TYPE);
        return serializer.apply(entry);
    }

    public Entry<Key,Value> createKeywordResults(String shard, String dt, String uid, String language, String identifier, String view,
                    LinkedHashMap<String,Double> results) throws IOException {
        String colf = "d";
        String colq = dt + "\0" + uid + "\0CONTENT";
        Key documentKey = new Key(shard, colf, colq);
        Value v = new Value(KeywordResults.serialize(new KeywordResults(identifier, view, language, results)));
        return Map.entry(documentKey, v);
    }

    @SuppressWarnings("RedundantOperationOnEmptyContainer")
    @Test
    public void noInputTest() {

        List<Entry<Key,Value>> input = Collections.emptyList();

        KeywordUUIDChainStrategy strategy = new KeywordUUIDChainStrategy();

        /* don't expect _anything_ to happen here */

        replayAll();

        Iterator<Entry<Key,Value>> result = strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        verifyAll();

        assertFalse(result.hasNext());
    }

    @Test
    public void singleInputTest() throws Exception {
        List<Entry<Key,Value>> input = List.of(createDocument("20250412", "test", "-cvy0gj.tlf59s.-duxzua", "ENGLISH", "PAGE_ID:12345"));

        LinkedHashMap<String,Double> results = new LinkedHashMap<>();
        results.put("cat", 0.2);
        results.put("cat food", 0.3);
        results.put("dog", 0.4);

        List<Entry<Key,Value>> intermediateInput = List
                        .of(createKeywordResults("20250412", "test", "-cvy0gj.tlf59s.-duxzua", "ENGLISH", "PAGE_ID:12345", "CONTENT", results));

        KeywordUUIDChainStrategy strategy = new KeywordUUIDChainStrategy();
        Capture<Query> intermediateSettings = Capture.newInstance();

        expect(mockLogic.initialize(eq(mockAccumulo), capture(intermediateSettings), eq(null))).andReturn(mockConfig).once();
        mockLogic.setupQuery(eq(mockConfig));
        expect(mockLogic.iterator()).andReturn(intermediateInput.iterator()).once();

        replayAll();

        Iterator<Entry<Key,Value>> result = strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        verifyAll();

        assertEquals("DOCUMENT:20250412/test/-cvy0gj.tlf59s.-duxzua!PAGE_ID:12345%LANGUAGE:ENGLISH", intermediateSettings.getValue().getQuery());

        assertTrue(result.hasNext());
        Entry<Key,Value> next = result.next();

        assertEquals("20250412 d:test%00;-cvy0gj.tlf59s.-duxzua%00;CONTENT [] 9223372036854775807 false", next.getKey().toString());

        KeywordResults keywordResults = KeywordResults.deserialize(next.getValue().get());
        assertEquals("PAGE_ID:12345", keywordResults.getSource());
        assertEquals("CONTENT", keywordResults.getView());
        assertEquals("ENGLISH", keywordResults.getLanguage());
        assertNotNull(keywordResults.getKeywords().get("cat"));

        assertFalse(result.hasNext());
    }

    @Test
    public void dualInputSingleBatchTest() throws Exception {
        List<Entry<Key,Value>> input = List.of(createDocument("20250412", "test", "-cvy0gj.tlf59s.-duxzua", "ENGLISH", "PAGE_ID:12345"),
                        createDocument("20250412", "test", "-cvy0gj.tlf59s.-duxzub", "NONE", "PAGE_ID:12346"));

        LinkedHashMap<String,Double> resultsOne = new LinkedHashMap<>();
        resultsOne.put("cat", 0.2);
        resultsOne.put("cat food", 0.3);
        resultsOne.put("dog", 0.4);

        LinkedHashMap<String,Double> resultsTwo = new LinkedHashMap<>();
        resultsTwo.put("bird", 0.2);
        resultsTwo.put("bird food", 0.3);
        resultsTwo.put("dog", 0.4);

        List<Entry<Key,Value>> intermediateInput = List.of(
                        createKeywordResults("20250412", "test", "-cvy0gj.tlf59s.-duxzua", "ENGLISH", "PAGE_ID:12345", "CONTENT", resultsOne),
                        createKeywordResults("20250412", "test", "-cvy0gj.tlf59s.-duxzub", "ENGLISH", "PAGE_ID:12346", "INDEXABLE_TEXT", resultsTwo)

        );

        KeywordUUIDChainStrategy strategy = new KeywordUUIDChainStrategy();
        Capture<Query> intermediateSettings = Capture.newInstance();

        expect(mockLogic.initialize(eq(mockAccumulo), capture(intermediateSettings), eq(null))).andReturn(mockConfig).once();
        mockLogic.setupQuery(eq(mockConfig));
        expect(mockLogic.iterator()).andReturn(intermediateInput.iterator()).once();

        replayAll();

        Iterator<Entry<Key,Value>> result = strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        verifyAll();

        assertEquals("DOCUMENT:20250412/test/-cvy0gj.tlf59s.-duxzua!PAGE_ID:12345%LANGUAGE:ENGLISH DOCUMENT:20250412/test/-cvy0gj.tlf59s.-duxzub!PAGE_ID:12346%LANGUAGE:NONE",
                        intermediateSettings.getValue().getQuery());

        assertTrue(result.hasNext());
        Entry<Key,Value> next = result.next();

        assertEquals("20250412 d:test%00;-cvy0gj.tlf59s.-duxzua%00;CONTENT [] 9223372036854775807 false", next.getKey().toString());

        {
            KeywordResults keywordResults = KeywordResults.deserialize(next.getValue().get());
            assertEquals("PAGE_ID:12345", keywordResults.getSource());
            assertEquals("CONTENT", keywordResults.getView());
            assertEquals("ENGLISH", keywordResults.getLanguage());
            assertNotNull(keywordResults.getKeywords().get("cat"));
        }

        assertTrue(result.hasNext());
        assertNotNull(next = result.next());

        {
            KeywordResults keywordResults = KeywordResults.deserialize(next.getValue().get());
            assertEquals("PAGE_ID:12346", keywordResults.getSource());
            assertEquals("INDEXABLE_TEXT", keywordResults.getView());
            assertEquals("ENGLISH", keywordResults.getLanguage());
            assertNotNull(keywordResults.getKeywords().get("bird"));
        }
    }

    @Test
    public void dualInputDualBatchTest() throws Exception {
        List<Entry<Key,Value>> input = List.of(createDocument("20250412", "test", "-cvy0gj.tlf59s.-duxzua", "ENGLISH", "PAGE_ID:12345"),
                        createDocument("20250412", "test", "-cvy0gj.tlf59s.-duxzub", "NONE", "PAGE_ID:12346"));

        LinkedHashMap<String,Double> resultsOne = new LinkedHashMap<>();
        resultsOne.put("cat", 0.2);
        resultsOne.put("cat food", 0.3);
        resultsOne.put("dog", 0.4);

        LinkedHashMap<String,Double> resultsTwo = new LinkedHashMap<>();
        resultsTwo.put("bird", 0.2);
        resultsTwo.put("bird food", 0.3);
        resultsTwo.put("dog", 0.4);

        List<Entry<Key,Value>> intermediateInput = List.of(
                        createKeywordResults("20250412", "test", "-cvy0gj.tlf59s.-duxzua", "ENGLISH", "PAGE_ID:12345", "CONTENT", resultsOne),
                        createKeywordResults("20250412", "test", "-cvy0gj.tlf59s.-duxzub", "ENGLISH", "PAGE_ID:12346", "INDEXABLE_TEXT", resultsTwo)

        );

        KeywordUUIDChainStrategy strategy = new KeywordUUIDChainStrategy();
        Capture<Query> intermediateSettings = Capture.newInstance();

        expect(mockLogic.initialize(eq(mockAccumulo), capture(intermediateSettings), eq(null))).andReturn(mockConfig).once();
        mockLogic.setupQuery(eq(mockConfig));
        expect(mockLogic.iterator()).andReturn(intermediateInput.iterator()).once();

        replayAll();

        strategy.setBatchSize(1);
        Iterator<Entry<Key,Value>> result = strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        verifyAll();

        assertEquals("DOCUMENT:20250412/test/-cvy0gj.tlf59s.-duxzua!PAGE_ID:12345%LANGUAGE:ENGLISH", intermediateSettings.getValue().getQuery());

        assertTrue(result.hasNext());
        Entry<Key,Value> next = result.next();

        assertEquals("20250412 d:test%00;-cvy0gj.tlf59s.-duxzua%00;CONTENT [] 9223372036854775807 false", next.getKey().toString());

        {
            KeywordResults keywordResults = KeywordResults.deserialize(next.getValue().get());
            assertEquals("PAGE_ID:12345", keywordResults.getSource());
            assertEquals("CONTENT", keywordResults.getView());
            assertEquals("ENGLISH", keywordResults.getLanguage());
            assertNotNull(keywordResults.getKeywords().get("cat"));
        }

        assertTrue(result.hasNext());
        assertNotNull(next = result.next());

        {
            KeywordResults keywordResults = KeywordResults.deserialize(next.getValue().get());
            assertEquals("PAGE_ID:12346", keywordResults.getSource());
            assertEquals("INDEXABLE_TEXT", keywordResults.getView());
            assertEquals("ENGLISH", keywordResults.getLanguage());
            assertNotNull(keywordResults.getKeywords().get("bird"));
        }
    }

}
