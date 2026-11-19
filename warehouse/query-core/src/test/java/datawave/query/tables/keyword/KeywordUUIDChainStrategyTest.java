package datawave.query.tables.keyword;

import static datawave.query.tables.keyword.KeywordUUIDChainStrategy.CATEGORY_PARAMETER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import datawave.data.type.NoOpType;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.config.KeywordQueryConfiguration;
import datawave.query.function.serializer.DocumentSerializer;
import datawave.query.tables.keyword.extractor.FieldedTagCloudInputExtractor;
import datawave.query.tables.keyword.transform.TagCloudInputTransformer;
import datawave.util.keyword.KeywordResults;
import datawave.util.keyword.TagCloudInput;
import datawave.util.keyword.TagCloudPartition;

public class KeywordUUIDChainStrategyTest {

    private AccumuloClient mockAccumulo;
    private KeywordQueryLogic mockLogic;
    private KeywordQueryConfiguration mockConfig;
    // private KeywordQueryState mockState;

    private Query settings;

    @BeforeEach
    public void setup() throws Exception {
        mockAccumulo = mock(AccumuloClient.class);
        mockLogic = mock(KeywordQueryLogic.class);
        mockConfig = mock(KeywordQueryConfiguration.class);
        // mockState = mock(KeywordQueryState.class);

        lenient().doReturn("secondLogic").when(mockLogic).getLogicName();

        settings = new QueryImpl();
    }

    public Entry<Key,Value> createDocument(String shard, String dt, String uid, String language, String identifier) {
        String colf = dt + "\0" + uid;
        Key documentKey = new Key(shard, colf, "", "ALL");

        Document d = new Document();

        d.put("LANGUAGE", new TypeAttribute<>(new NoOpType(language), documentKey, true));
        d.put("HIT_TERM", new Content(identifier, documentKey, true));
        d.put("FOO", new TypeAttribute<>(new NoOpType("x"), documentKey, true));
        d.put("BAR", new TypeAttribute<>(new NoOpType("y"), documentKey, true));
        d.put("FOO2", new TypeAttribute<>(new NoOpType("xx"), documentKey, true));
        d.put("BAR2", new TypeAttribute<>(new NoOpType("yy"), documentKey, true));

        Entry<Key,Document> entry = Map.entry(documentKey, d);

        DocumentSerializer serializer = DocumentSerialization.getDocumentSerializer(DocumentSerialization.DEFAULT_RETURN_TYPE);
        return serializer.apply(entry);
    }

    public Entry<Key,Value> createKeywordResults(String shard, String dt, String uid, String language, String identifier, String view, String visibility,
                    LinkedHashMap<String,Double> results) throws IOException {
        String colf = "d";
        String colq = dt + "\0" + uid + "\0CONTENT";
        Key documentKey = new Key(shard, colf, colq);
        Value v = new Value(KeywordResults.serialize(new KeywordResults(identifier, view, language, visibility, results)));
        return Map.entry(documentKey, v);
    }

    @SuppressWarnings("RedundantOperationOnEmptyContainer")
    @Test
    public void noInputTest() {

        List<Entry<Key,Value>> input = Collections.emptyList();

        KeywordUUIDChainStrategy strategy = new KeywordUUIDChainStrategy();

        Iterator<Entry<Key,Value>> result = strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        assertFalse(result.hasNext());

        if (mockAccumulo != null && mockingDetails(mockAccumulo).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockAccumulo));
        }
        if (mockConfig != null && mockingDetails(mockConfig).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockConfig));
        }
        if (mockLogic != null && mockingDetails(mockLogic).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockLogic));
        }
    }

    @Test
    public void singleInputTest() throws Exception {
        List<Entry<Key,Value>> input = List.of(createDocument("20250412", "test", "-cvy0gj.tlf59s.-duxzua", "ENGLISH", "PAGE_ID:12345"));

        LinkedHashMap<String,Double> results = new LinkedHashMap<>();
        results.put("cat", 0.2);
        results.put("cat food", 0.3);
        results.put("dog", 0.4);

        List<Entry<Key,Value>> intermediateInput = List
                        .of(createKeywordResults("20250412", "test", "-cvy0gj.tlf59s.-duxzua", "ENGLISH", "PAGE_ID:12345", "CONTENT", "PUBLIC", results));

        KeywordUUIDChainStrategy strategy = new KeywordUUIDChainStrategy();
        ArgumentCaptor<Query> intermediateSettings = ArgumentCaptor.forClass(Query.class);

        doReturn(mockConfig).when(mockLogic).initialize(eq(mockAccumulo), intermediateSettings.capture(), eq(null));

        doReturn(intermediateInput.iterator()).when(mockLogic).iterator();

        Iterator<Entry<Key,Value>> result = strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        verify(mockLogic, times(1)).initialize(eq(mockAccumulo), intermediateSettings.capture(), eq(null));
        verify(mockLogic, times(1)).iterator();
        verify(mockLogic, times(1)).setupQuery(eq(mockConfig));

        assertEquals("DOCUMENT:20250412/test/-cvy0gj.tlf59s.-duxzua!PAGE_ID:12345%LANGUAGE:ENGLISH", intermediateSettings.getValue().getQuery());

        assertTrue(result.hasNext());
        Entry<Key,Value> next = result.next();

        assertEquals("20250412 d:test%00;-cvy0gj.tlf59s.-duxzua%00;CONTENT [] 9223372036854775807 false", next.getKey().toString());

        KeywordResults keywordResults = KeywordResults.deserialize(next.getValue().get());
        assertEquals("PAGE_ID:12345", keywordResults.getSource());
        assertEquals("CONTENT", keywordResults.getView());
        assertEquals("ENGLISH", keywordResults.getLanguage());
        assertEquals("PUBLIC", keywordResults.getVisibility());

        assertNotNull(keywordResults.getKeywords().get("cat"));

        assertFalse(result.hasNext());

        if (mockAccumulo != null && mockingDetails(mockAccumulo).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockAccumulo));
        }
        if (mockConfig != null && mockingDetails(mockConfig).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockConfig));
        }
        if (mockLogic != null && mockingDetails(mockLogic).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockLogic));
        }
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
                        createKeywordResults("20250412", "test", "-cvy0gj.tlf59s.-duxzua", "ENGLISH", "PAGE_ID:12345", "CONTENT", "PUBLIC", resultsOne),
                        createKeywordResults("20250412", "test", "-cvy0gj.tlf59s.-duxzub", "ENGLISH", "PAGE_ID:12346", "INDEXABLE_TEXT", "PUBLIC", resultsTwo)

        );

        KeywordUUIDChainStrategy strategy = new KeywordUUIDChainStrategy();
        ArgumentCaptor<Query> intermediateSettings = ArgumentCaptor.forClass(Query.class);

        doReturn(mockConfig).when(mockLogic).initialize(eq(mockAccumulo), intermediateSettings.capture(), eq(null));

        doReturn(intermediateInput.iterator()).when(mockLogic).iterator();

        Iterator<Entry<Key,Value>> result = strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        verify(mockLogic, times(1)).initialize(eq(mockAccumulo), intermediateSettings.capture(), eq(null));
        verify(mockLogic, times(1)).iterator();
        verify(mockLogic, times(1)).setupQuery(eq(mockConfig));

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
            assertEquals("PUBLIC", keywordResults.getVisibility());

            assertNotNull(keywordResults.getKeywords().get("cat"));
        }

        assertTrue(result.hasNext());
        assertNotNull(next = result.next());

        {
            KeywordResults keywordResults = KeywordResults.deserialize(next.getValue().get());
            assertEquals("PAGE_ID:12346", keywordResults.getSource());
            assertEquals("INDEXABLE_TEXT", keywordResults.getView());
            assertEquals("ENGLISH", keywordResults.getLanguage());
            assertEquals("PUBLIC", keywordResults.getVisibility());
            assertNotNull(keywordResults.getKeywords().get("bird"));
        }

        if (mockAccumulo != null && mockingDetails(mockAccumulo).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockAccumulo));
        }
        if (mockConfig != null && mockingDetails(mockConfig).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockConfig));
        }
        if (mockLogic != null && mockingDetails(mockLogic).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockLogic));
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
                        createKeywordResults("20250412", "test", "-cvy0gj.tlf59s.-duxzua", "ENGLISH", "PAGE_ID:12345", "CONTENT", "PUBLIC", resultsOne),
                        createKeywordResults("20250412", "test", "-cvy0gj.tlf59s.-duxzub", "ENGLISH", "PAGE_ID:12346", "INDEXABLE_TEXT", "PUBLIC", resultsTwo)

        );

        KeywordUUIDChainStrategy strategy = new KeywordUUIDChainStrategy();
        ArgumentCaptor<Query> intermediateSettings = ArgumentCaptor.forClass(Query.class);

        doReturn(mockConfig).when(mockLogic).initialize(eq(mockAccumulo), intermediateSettings.capture(), eq(null));

        doReturn(intermediateInput.iterator()).when(mockLogic).iterator();

        strategy.setBatchSize(1);
        Iterator<Entry<Key,Value>> result = strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        verify(mockLogic, times(1)).initialize(eq(mockAccumulo), intermediateSettings.capture(), eq(null));
        verify(mockLogic, times(1)).iterator();
        verify(mockLogic, times(1)).setupQuery(eq(mockConfig));

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

        if (mockAccumulo != null && mockingDetails(mockAccumulo).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockAccumulo));
        }
        if (mockConfig != null && mockingDetails(mockConfig).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockConfig));
        }
        if (mockLogic != null && mockingDetails(mockLogic).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockLogic));
        }
    }

    @Test
    public void singleFieldedTest() throws Exception {
        settings.addParameter(CATEGORY_PARAMETER, "external,keyword");

        List<Entry<Key,Value>> input = List.of(createDocument("20250412", "test", "-cvy0gj.tlf59s.-duxzua", "ENGLISH", "PAGE_ID:12345"));

        LinkedHashMap<String,Double> results = new LinkedHashMap<>();
        results.put("cat", 0.2);
        results.put("cat food", 0.3);
        results.put("dog", 0.4);

        List<Entry<Key,Value>> intermediateInput = List
                        .of(createKeywordResults("20250412", "test", "-cvy0gj.tlf59s.-duxzua", "ENGLISH", "PAGE_ID:12345", "CONTENT", "PUBLIC", results));

        KeywordUUIDChainStrategy strategy = new KeywordUUIDChainStrategy();
        FieldedTagCloudInputExtractor fieldedExtractor = new FieldedTagCloudInputExtractor();
        fieldedExtractor.setFields(List.of("FOO", "BAR"));
        fieldedExtractor.setCategory("external");
        strategy.setExtractors(List.of(fieldedExtractor));

        ArgumentCaptor<Query> intermediateSettings = ArgumentCaptor.forClass(Query.class);

        doReturn(mockConfig).when(mockLogic).initialize(eq(mockAccumulo), intermediateSettings.capture(), eq(null));

        doReturn(intermediateInput.iterator()).when(mockLogic).iterator();

        ArgumentCaptor<List<Entry<Key,Value>>> externalData = ArgumentCaptor.forClass((Class) List.class);
        ArgumentCaptor<Set<TagCloudInputTransformer<?>>> transformers = ArgumentCaptor.forClass((Class) Set.class);

        Iterator<Entry<Key,Value>> result = strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        verify(mockLogic, times(1)).initialize(eq(mockAccumulo), intermediateSettings.capture(), eq(null));
        verify(mockLogic, times(1)).iterator();
        verify(mockLogic, times(1)).setupQuery(eq(mockConfig));
        verify(mockLogic, times(1)).setExternalData(externalData.capture(), transformers.capture());

        assertEquals("DOCUMENT:20250412/test/-cvy0gj.tlf59s.-duxzua!PAGE_ID:12345%LANGUAGE:ENGLISH", intermediateSettings.getValue().getQuery());

        assertTrue(result.hasNext());
        Entry<Key,Value> next = result.next();

        assertEquals("20250412 d:test%00;-cvy0gj.tlf59s.-duxzua%00;CONTENT [] 9223372036854775807 false", next.getKey().toString());

        KeywordResults keywordResults = KeywordResults.deserialize(next.getValue().get());
        assertEquals("PAGE_ID:12345", keywordResults.getSource());
        assertEquals("CONTENT", keywordResults.getView());
        assertEquals("ENGLISH", keywordResults.getLanguage());
        assertEquals("PUBLIC", keywordResults.getVisibility());

        assertNotNull(keywordResults.getKeywords().get("cat"));

        assertFalse(result.hasNext());

        Set<TagCloudInputTransformer<?>> capturedTransformers = transformers.getValue();
        assertNotNull(capturedTransformers);
        assertEquals(1, capturedTransformers.size());
        List<Entry<Key,Value>> capturedExternalData = externalData.getValue();
        assertNotNull(capturedExternalData);
        assertEquals(1, capturedExternalData.size());
        TagCloudInputTransformer theTransformer = capturedTransformers.iterator().next();
        TagCloudPartition partition = theTransformer.decode(capturedExternalData.get(0));
        assertNotNull(partition);
        assertEquals("external", partition.getPartition());
        assertEquals("external", partition.getLabel());
        assertEquals(TagCloudPartition.ScoreType.HIGHER_IS_BETTER, partition.getScoreType());
        assertEquals(1, partition.getInputs().size());
        TagCloudInput tagCloudInput = partition.getInputs().get(0);
        assertNotNull(tagCloudInput);
        assertEquals("PAGE_ID:12345", tagCloudInput.getSource());
        assertEquals("ALL", tagCloudInput.getVisibility());
        assertEquals(2, tagCloudInput.getEntities().size());
        Map<String,Double> expectedEntites = Map.of("x", 1d, "y", 1d);
        assertEquals(expectedEntites, tagCloudInput.getEntities());

        if (mockAccumulo != null && mockingDetails(mockAccumulo).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockAccumulo));
        }
        if (mockConfig != null && mockingDetails(mockConfig).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockConfig));
        }
        if (mockLogic != null && mockingDetails(mockLogic).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockLogic));
        }
    }

    @Test
    public void multipleNamedExtractorsTest() throws Exception {
        settings.addParameter(CATEGORY_PARAMETER, "external");

        List<Entry<Key,Value>> input = List.of(createDocument("20250412", "test", "-cvy0gj.tlf59s.-duxzua", "ENGLISH", "PAGE_ID:12345"));

        KeywordUUIDChainStrategy strategy = new KeywordUUIDChainStrategy();
        FieldedTagCloudInputExtractor fieldedExtractor = new FieldedTagCloudInputExtractor();
        fieldedExtractor.setFields(List.of("FOO", "BAR"));
        fieldedExtractor.setCategory("external");

        FieldedTagCloudInputExtractor fieldedExtractor2 = new FieldedTagCloudInputExtractor();
        fieldedExtractor2.setFields(List.of("FOO2", "BAR2"));
        fieldedExtractor2.setCategory("external");

        strategy.setExtractors(List.of(fieldedExtractor, fieldedExtractor2));
        ArgumentCaptor<List<Entry<Key,Value>>> externalData = ArgumentCaptor.forClass((Class) List.class);
        ArgumentCaptor<Set<TagCloudInputTransformer<?>>> transformers = ArgumentCaptor.forClass((Class) Set.class);

        ArgumentCaptor<Query> intermediateSettings = ArgumentCaptor.forClass(Query.class);
        doReturn(mockConfig).when(mockLogic).initialize(eq(mockAccumulo), intermediateSettings.capture(), eq(null));

        // this response is meaningless here
        List<Entry<Key,Value>> responseData = new ArrayList<>();
        doReturn(responseData.iterator()).when(mockLogic).iterator();

        // because the internals are all mocked the output isn't valuable
        strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        verify(mockLogic, times(1)).initialize(eq(mockAccumulo), intermediateSettings.capture(), eq(null));
        verify(mockLogic, times(1)).iterator();
        verify(mockLogic, times(1)).setExternalData(externalData.capture(), transformers.capture());
        verify(mockLogic, times(1)).setupQuery(eq(mockConfig));

        // look at the captures to verify the right things were passed in
        Set<TagCloudInputTransformer<?>> capturedTransformers = transformers.getValue();
        assertNotNull(capturedTransformers);
        // both extractors use the same transformer
        assertEquals(1, capturedTransformers.size());
        List<Entry<Key,Value>> capturedExternalData = externalData.getValue();
        assertNotNull(capturedExternalData);
        assertEquals(2, capturedExternalData.size());
        TagCloudInputTransformer theTransformer = capturedTransformers.iterator().next();
        // first extractors data
        TagCloudPartition partition = theTransformer.decode(capturedExternalData.get(0));
        assertNotNull(partition);
        assertEquals("external", partition.getPartition());
        assertEquals("external", partition.getLabel());
        assertEquals(TagCloudPartition.ScoreType.HIGHER_IS_BETTER, partition.getScoreType());
        assertEquals(1, partition.getInputs().size());
        TagCloudInput tagCloudInput = partition.getInputs().get(0);
        assertNotNull(tagCloudInput);
        assertEquals("PAGE_ID:12345", tagCloudInput.getSource());
        assertEquals("ALL", tagCloudInput.getVisibility());
        assertEquals(2, tagCloudInput.getEntities().size());
        Map<String,Double> expectedEntites = Map.of("x", 1d, "y", 1d);
        assertEquals(expectedEntites, tagCloudInput.getEntities());
        // second extractors data
        partition = theTransformer.decode(capturedExternalData.get(1));
        assertNotNull(partition);
        assertEquals("external", partition.getPartition());
        assertEquals("external", partition.getLabel());
        assertEquals(TagCloudPartition.ScoreType.HIGHER_IS_BETTER, partition.getScoreType());
        assertEquals(1, partition.getInputs().size());
        tagCloudInput = partition.getInputs().get(0);
        assertNotNull(tagCloudInput);
        assertEquals("PAGE_ID:12345", tagCloudInput.getSource());
        assertEquals("ALL", tagCloudInput.getVisibility());
        assertEquals(2, tagCloudInput.getEntities().size());
        expectedEntites = Map.of("xx", 1d, "yy", 1d);
        assertEquals(expectedEntites, tagCloudInput.getEntities());

        if (mockAccumulo != null && mockingDetails(mockAccumulo).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockAccumulo));
        }
        if (mockConfig != null && mockingDetails(mockConfig).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockConfig));
        }
        if (mockLogic != null && mockingDetails(mockLogic).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockLogic));
        }
    }

    @Test
    public void everythingMissesTest() throws Exception {
        settings.addParameter(CATEGORY_PARAMETER, "external");

        List<Entry<Key,Value>> input = List.of(createDocument("20250412", "test", "-cvy0gj.tlf59s.-duxzua", "ENGLISH", "PAGE_ID:12345"));

        KeywordUUIDChainStrategy strategy = new KeywordUUIDChainStrategy();
        FieldedTagCloudInputExtractor fieldedExtractor = new FieldedTagCloudInputExtractor();
        fieldedExtractor.setFields(List.of("FOO_NOPE", "BAR_NOPE"));
        fieldedExtractor.setCategory("external");

        strategy.setExtractors(List.of(fieldedExtractor));

        // the important part here is that there is no external data set on the mockLogic, if there is nothing extracted nothing, should be passed

        // because the internals are all mocked the output isn't valuable
        strategy.runChainedQuery(mockAccumulo, settings, null, input.iterator(), mockLogic);

        if (mockAccumulo != null && mockingDetails(mockAccumulo).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockAccumulo));
        }
        if (mockConfig != null && mockingDetails(mockConfig).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockConfig));
        }
        if (mockLogic != null && mockingDetails(mockLogic).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(mockLogic));
        }
    }
}
