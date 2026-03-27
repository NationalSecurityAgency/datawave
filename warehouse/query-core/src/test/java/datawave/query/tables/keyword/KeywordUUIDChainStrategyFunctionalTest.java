package datawave.query.tables.keyword;

import static datawave.query.function.JexlEvaluation.HIT_TERM_FIELD;
import static datawave.query.tables.keyword.KeywordUUIDChainStrategy.CATEGORY_PARAMETER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import datawave.microservice.query.QueryImpl;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.function.serializer.DocumentSerializer;
import datawave.query.tables.keyword.extractor.FieldedTagCloudInputExtractor;
import datawave.query.tables.keyword.extractor.TagCloudInputExtractor;
import datawave.query.tables.keyword.transform.TagCloudPartitionTransformer;
import datawave.util.keyword.TagCloudInput;
import datawave.util.keyword.TagCloudPartition;

public class KeywordUUIDChainStrategyFunctionalTest {
    private static final Key DOC_KEY = new Key("20260210_0", "test" + '\u0000' + "123.345.456");

    private QueryImpl settings;
    private Authorizations auths;
    private KeywordQueryLogic keywordQueryLogic;
    private List<Entry<Key,Value>> initialResults;
    private KeywordUUIDChainStrategy strategy;
    private List<TagCloudInputExtractor> extractors;
    private AccumuloClient accumuloClient;

    @TempDir
    private static File accumuloDir;
    private static MiniAccumuloCluster mac;

    @BeforeAll
    public static void initAccumulo() throws IOException, InterruptedException {
        mac = new MiniAccumuloCluster(new MiniAccumuloConfig(accumuloDir, "pass"));
        mac.start();
    }

    @BeforeEach
    public void setup() throws AccumuloException, TableExistsException, AccumuloSecurityException, IOException, InterruptedException {
        settings = new QueryImpl();
        auths = new Authorizations("ALL");
        keywordQueryLogic = new KeywordQueryLogic();
        initialResults = new ArrayList<>();
        strategy = new KeywordUUIDChainStrategy();
        extractors = new ArrayList<>();

        accumuloClient = mac.createAccumuloClient("root", new PasswordToken("pass"));
    }

    @AfterEach
    public void cleanup() {
        accumuloClient.close();
    }

    @AfterAll
    public static void stopAccumulo() throws IOException, InterruptedException {
        mac.stop();
    }

    @Test
    public void noInitialDataTest() {
        Iterator<Entry<Key,Value>> resultsIterator = strategy.runChainedQuery(null, settings, Set.of(auths), initialResults.iterator(), keywordQueryLogic);
        assertFalse(resultsIterator.hasNext());
    }

    @Test
    public void noInitialDataWithExtractorsTest() {
        settings.addParameter(CATEGORY_PARAMETER, "external");
        FieldedTagCloudInputExtractor extractor = new FieldedTagCloudInputExtractor();
        extractor.setCategory("external");
        extractors.add(extractor);

        // set an extractor that finds nothing to extract
        strategy.setExtractors(extractors);

        Iterator<Entry<Key,Value>> resultsIterator = strategy.runChainedQuery(accumuloClient, settings, Set.of(auths), initialResults.iterator(),
                        keywordQueryLogic);
        assertFalse(resultsIterator.hasNext());
    }

    @Test
    public void initialDataWithExtractorsAndNothingExtractedTest() {
        settings.addParameter(CATEGORY_PARAMETER, "external");
        FieldedTagCloudInputExtractor extractor = new FieldedTagCloudInputExtractor();
        extractor.setCategory("external");
        extractors.add(extractor);

        // set an extractor that finds nothing to extract
        strategy.setExtractors(extractors);

        DocumentSerializer serializer = DocumentSerialization.getDocumentSerializer(settings);
        initialResults.add(serializer.apply(Map.entry(new Key("20260210_0", "test" + '\u0000' + "123.345.456"), new Document())));

        Iterator<Entry<Key,Value>> resultsIterator = strategy.runChainedQuery(accumuloClient, settings, Set.of(auths), initialResults.iterator(),
                        keywordQueryLogic);
        assertFalse(resultsIterator.hasNext());
    }

    @Test
    public void initialDataWithExtractorsExtractedTest() {
        settings.addParameter(CATEGORY_PARAMETER, "external");
        FieldedTagCloudInputExtractor extractor = new FieldedTagCloudInputExtractor();
        extractor.setCategory("external");
        extractor.setFields(List.of("FOO"));
        extractors.add(extractor);

        // set an extractor that finds nothing to extract
        strategy.setExtractors(extractors);

        DocumentSerializer serializer = DocumentSerialization.getDocumentSerializer(settings);
        Document d = new Document();
        d.put("FOO", new Content("bar", DOC_KEY, true));
        initialResults.add(serializer.apply(Map.entry(DOC_KEY, d)));

        TagCloudPartition expectedPartition = new TagCloudPartition("external", "external", TagCloudPartition.ScoreType.HIGHER_IS_BETTER,
                        List.of(new TagCloudInput("20260210_0/test/123.345.456", "", Map.of("bar", 1.0), Map.of("type", "external"))));

        Iterator<Entry<Key,Value>> resultsIterator = strategy.runChainedQuery(accumuloClient, settings, Set.of(auths), initialResults.iterator(),
                        keywordQueryLogic);
        assertTrue(resultsIterator.hasNext());
        Entry<Key,Value> resultEntry = resultsIterator.next();
        assertNotNull(resultEntry);
        assertEquals(new Key(), resultEntry.getKey());
        TagCloudPartitionTransformer transformer = TagCloudPartitionTransformer.getInstance();
        assertTrue(transformer.canDecode(resultEntry));
        TagCloudPartition partition = transformer.decode(resultEntry);
        assertEquals(expectedPartition, partition);
    }

    @Test
    public void withSubTypeTest() {
        settings.addParameter(CATEGORY_PARAMETER, "external");
        FieldedTagCloudInputExtractor extractor = new FieldedTagCloudInputExtractor();
        extractor.setCategory("external");
        extractor.setSubType("sub1");
        extractor.setFields(List.of("FOO"));
        extractors.add(extractor);

        // set an extractor that finds nothing to extract
        strategy.setExtractors(extractors);

        DocumentSerializer serializer = DocumentSerialization.getDocumentSerializer(settings);
        Document d = new Document();
        d.put("FOO", new Content("bar", DOC_KEY, true));
        initialResults.add(serializer.apply(Map.entry(DOC_KEY, d)));

        TagCloudPartition expectedPartition = new TagCloudPartition("external.sub1", "external", TagCloudPartition.ScoreType.HIGHER_IS_BETTER,
                        List.of(new TagCloudInput("20260210_0/test/123.345.456", "", Map.of("bar", 1.0), Map.of("type", "external", "subType", "sub1"))));

        Iterator<Entry<Key,Value>> resultsIterator = strategy.runChainedQuery(accumuloClient, settings, Set.of(auths), initialResults.iterator(),
                        keywordQueryLogic);
        assertTrue(resultsIterator.hasNext());
        Entry<Key,Value> resultEntry = resultsIterator.next();
        assertNotNull(resultEntry);
        assertEquals(new Key(), resultEntry.getKey());
        TagCloudPartitionTransformer transformer = TagCloudPartitionTransformer.getInstance();
        assertTrue(transformer.canDecode(resultEntry));
        TagCloudPartition partition = transformer.decode(resultEntry);
        assertEquals(expectedPartition, partition);
    }

    @Test
    public void withHitTermTest() {
        settings.addParameter(CATEGORY_PARAMETER, "external");
        FieldedTagCloudInputExtractor extractor = new FieldedTagCloudInputExtractor();
        extractor.setCategory("external");
        extractor.setSubType("sub1");
        extractor.setFields(List.of("FOO"));
        extractors.add(extractor);

        // set an extractor that finds nothing to extract
        strategy.setExtractors(extractors);

        DocumentSerializer serializer = DocumentSerialization.getDocumentSerializer(settings);
        Document d = new Document();
        d.put("FOO", new Content("bar", DOC_KEY, true));
        d.put(HIT_TERM_FIELD, new Content("UUID:12345", DOC_KEY, true));
        initialResults.add(serializer.apply(Map.entry(DOC_KEY, d)));

        TagCloudPartition expectedPartition = new TagCloudPartition("external.sub1", "external", TagCloudPartition.ScoreType.HIGHER_IS_BETTER,
                        List.of(new TagCloudInput("UUID:12345", "", Map.of("bar", 1.0), Map.of("type", "external", "subType", "sub1"))));

        Iterator<Entry<Key,Value>> resultsIterator = strategy.runChainedQuery(accumuloClient, settings, Set.of(auths), initialResults.iterator(),
                        keywordQueryLogic);
        assertTrue(resultsIterator.hasNext());
        Entry<Key,Value> resultEntry = resultsIterator.next();
        assertNotNull(resultEntry);
        assertEquals(new Key(), resultEntry.getKey());
        TagCloudPartitionTransformer transformer = TagCloudPartitionTransformer.getInstance();
        assertTrue(transformer.canDecode(resultEntry));
        TagCloudPartition partition = transformer.decode(resultEntry);
        assertEquals(expectedPartition, partition);
    }
}
