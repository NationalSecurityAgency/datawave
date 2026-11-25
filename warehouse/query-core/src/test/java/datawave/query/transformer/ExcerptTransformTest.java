package datawave.query.transformer;

import static datawave.query.iterator.logic.TermFrequencyExcerptIterator.Configuration.END_OFFSET;
import static datawave.query.iterator.logic.TermFrequencyExcerptIterator.Configuration.FIELD_NAME;
import static datawave.query.iterator.logic.TermFrequencyExcerptIterator.Configuration.START_OFFSET;
import static datawave.query.transformer.ExcerptTransform.HIT_EXCERPT;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import datawave.accumulo.inmemory.util.SortedMapIterator;
import datawave.ingest.protobuf.TermWeight;
import datawave.query.Constants;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.attributes.ExcerptFields;
import datawave.query.function.JexlEvaluation;
import datawave.query.iterator.logic.TermFrequencyExcerptIterator;
import datawave.query.postprocessing.tf.PhraseIndexes;

@ExtendWith(MockitoExtension.class)
public class ExcerptTransformTest {

    private final Map<List<?>,Integer> expectedCallCounts = new HashMap<>();
    private final List<Executable> interactionChecks = new ArrayList<>();
    private final Deque<Boolean> iteratorHasTopResults = new ArrayDeque<>();
    private final Deque<Key> iteratorTopKeyResults = new ArrayDeque<>();
    private final List<Map<String,String>> expectedIteratorOptions = new ArrayList<>();
    private final List<Map<String,String>> actualIteratorOptions = new ArrayList<>();

    @Mock
    private TermFrequencyExcerptIterator iterator;

    @Mock
    private IteratorEnvironment env;

    @Mock
    private SortedKeyValueIterator<Key,Value> source;

    private PhraseIndexes phraseIndexes;
    private ExcerptFields excerptFields;

    private Document document;
    private ExcerptTransform excerptTransform;

    private static final String EVENT_ID = "shard\u0000dt\u0000uid";

    @BeforeEach
    public void setUp() {
        phraseIndexes = new PhraseIndexes();
        excerptFields = new ExcerptFields();
    }

    @AfterEach
    public void tearDown() {
        document = null;
        excerptTransform = null;
    }

    /**
     * Verify that a null entry is returned for a null input.
     */
    @Test
    public void testNullDocumentEntry() {
        initTransform();

        assertNull(excerptTransform.apply(null));
        assertAll(interactionChecks);

        if (env != null && mockingDetails(env).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(env));
        }
        if (iterator != null && mockingDetails(iterator).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(iterator));
        }
        if (source != null && mockingDetails(source).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(source));
        }
    }

    /**
     * Verify that excerpts are not added for documents that are not marked as to-keep.
     */
    @Test
    public void testNonToKeepDocumentEntry() {
        givenDocument(new Document(new Key(), false));
        initTransform();

        applyTransform();

        assertFalse(document.containsKey(HIT_EXCERPT));
        assertAll(interactionChecks);

        if (env != null && mockingDetails(env).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(env));
        }
        if (iterator != null && mockingDetails(iterator).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(iterator));
        }
        if (source != null && mockingDetails(source).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(source));
        }
    }

    /**
     * Verify that excerpts are not added for documents that don't have PHRASE_INDEXES_ATTRIBUTE.
     */
    @Test
    public void testNullPhraseIndexes() {
        givenDocument(new Document(new Key(), true));
        initTransform();

        applyTransform();

        assertFalse(document.containsKey(HIT_EXCERPT));
        assertAll(interactionChecks);

        if (env != null && mockingDetails(env).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(env));
        }
        if (iterator != null && mockingDetails(iterator).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(iterator));
        }
        if (source != null && mockingDetails(source).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(source));
        }
    }

    /**
     * Verify that excerpts are retrieved with the expected inputs.
     */
    @Test
    public void testExcerpts() throws IOException {
        // Setup our excerpts to match 2 words on either side of the BODY
        givenExcerptField("BODY", 2);

        // setup a matching phrase at 10-14
        // end offset is inclusive
        givenPhraseIndex("BODY", 10, 14);

        // setup a matching term for BODY:word
        givenDocumentWithHitTerm("BODY", "word");
        givenMatchingTermFrequencies("BODY", new int[][] {{24, 24}}, "word");
        // end offset is inclusive
        givenMatchingPhrase("BODY", 22, 26, "and the [word] from bird", List.of("word"));

        // also setup a phrase to match on either side of the matching phrase
        // end offset is inclusive
        givenMatchingPhrase("BODY", 8, 16, "the quick brown fox jumped over the lazy dog", List.of("word"));

        initTransform();

        applyTransform();

        Attributes arg = (Attributes) document.get(HIT_EXCERPT);
        assertNotNull(arg);
        assertEquals(2, arg.size());
        Set<String> excerpts = arg.getAttributes().stream().map(a -> a.getData().toString()).collect(Collectors.toSet());
        // both excerpts should be returned
        assertTrue(excerpts.contains("and the [word] from bird"));
        assertTrue(excerpts.contains("the quick brown fox jumped over the lazy dog"));
        assertAll(interactionChecks);

        if (env != null && mockingDetails(env).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(env));
        }
        if (iterator != null && mockingDetails(iterator).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(iterator));
        }
        if (source != null && mockingDetails(source).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(source));
        }
    }

    /**
     * Verify that excerpts are retrieved with the expected inputs.
     */
    @Test
    public void testExcerptOverlapped() throws IOException {
        // Setup our excerpts to match 2 words on either side of the BODY
        givenExcerptField("BODY", 2);

        // setup a matching phrase at 10-14
        // end offset is inclusive
        givenPhraseIndex("BODY", 10, 14);

        // setup a matching term for BODY:quick brown overlapping the phrase match
        givenDocumentWithHitTerm("BODY", "quick brown");
        givenMatchingTermFrequencies("BODY", new int[][] {{1, 2}, {2, 3}, {9, 10}, {20, 21}}, "quick brown");
        // note that the start is relative to index 9 (i.e. 9-2=7) because the overlapping term starts at 9
        // end offset is inclusive
        givenMatchingPhrase("BODY", 7, 16, "and the [quick] [brown] fox jumped over the lazy dog", List.of("quick", "brown"));

        initTransform();

        applyTransform();

        Attributes arg = (Attributes) document.get(HIT_EXCERPT);
        assertNotNull(arg);
        assertEquals(1, arg.size());
        String excerpt = arg.getAttributes().iterator().next().getData().toString();
        // only one excerpt should return
        assertEquals("and the [quick] [brown] fox jumped over the lazy dog", excerpt);
        assertAll(interactionChecks);

        if (env != null && mockingDetails(env).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(env));
        }
        if (iterator != null && mockingDetails(iterator).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(iterator));
        }
        if (source != null && mockingDetails(source).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(source));
        }
    }

    /**
     * Verify that excerpts are retrieved with the expected inputs.
     */
    @Test
    public void testExcerptOverlappedAndPhraseOverlapped() throws IOException {
        // Setup our excerpts to match 2 words on either side of the BODY
        givenExcerptField("BODY", 2);

        // setup a matching phrase at 10-14, ...
        // end offset is inclusive
        givenPhraseIndex("BODY", 10, 14);
        givenPhraseIndex("BODY", 25, 26);
        givenPhraseIndex("BODY", 4, 5);

        // setup a matching term for BODY:quick brown overlapping the phrase match
        givenDocumentWithHitTerm("BODY", "quick brown");
        givenMatchingTermFrequencies("BODY", new int[][] {{1, 2}, {2, 3}, {9, 10}, {20, 21}}, "quick brown");
        // end offset is inclusive
        givenMatchingPhrase("BODY", 23, 28, "Jack and Jill jumped over the", List.of("quick", "brown"));
        // note that the start is relative to overlapping term index 9 (i.e. 9-2=7) because the overlapping term starts at 9
        // AND then we combined the phrase from 2 to 7 with the one from 7 to 16
        givenMatchingPhrase("BODY", 2, 16, "the [brown] chicken layed an egg and the [quick] [brown] fox jumped over the lazy dog", List.of("quick", "brown"));

        initTransform();

        applyTransform();

        Attributes arg = (Attributes) document.get(HIT_EXCERPT);
        assertNotNull(arg);
        assertEquals(2, arg.size());
        Set<String> excerpts = arg.getAttributes().stream().map(a -> a.getData().toString()).collect(Collectors.toSet());
        // all excerpts should be returned
        assertTrue(excerpts.contains("Jack and Jill jumped over the"));
        assertTrue(excerpts.contains("the [brown] chicken layed an egg and the [quick] [brown] fox jumped over the lazy dog"));
        assertAll(interactionChecks);

        if (env != null && mockingDetails(env).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(env));
        }
        if (iterator != null && mockingDetails(iterator).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(iterator));
        }
        if (source != null && mockingDetails(source).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(source));
        }
    }

    /**
     * Verify that a grouped phrase index can be excerpted when the base field is requested.
     */
    @Test
    public void testGroupedPhraseIndexUsesBaseExcerptField() throws IOException {
        givenExcerptField("BODY", 2);
        givenPhraseIndex("BODY.hash1", 10, 14);

        givenDocumentWithPhraseIndexes();
        givenMatchingPhrase("BODY.hash1", 8, 16, "the quick brown fox jumped over the lazy dog", Collections.emptyList());

        initTransform();

        applyTransform();

        Attributes arg = (Attributes) document.get(HIT_EXCERPT);
        assertNotNull(arg);
        assertEquals(1, arg.size());
        String excerpt = arg.getAttributes().iterator().next().getData().toString();
        assertEquals("the quick brown fox jumped over the lazy dog", excerpt);
        assertAll(interactionChecks);

        if (env != null && mockingDetails(env).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(env));
        }
        if (iterator != null && mockingDetails(iterator).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(iterator));
        }
        if (source != null && mockingDetails(source).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(source));
        }
    }

    /**
     * Verify that a grouped hit term drives excerpt generation using the concrete grouped field while still using base field excerpt options.
     */
    @Test
    public void testGroupedHitTermUsesGroupedFieldForExcerpt() throws IOException {
        givenExcerptField("BODY", 2);

        givenDocumentWithHitTerm("BODY.hash1", "word");
        givenMatchingTermFrequencies("BODY.hash1", new int[][] {{24, 24}}, "word");
        givenMatchingPhrase("BODY.hash1", 22, 26, "and the [word] from bird", List.of("word"));

        initTransform();

        applyTransform();

        Attributes arg = (Attributes) document.get(HIT_EXCERPT);
        assertNotNull(arg);
        assertEquals(1, arg.size());
        String excerpt = arg.getAttributes().iterator().next().getData().toString();
        assertEquals("and the [word] from bird", excerpt);
        assertAll(interactionChecks);

        if (env != null && mockingDetails(env).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(env));
        }
        if (iterator != null && mockingDetails(iterator).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(iterator));
        }
        if (source != null && mockingDetails(source).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(source));
        }
    }

    /**
     * Verify that hit terms collected for one document do not leak into excerpts generated for a later document.
     */
    @Test
    @SuppressWarnings("rawtypes")
    public void testHitTermsClearedBetweenDocuments() throws IOException {
        givenExcerptField("BODY", 2);

        Key metadata = new Key("shard", "dt\u0000uid");
        Document firstDocument = new Document(metadata, true);
        Attribute firstHitTerms = new Attributes(Collections.singletonList(new Content("BODY:first", metadata, true)), true);
        firstDocument.put(JexlEvaluation.HIT_TERM_FIELD, firstHitTerms);
        givenMatchingTermFrequencies("BODY", new int[][] {{2, 2}}, "first");
        givenMatchingPhrase("BODY", 0, 4, "the [first] document excerpt", List.of("first"));

        PhraseIndexes secondPhraseIndexes = new PhraseIndexes();
        secondPhraseIndexes.addIndexTriplet("BODY", EVENT_ID, 4, 4);
        Document secondDocument = new Document(metadata, true);
        Attribute phraseIndexAttribute = new Content(secondPhraseIndexes.toString(), metadata, false);
        secondDocument.put(ExcerptTransform.PHRASE_INDEXES_ATTRIBUTE, phraseIndexAttribute);
        givenMatchingPhrase("BODY", 2, 6, "the second document excerpt", Collections.emptyList());

        initTransform();

        excerptTransform.apply(Map.entry(new Key(), firstDocument));
        excerptTransform.apply(Map.entry(new Key(), secondDocument));
        assertEquals("the [first] document excerpt", ((Attributes) firstDocument.get(HIT_EXCERPT)).getAttributes().iterator().next().getData().toString());
        assertEquals("the second document excerpt", ((Attributes) secondDocument.get(HIT_EXCERPT)).getAttributes().iterator().next().getData().toString());
        assertAll(interactionChecks);

        if (env != null && mockingDetails(env).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(env));
        }
        if (iterator != null && mockingDetails(iterator).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(iterator));
        }
        if (source != null && mockingDetails(source).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(source));
        }
    }

    /**
     * Verify that when a start index is less than the specified excerpt offset, that the excerpt start defaults to 0.
     */
    @Test
    public void testOffsetGreaterThanStartIndex() throws IOException {
        givenExcerptField("CONTENT", 2);
        // end offset is inclusive
        givenPhraseIndex("CONTENT", 1, 5);

        givenDocumentWithPhraseIndexes();
        // end offset is inclusive
        givenMatchingPhrase("CONTENT", 0, 7, "the quick brown fox jumped over the lazy dog", Collections.emptyList());

        initTransform();

        applyTransform();

        Attributes arg = (Attributes) document.get(HIT_EXCERPT);
        assertNotNull(arg);
        assertEquals(1, arg.size());
        Attribute<?>[] attributes = arg.getAttributes().toArray(new Attribute[0]);
        assertEquals("the quick brown fox jumped over the lazy dog", ((Content) attributes[0]).getContent());
        assertAll(interactionChecks);

        if (env != null && mockingDetails(env).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(env));
        }
        if (iterator != null && mockingDetails(iterator).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(iterator));
        }
        if (source != null && mockingDetails(source).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(source));
        }
    }

    @Test
    public void testContextTFAgainstFiBodyHitTerms() throws IOException {
        givenExcerptField("BODY", 2);

        // setup a document with hit terms only, no phrase hits
        this.document = new Document(new Key("20260715_0", "sample" + '\u0000' + "123.234.345"), true);
        Attributes attributes = new Attributes(true);
        attributes.add(new Content("BODY:fox", new Key("20260715_0", "sample" + '\u0000' + "123.234.345"), true));
        document.put(JexlEvaluation.HIT_TERM_FIELD, attributes);

        // term frequencies will occur in the context BODY.1234, not BODY directly
        givenMatchingTermFrequencies("BODY.1234", new int[][] {{24, 24}}, "fox");

        // verify what actually gets seeked is the BODY.1234
        givenMatchingPhrase("BODY.1234", 22, 26, "and the [fox] from bird", List.of("fox"));

        initTransform();
        applyTransform();

        // verify the document got the EXCERPT
        assertNotNull(document.get(HIT_EXCERPT));
        Attribute<?> a = document.get(HIT_EXCERPT);
        assertTrue(a instanceof Attributes);
        Attributes allAttributes = (Attributes) a;
        assertEquals(1, allAttributes.size());
        Content c = (Content) allAttributes.getAttributes().iterator().next();
        assertEquals("and the [fox] from bird", c.getContent());
        assertAll(interactionChecks);

        if (env != null && mockingDetails(env).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(env));
        }
        if (iterator != null && mockingDetails(iterator).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(iterator));
        }
        if (source != null && mockingDetails(source).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(source));
        }
    }

    // the behavior mocked in testContextTFAgainstFiBodyHitTerms() leaves too much room for accidental decoupling to reality. Added this test to verify the real
    // interactions
    @Test
    public void testContextTFAgainstFiBodyHitTermsUnmocked() throws IOException {
        givenExcerptField("BODY", 2);

        // setup a document with hit terms only, no phrase hits
        this.document = new Document(new Key("20260715_0", "sample" + '\u0000' + "123.234.345"), true);
        Attributes attributes = new Attributes(true);
        attributes.add(new Content("BODY:fox", new Key("20260715_0", "sample" + '\u0000' + "123.234.345"), true));
        document.put(JexlEvaluation.HIT_TERM_FIELD, attributes);

        TermFrequencyExcerptIterator termFrequencyIterator = new TermFrequencyExcerptIterator();
        SortedMap<Key,Value> data = new TreeMap<>();

        // add the tf value to the data
        data.put(new Key("20260715_0", "tf", "sample" + '\u0000' + "123.234.345" + '\u0000' + "and" + '\u0000' + "BODY.1234"),
                        getTfValue(new int[][] {{22, 22}}, null));
        data.put(new Key("20260715_0", "tf", "sample" + '\u0000' + "123.234.345" + '\u0000' + "the" + '\u0000' + "BODY.1234"),
                        getTfValue(new int[][] {{23, 23}}, null));
        data.put(new Key("20260715_0", "tf", "sample" + '\u0000' + "123.234.345" + '\u0000' + "fox" + '\u0000' + "BODY.1234"),
                        getTfValue(new int[][] {{24, 24}}, null));
        data.put(new Key("20260715_0", "tf", "sample" + '\u0000' + "123.234.345" + '\u0000' + "from" + '\u0000' + "BODY.1234"),
                        getTfValue(new int[][] {{25, 25}}, null));
        data.put(new Key("20260715_0", "tf", "sample" + '\u0000' + "123.234.345" + '\u0000' + "bird" + '\u0000' + "BODY.1234"),
                        getTfValue(new int[][] {{26, 26}}, null));

        SortedKeyValueIterator<Key,Value> source = new SortedMapIterator(data);

        excerptTransform = new ExcerptTransform(excerptFields, env, source, termFrequencyIterator);
        excerptTransform.apply(getDocumentEntry());

        // verify the document got the EXCERPT
        assertNotNull(document.get(HIT_EXCERPT));
        Attribute<?> a = document.get(HIT_EXCERPT);
        assertTrue(a instanceof Attributes);
        Attributes allAttributes = (Attributes) a;
        assertEquals(1, allAttributes.size());
        Content c = (Content) allAttributes.getAttributes().iterator().next();
        assertEquals("and the [fox] from bird", c.getContent());
    }

    /**
     * Verify that a phrase index with the end before the start does not mess us up
     */
    @Test
    public void testEmptyPhraseIndexes() throws IOException {
        // Setup our excerpts to match 2 words on either side of the BODY
        givenExcerptField("BODY", 2);

        // setup a matching term for BODY:word with an empty phrase index attribute
        givenDocumentWithHitTerm("BODY", "word");
        givenMatchingTermFrequencies("BODY", new int[][] {{24, 24}}, "word");
        // end offset is inclusive
        givenMatchingPhrase("BODY", 22, 26, "and the [word] from bird", List.of("word"));

        initTransform();

        applyTransform();

        Attributes arg = (Attributes) document.get(HIT_EXCERPT);
        assertNotNull(arg);
        assertEquals(1, arg.size());
        Set<String> excerpts = arg.getAttributes().stream().map(a -> a.getData().toString()).collect(Collectors.toSet());
        // both excerpts should be returned
        assertTrue(excerpts.contains("and the [word] from bird"));
        assertAll(interactionChecks);

        if (env != null && mockingDetails(env).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(env));
        }
        if (iterator != null && mockingDetails(iterator).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(iterator));
        }
        if (source != null && mockingDetails(source).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(source));
        }
    }

    private void initTransform() {
        excerptTransform = new ExcerptTransform(excerptFields, env, source, iterator);
    }

    private void applyTransform() {
        excerptTransform.apply(getDocumentEntry());
    }

    private void givenDocumentWithPhraseIndexes() {
        Document document = new Document(new Key("Row", "cf", "cq"), true);
        document.put(ExcerptTransform.PHRASE_INDEXES_ATTRIBUTE, new Content(phraseIndexes.toString(), document.getMetadata(), false));
        givenDocument(document);
    }

    private void givenDocumentWithHitTerm(String field, String value) {
        Key metadata = new Key("shard", "dt\u0000uid");
        Document document = new Document(metadata, true);
        document.put(ExcerptTransform.PHRASE_INDEXES_ATTRIBUTE, new Content(phraseIndexes.toString(), metadata, false));
        document.put(JexlEvaluation.HIT_TERM_FIELD, new Attributes(Collections.singletonList(new Content(field + ":" + value, metadata, true)), true));
        givenDocument(document);
    }

    private void givenDocument(Document document) {
        this.document = document;
    }

    private Map.Entry<Key,Document> getDocumentEntry() {
        return Map.entry(new Key(), document);
    }

    private void givenExcerptField(String field, int offset) {
        excerptFields.put(field, offset);
    }

    private void givenPhraseIndex(String field, int start, int end) {
        // end offset is inclusive
        phraseIndexes.addIndexTriplet(field, EVENT_ID, start, end);
    }

    private void givenMatchingPhrase(String field, int start, int end, String phrase, List<String> hitTerms) throws IOException {
        Map<String,String> options = getOptions(field, start, end);

        if (phrase != null) {
            iteratorHasTopResults.add(true);
            Key key = new Key(new Text("row"), new Text("cf"),
                            new Text(field + Constants.NULL + "XXXNOTSCOREDXXX" + Constants.NULL + phrase + Constants.NULL + "XXXNOTSCOREDXXX"));
            iteratorTopKeyResults.add(key);
            if (iteratorTopKeyResults.size() == 1) {
                doAnswer(invocation -> iteratorTopKeyResults.removeFirst()).when(iterator).getTopKey();
            }
            List<?> expectedCall6 = Arrays.asList(iterator, "getTopKey");
            expectedCallCounts.merge(expectedCall6, 1, Integer::sum);
            interactionChecks.add(() -> verify(iterator, times(expectedCallCounts.get(expectedCall6))).getTopKey());
        } else {
            iteratorHasTopResults.add(false);
        }
        if (iteratorHasTopResults.size() == 1) {
            doAnswer(invocation -> iteratorHasTopResults.removeFirst()).when(iterator).hasTop();
        }
        List<?> expectedCall5 = Arrays.asList(iterator, "hasTop");
        expectedCallCounts.merge(expectedCall5, 1, Integer::sum);
        interactionChecks.add(() -> verify(iterator, times(expectedCallCounts.get(expectedCall5))).hasTop());
        List<?> expectedCall8 = Arrays.asList(iterator, "init", source, options, env);
        expectedCallCounts.merge(expectedCall8, 1, Integer::sum);
        expectedIteratorOptions.add(options);
        // The transform reuses and mutates the options map, so preserve its contents at each call.
        if (expectedIteratorOptions.size() == 1) {
            doAnswer(invocation -> {
                actualIteratorOptions.add(new HashMap<>(invocation.<Map<String,String>> getArgument(1)));
                return null;
            }).when(iterator).init(eq(source), anyMap(), eq(env));
        }
        interactionChecks.add(() -> {
            verify(iterator, times(expectedIteratorOptions.size())).init(eq(source), anyMap(), eq(env));
            assertEquals(Collections.frequency(expectedIteratorOptions, options), Collections.frequency(actualIteratorOptions, options));
        });
        List<?> expectedCall9 = Arrays.asList(iterator, "setHitTermsList", hitTerms);
        expectedCallCounts.merge(expectedCall9, 1, Integer::sum);
        interactionChecks.add(() -> verify(iterator, times(expectedCallCounts.get(expectedCall9))).setHitTermsList(hitTerms));
        List<?> expectedCall10 = Arrays.asList(iterator, "setDirection", "BOTH");
        expectedCallCounts.merge(expectedCall10, 1, Integer::sum);
        interactionChecks.add(() -> verify(iterator, times(expectedCallCounts.get(expectedCall10))).setDirection("BOTH"));
        List<?> expectedCall11 = Arrays.asList(iterator, "setOrigHalfSize", (float) ((end + 1) - start) / 2);
        expectedCallCounts.merge(expectedCall11, 1, Integer::sum);
        interactionChecks.add(() -> verify(iterator, times(expectedCallCounts.get(expectedCall11))).setOrigHalfSize((float) ((end + 1) - start) / 2));
        List<?> expectedCall12 = Arrays.asList(iterator, "seek", "any", "any", false);
        expectedCallCounts.merge(expectedCall12, 1, Integer::sum);
        interactionChecks.add(() -> verify(iterator, times(expectedCallCounts.get(expectedCall12))).seek(any(), any(), eq(false)));
    }

    private Value getTfValue(int[][] offsets, Integer score) throws IOException {
        TermWeight.Info.Builder builder = TermWeight.Info.newBuilder();
        for (int[] offset : offsets) {
            builder.addTermOffset(offset[1]);
            builder.addPrevSkips(offset[1] - offset[0]);
            if (score != null) {
                builder.addScore(score);
            }
        }

        return new Value(builder.build().toByteArray());
    }

    private void givenMatchingTermFrequencies(String field, int[][] offsets, String value) throws IOException {
        Value tfpb = getTfValue(offsets, 1);

        doReturn(true).when(source).hasTop();
        List<?> expectedCall18 = Arrays.asList(source, "hasTop");
        expectedCallCounts.merge(expectedCall18, 1, Integer::sum);
        interactionChecks.add(() -> verify(source, times(expectedCallCounts.get(expectedCall18))).hasTop());
        // The getTopKey() is called to extract the parsed field name from the column qualifier
        Key tfKey = new Key(new Text("row"), new Text("cf"), new Text("cf\u0000" + value + "\u0000" + field));
        doReturn(tfKey).when(source).getTopKey();
        List<?> expectedCall19 = Arrays.asList(source, "getTopKey");
        expectedCallCounts.merge(expectedCall19, 1, Integer::sum);
        interactionChecks.add(() -> verify(source, times(expectedCallCounts.get(expectedCall19))).getTopKey());
        doReturn(tfpb).when(source).getTopValue();
        List<?> expectedCall20 = Arrays.asList(source, "getTopValue");
        expectedCallCounts.merge(expectedCall20, 1, Integer::sum);
        interactionChecks.add(() -> verify(source, times(expectedCallCounts.get(expectedCall20))).getTopValue());
        List<?> expectedCall21 = Arrays.asList(source, "seek", "any", "any", false);
        expectedCallCounts.merge(expectedCall21, 1, Integer::sum);
        interactionChecks.add(() -> verify(source, times(expectedCallCounts.get(expectedCall21))).seek(any(), any(), eq(false)));
    }

    private Map<String,String> getOptions(String field, int start, int end) {
        Map<String,String> options = new HashMap<>();
        options.put(FIELD_NAME, field);
        options.put(START_OFFSET, String.valueOf(start));
        // for the options, the end offset is exclusive so add 1
        options.put(END_OFFSET, String.valueOf(end + 1));
        return options;
    }
}
