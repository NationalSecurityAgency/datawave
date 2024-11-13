package datawave.query.transformer;

import static datawave.query.iterator.logic.TermFrequencyExcerptIterator.Configuration.END_OFFSET;
import static datawave.query.iterator.logic.TermFrequencyExcerptIterator.Configuration.FIELD_NAME;
import static datawave.query.iterator.logic.TermFrequencyExcerptIterator.Configuration.START_OFFSET;
import static org.easymock.EasyMock.and;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.easymock.Capture;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

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

@RunWith(EasyMockRunner.class)
public class ExcerptTransformTest extends EasyMockSupport {

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

    @Before
    public void setUp() {
        phraseIndexes = new PhraseIndexes();
        excerptFields = new ExcerptFields();
    }

    @After
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

        replayAll();

        assertNull(excerptTransform.apply(null));
        verifyAll();
    }

    /**
     * Verify that excerpts are not added for documents that are not marked as to-keep.
     */
    @Test
    public void testNonToKeepDocumentEntry() {
        givenDocument(new Document(new Key(), false));
        initTransform();
        replayAll();

        applyTransform();

        assertFalse(document.containsKey(ExcerptTransform.HIT_EXCERPT));
        verifyAll();
    }

    /**
     * Verify that excerpts are not added for documents that don't have PHRASE_INDEXES_ATTRIBUTE.
     */
    @Test
    public void testNullPhraseIndexes() {
        givenDocument(new Document(new Key(), true));
        initTransform();
        replayAll();

        applyTransform();

        assertFalse(document.containsKey(ExcerptTransform.HIT_EXCERPT));
        verifyAll();
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
        givenMockDocumentWithHitTerm("BODY", "word");
        givenMatchingTermFrequencies("BODY", new int[][] {{24, 24}}, "word");
        // end offset is inclusive
        givenMatchingPhrase("BODY", 22, 26, "and the [word] from bird", List.of("word"));

        // also setup a phrase to match on either side of the matching phrase
        // end offset is inclusive
        givenMatchingPhrase("BODY", 8, 16, "the quick brown fox jumped over the lazy dog", List.of("word"));

        Capture<Attributes> capturedArg = Capture.newInstance();
        document.put(eq(ExcerptTransform.HIT_EXCERPT), and(capture(capturedArg), isA(Attributes.class)));

        initTransform();
        replayAll();

        applyTransform();
        verifyAll();

        Attributes arg = capturedArg.getValue();
        assertEquals(2, arg.size());
        Set<String> excerpts = arg.getAttributes().stream().map(a -> a.getData().toString()).collect(Collectors.toSet());
        // both excerpts should be returned
        assertTrue(excerpts.contains("and the [word] from bird"));
        assertTrue(excerpts.contains("the quick brown fox jumped over the lazy dog"));
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
        givenMockDocumentWithHitTerm("BODY", "quick brown");
        givenMatchingTermFrequencies("BODY", new int[][] {{1, 2}, {2, 3}, {9, 10}, {20, 21}}, "quick brown");
        // note that the start is relative to index 9 (i.e. 9-2=7) because the overlapping term starts at 9
        // end offset is inclusive
        givenMatchingPhrase("BODY", 7, 16, "and the [quick] [brown] fox jumped over the lazy dog", List.of("quick", "brown"));

        Capture<Attributes> capturedArg = Capture.newInstance();
        document.put(eq(ExcerptTransform.HIT_EXCERPT), and(capture(capturedArg), isA(Attributes.class)));

        initTransform();
        replayAll();

        applyTransform();
        verifyAll();

        Attributes arg = capturedArg.getValue();
        assertEquals(1, arg.size());
        String excerpt = arg.getAttributes().iterator().next().getData().toString();
        // only one excerpt should return
        assertEquals("and the [quick] [brown] fox jumped over the lazy dog", excerpt);
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
        givenMockDocumentWithHitTerm("BODY", "quick brown");
        givenMatchingTermFrequencies("BODY", new int[][] {{1, 2}, {2, 3}, {9, 10}, {20, 21}}, "quick brown");
        // end offset is inclusive
        givenMatchingPhrase("BODY", 23, 28, "Jack and Jill jumped over the", List.of("quick", "brown"));
        // note that the start is relative to overlapping term index 9 (i.e. 9-2=7) because the overlapping term starts at 9
        // AND then we combined the phrase from 2 to 7 with the one from 7 to 16
        givenMatchingPhrase("BODY", 2, 16, "the [brown] chicken layed an egg and the [quick] [brown] fox jumped over the lazy dog", List.of("quick", "brown"));

        Capture<Attributes> capturedArg = Capture.newInstance();
        document.put(eq(ExcerptTransform.HIT_EXCERPT), and(capture(capturedArg), isA(Attributes.class)));

        initTransform();
        replayAll();

        applyTransform();
        verifyAll();

        Attributes arg = capturedArg.getValue();
        assertEquals(2, arg.size());
        Set<String> excerpts = arg.getAttributes().stream().map(a -> a.getData().toString()).collect(Collectors.toSet());
        // all excerpts should be returned
        assertTrue(excerpts.contains("Jack and Jill jumped over the"));
        assertTrue(excerpts.contains("the [brown] chicken layed an egg and the [quick] [brown] fox jumped over the lazy dog"));
    }

    /**
     * Verify that when a start index is less than the specified excerpt offset, that the excerpt start defaults to 0.
     */
    @Test
    public void testOffsetGreaterThanStartIndex() throws IOException {
        givenExcerptField("CONTENT", 2);
        // end offset is inclusive
        givenPhraseIndex("CONTENT", 1, 5);

        givenMockDocument();
        // end offset is inclusive
        givenMatchingPhrase("CONTENT", 0, 7, "the quick brown fox jumped over the lazy dog", Collections.emptyList());

        Capture<Attributes> capturedArg = Capture.newInstance();
        document.put(eq(ExcerptTransform.HIT_EXCERPT), and(capture(capturedArg), isA(Attributes.class)));

        initTransform();
        replayAll();

        applyTransform();
        verifyAll();

        Attributes arg = capturedArg.getValue();
        assertEquals(1, arg.size());
        Attribute<?>[] attributes = arg.getAttributes().toArray(new Attribute[0]);
        assertEquals("the quick brown fox jumped over the lazy dog", ((Content) attributes[0]).getContent());
    }

    /**
     * Verify that a phrase index with the end before the start does not mess us up
     */
    @Test
    public void testEmptyPhraseIndexes() throws IOException {
        // Setup our excerpts to match 2 words on either side of the BODY
        givenExcerptField("BODY", 2);

        // setup a matching term for BODY:word with an empty phrase index attribute
        givenMockDocumentWithHitTerm("BODY", "word");
        givenMatchingTermFrequencies("BODY", new int[][] {{24, 24}}, "word");
        // end offset is inclusive
        givenMatchingPhrase("BODY", 22, 26, "and the [word] from bird", List.of("word"));

        Capture<Attributes> capturedArg = Capture.newInstance();
        document.put(eq(ExcerptTransform.HIT_EXCERPT), and(capture(capturedArg), isA(Attributes.class)));

        initTransform();
        replayAll();

        applyTransform();
        verifyAll();

        Attributes arg = capturedArg.getValue();
        assertEquals(1, arg.size());
        Set<String> excerpts = arg.getAttributes().stream().map(a -> a.getData().toString()).collect(Collectors.toSet());
        // both excerpts should be returned
        assertTrue(excerpts.contains("and the [word] from bird"));
    }

    private void initTransform() {
        excerptTransform = new ExcerptTransform(excerptFields, env, source, iterator);
    }

    private void applyTransform() {
        excerptTransform.apply(getDocumentEntry());
    }

    private void givenMockDocument() {
        Document document = mock(Document.class);

        expect(document.isToKeep()).andReturn(true);
        expect(document.containsKey(ExcerptTransform.PHRASE_INDEXES_ATTRIBUTE)).andReturn(true);
        Key metadata = new Key("Row", "cf", "cq");
        @SuppressWarnings("rawtypes")
        Attribute phraseIndexAttribute = new Content(phraseIndexes.toString(), metadata, false);
        // noinspection unchecked
        expect(document.get(ExcerptTransform.PHRASE_INDEXES_ATTRIBUTE)).andReturn(phraseIndexAttribute);

        expect(document.containsKey(JexlEvaluation.HIT_TERM_FIELD)).andReturn(false);

        givenDocument(document);
    }

    @SuppressWarnings("rawtypes")
    private void givenMockDocumentWithHitTerm(String field, String value) {
        Document document = mock(Document.class);

        expect(document.isToKeep()).andReturn(true);
        expect(document.containsKey(ExcerptTransform.PHRASE_INDEXES_ATTRIBUTE)).andReturn(true);
        Key metadata = new Key("shard", "dt\u0000uid");
        Attribute phraseIndexAttribute = new Content(phraseIndexes.toString(), metadata, false);
        // noinspection unchecked
        expect(document.get(ExcerptTransform.PHRASE_INDEXES_ATTRIBUTE)).andReturn(phraseIndexAttribute);

        expect(document.containsKey(JexlEvaluation.HIT_TERM_FIELD)).andReturn(true);

        Attribute hitTerms = new Attributes(Collections.singletonList(new Content(field + ":" + value, metadata, true)), true);
        expect(document.get(JexlEvaluation.HIT_TERM_FIELD)).andReturn(hitTerms);

        givenDocument(document);
    }

    private void givenDocument(Document document) {
        this.document = document;
    }

    private Map.Entry<Key,Document> getDocumentEntry() {
        return new AbstractMap.SimpleEntry<>(new Key(), document);
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
        iterator.init(source, options, env);
        iterator.setHitTermsList(hitTerms);
        iterator.setDirection("BOTH");
        iterator.setOrigHalfSize((float) ((end + 1) - start) / 2);
        iterator.seek(anyObject(), anyObject(), eq(false));
        if (phrase != null) {
            expect(iterator.hasTop()).andReturn(true);
            Key key = new Key(new Text("row"), new Text("cf"),
                            new Text(field + Constants.NULL + "XXXNOTSCOREDXXX" + Constants.NULL + phrase + Constants.NULL + "XXXNOTSCOREDXXX"));
            expect(iterator.getTopKey()).andReturn(key);
        } else {
            expect(iterator.hasTop()).andReturn(false);
        }
    }

    private void givenMatchingTermFrequencies(String field, int[][] offsets, String value) throws IOException {
        TermWeight.Info.Builder builder = TermWeight.Info.newBuilder();
        for (int[] offset : offsets) {
            builder.addTermOffset(offset[1]);
            builder.addPrevSkips(offset[1] - offset[0]);
            builder.addScore(1);
        }

        Value tfpb = new Value(builder.build().toByteArray());

        source.seek(anyObject(), anyObject(), eq(false));
        expect(source.hasTop()).andReturn(true);
        expect(source.getTopValue()).andReturn(tfpb);
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
