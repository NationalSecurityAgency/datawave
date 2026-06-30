package datawave.query.transformer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

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

/**
 * Tests for {@link ExcerptTransform}. These tests exercise the transform against a real {@link TermFrequencyExcerptIterator} backed by a
 * {@link SortedMapIterator} over term-frequency data, rather than mocking the iterator and source. This way the excerpts are actually generated from the
 * term-frequency entries, exercising both the hit-term offset resolution in {@code getOffset} and the excerpt generation in the iterator.
 */
public class ExcerptTransformTest {

    // the document under test: row=shard, cf=dt\0uid, so the event id is shard\0dt\0uid
    private static final String DATATYPE = "dt";
    private static final String UID = "uid";
    private static final Text SHARD = new Text("shard");
    private static final Key DOC_KEY = new Key(SHARD, new Text(DATATYPE + Constants.NULL + UID));
    // the event id as computed by ExcerptTransform.keyToEventId(DOC_KEY): row + NUL + columnFamily
    private static final String EVENT_ID = "shard" + Constants.NULL + DATATYPE + Constants.NULL + UID;

    // token streams used to build term-frequency data. each token occupies the offset matching its index in the phrase.
    private static final String BODY_TEXT = "the quick brown fox jumped over the lazy dog and the cat sat on the mat near the red barn";
    private static final String CONTENT_TEXT = "alpha beta gamma delta epsilon zeta eta theta";

    private final TreeMap<Key,Value> termFrequencyData = new TreeMap<>();
    private PhraseIndexes phraseIndexes;
    private ExcerptFields excerptFields;

    private Document document;
    private ExcerptTransform excerptTransform;

    @Before
    public void setUp() {
        termFrequencyData.clear();
        phraseIndexes = new PhraseIndexes();
        excerptFields = new ExcerptFields();
    }

    /**
     * Verify that a null entry is returned for a null input.
     */
    @Test
    public void testNullDocumentEntry() {
        initTransform();
        assertNull(excerptTransform.apply(null));
    }

    /**
     * Verify that excerpts are not added for documents that are not marked as to-keep.
     */
    @Test
    public void testNonToKeepDocumentEntry() {
        document = new Document(DOC_KEY, false);
        initTransform();

        applyTransform();

        assertFalse(document.containsKey(ExcerptTransform.HIT_EXCERPT));
    }

    /**
     * Verify that excerpts are not added for documents that don't have any phrase indexes or hit terms.
     */
    @Test
    public void testNoPhraseIndexesOrHitTerms() {
        document = new Document(DOC_KEY, true);
        initTransform();

        applyTransform();

        assertFalse(document.containsKey(ExcerptTransform.HIT_EXCERPT));
    }

    /**
     * Verify that excerpts are generated for both a phrase-function phrase index and a separate, non-overlapping hit term.
     */
    @Test
    public void testExcerpts() {
        givenExcerptField("BODY", 2);
        givenTermFrequencyData("BODY", BODY_TEXT);

        // a phrase function matched "jumped over" at offsets 4-5
        givenPhraseIndex("BODY", 4, 5);
        // and BODY:cat is a hit term, found at offset 11, which does not overlap the phrase
        givenDocumentWithHitTerms("BODY:cat");

        initTransform();
        applyTransform();

        Set<String> excerpts = getExcerpts();
        assertEquals(2, excerpts.size());
        // window around the phrase [4,5] +/- 2 -> offsets 2..7
        assertTrue(excerpts.contains("brown fox jumped over the lazy"));
        // window around the hit term at 11 +/- 2 -> offsets 9..13, with the hit term bracketed
        assertTrue(excerpts.contains("and the [cat] sat on"));
    }

    /**
     * Verify that a hit term overlapping a phrase index is merged into a single excerpt.
     */
    @Test
    public void testExcerptOverlapped() {
        givenExcerptField("BODY", 2);
        givenTermFrequencyData("BODY", BODY_TEXT);

        // a phrase function matched "jumped over" at offsets 4-5
        givenPhraseIndex("BODY", 4, 5);
        // and BODY:jumped is a hit term at offset 4, which overlaps the phrase, so the two are merged
        givenDocumentWithHitTerms("BODY:jumped");

        initTransform();
        applyTransform();

        Set<String> excerpts = getExcerpts();
        assertEquals(1, excerpts.size());
        assertTrue(excerpts.contains("brown fox [jumped] over the lazy"));
    }

    /**
     * Verify that multiple phrase indexes each produce an excerpt, while a hit term overlapping one of them is merged in.
     */
    @Test
    public void testExcerptOverlappedAndPhraseOverlapped() {
        givenExcerptField("BODY", 2);
        givenTermFrequencyData("BODY", BODY_TEXT);

        // phrase indexes spaced far enough apart that their +/- 2 windows do not overlap and merge
        givenPhraseIndex("BODY", 4, 5);
        givenPhraseIndex("BODY", 11, 12);
        givenPhraseIndex("BODY", 18, 19);
        // BODY:jumped at offset 4 overlaps the first phrase index and is merged into it
        givenDocumentWithHitTerms("BODY:jumped");

        initTransform();
        applyTransform();

        Set<String> excerpts = getExcerpts();
        assertEquals(3, excerpts.size());
        assertTrue(excerpts.contains("brown fox [jumped] over the lazy"));
        assertTrue(excerpts.contains("and the cat sat on the"));
        assertTrue(excerpts.contains("near the red barn"));
    }

    /**
     * Verify that when a phrase index start is less than the specified excerpt offset, the excerpt start defaults to 0.
     */
    @Test
    public void testOffsetGreaterThanStartIndex() {
        givenExcerptField("CONTENT", 5);
        givenTermFrequencyData("CONTENT", CONTENT_TEXT);

        // phrase index [1,2] with offset 5 would start at -4, which should be clamped to 0
        givenPhraseIndex("CONTENT", 1, 2);
        givenDocumentWithPhraseIndexesOnly();

        initTransform();
        applyTransform();

        Set<String> excerpts = getExcerpts();
        assertEquals(1, excerpts.size());
        assertTrue(excerpts.contains("alpha beta gamma delta epsilon zeta eta theta"));
    }

    /**
     * Verify that an excerpt is generated for a hit term even when there are no phrase indexes.
     */
    @Test
    public void testNoPhraseIndexes() {
        givenExcerptField("BODY", 2);
        givenTermFrequencyData("BODY", BODY_TEXT);

        // no phrase indexes, only a hit term
        givenDocumentWithHitTerms("BODY:cat");

        initTransform();
        applyTransform();

        Set<String> excerpts = getExcerpts();
        assertEquals(1, excerpts.size());
        assertTrue(excerpts.contains("and the [cat] sat on"));
    }

    /**
     * Verify that an excerpt requested for a base field is generated from term frequencies stored under a grouped variant of that field (e.g. BODY.A1B2C3).
     * This exercises both the grouped-field-aware hit-term offset resolution in {@code getOffset} and the grouped-field matching in the iterator.
     */
    @Test
    public void testExcerptForGroupedField() {
        givenExcerptField("BODY", 2);
        // the term frequencies are stored under a grouped field name rather than the base field name
        givenTermFrequencyData("BODY.A1B2C3", BODY_TEXT);

        givenDocumentWithHitTerms("BODY:cat");

        initTransform();
        applyTransform();

        Set<String> excerpts = getExcerpts();
        assertEquals(1, excerpts.size());
        assertTrue(excerpts.contains("and the [cat] sat on"));
    }

    private void initTransform() {
        SortedMapIterator source = new SortedMapIterator(termFrequencyData);
        excerptTransform = new ExcerptTransform(excerptFields, null, source, new TermFrequencyExcerptIterator());
    }

    private void applyTransform() {
        excerptTransform.apply(new AbstractMap.SimpleEntry<>(new Key(), document));
    }

    private void givenExcerptField(String field, int offset) {
        excerptFields.put(field, offset);
    }

    private void givenPhraseIndex(String field, int start, int end) {
        // end offset is inclusive
        phraseIndexes.addIndexTriplet(field, EVENT_ID, start, end);
    }

    /** Build a document with the accumulated phrase indexes and the given {@code FIELD:value} hit terms. */
    private void givenDocumentWithHitTerms(String... hitTerms) {
        document = new Document(DOC_KEY, true);
        document.put(ExcerptTransform.PHRASE_INDEXES_ATTRIBUTE, new Content(phraseIndexes.toString(), DOC_KEY, false));

        List<Attribute<? extends Comparable<?>>> hits = new ArrayList<>();
        for (String hitTerm : hitTerms) {
            hits.add(new Content(hitTerm, DOC_KEY, true));
        }
        document.put(JexlEvaluation.HIT_TERM_FIELD, new Attributes(hits, true));
    }

    /** Build a document with the accumulated phrase indexes and no hit terms. */
    private void givenDocumentWithPhraseIndexesOnly() {
        document = new Document(DOC_KEY, true);
        document.put(ExcerptTransform.PHRASE_INDEXES_ATTRIBUTE, new Content(phraseIndexes.toString(), DOC_KEY, false));
    }

    /**
     * Add term-frequency entries for the given field, one entry per distinct token, where each token occupies the offset matching its position in the phrase.
     *
     * @param field
     *            the field name to store the term frequencies under (may be a grouped field name such as BODY.A1B2C3)
     * @param phrase
     *            the space-separated tokens to index
     */
    private void givenTermFrequencyData(String field, String phrase) {
        String[] tokens = phrase.split(" ");
        Multimap<String,Integer> wordOffsets = ArrayListMultimap.create();
        for (int i = 0; i < tokens.length; i++) {
            wordOffsets.put(tokens[i], i);
        }

        for (String word : wordOffsets.keySet()) {
            List<Integer> offsets = new ArrayList<>(wordOffsets.get(word));
            Collections.sort(offsets);

            TermWeight.Info.Builder builder = TermWeight.Info.newBuilder();
            for (int offset : offsets) {
                builder.addTermOffset(offset);
                builder.addPrevSkips(0);
                builder.addScore(-1);
            }

            Text colq = new Text(DATATYPE + Constants.NULL + UID + Constants.NULL + word + Constants.NULL + field);
            Key key = new Key(SHARD, Constants.TERM_FREQUENCY_COLUMN_FAMILY, colq);
            termFrequencyData.put(key, new Value(builder.build().toByteArray()));
        }
    }

    private Set<String> getExcerpts() {
        assertTrue("expected HIT_EXCERPT to be populated", document.containsKey(ExcerptTransform.HIT_EXCERPT));
        Attributes attributes = (Attributes) document.get(ExcerptTransform.HIT_EXCERPT);
        return attributes.getAttributes().stream().map(a -> a.getData().toString()).collect(Collectors.toSet());
    }
}
