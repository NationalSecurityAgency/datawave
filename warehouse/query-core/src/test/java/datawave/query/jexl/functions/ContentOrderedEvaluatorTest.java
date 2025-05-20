package datawave.query.jexl.functions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import datawave.ingest.protobuf.TermWeightPosition;
import datawave.query.postprocessing.tf.PhraseOffset;
import datawave.query.postprocessing.tf.TermOffsetMap;

public class ContentOrderedEvaluatorTest {

    private static final String EVENT_ID = "shard\u0000dt\u0000uid";
    private TermOffsetMap termOffsetMap;
    private final List<List<TermWeightPosition>> offsets = new ArrayList<>();
    private String field;
    private String eventId = EVENT_ID;
    private int distance;
    private String[] terms;

    private WrappedContentOrderedEvaluator evaluator;

    @Before
    public void setup() {
        termOffsetMap = new TermOffsetMap();
    }

    @After
    public void teardown() {
        field = null;
        eventId = EVENT_ID;
        offsets.clear();
        terms = null;
    }

    /**
     * Issue #659
     * <p>
     * Test for edge case that causes {@code ContentOrderedEvaluator.traverseFailure(...)} to be invoked during forward-order evaluation (see
     * {@link ContentOrderedEvaluator#FORWARD})
     * <p>
     * That is, when partial-match is encountered first (as with offsets 10->11 below), the traverseFailure method is invoked and must propagate the subsequent
     * full-match result (19->20->21) back up the recursion stack. Formerly, the fact of the full match was lost and the associated document was omitted from
     * search results
     */
    @Test
    public void evaluate_traverseFailureFORWARDTest() {
        // offsets[0].size() <= offsets[N-1].size() will trigger forward-order traversal,
        // evaluating the document for the phrase "a b c", starting with 'a' at offset 10

        givenField("CONTENT");
        givenDistance(1);
        givenOffsets(10, 19);
        givenOffsets(11, 20);
        givenOffsets(3, 21, 100);
        givenTerms("a", "b", "c");
        givenGatherPhraseOffsets(true);
        givenExcerptFields("CONTENT");

        initEvaluator();

        assertEvaluate(true);
        assertPhraseOffsetsContain("CONTENT", 19, 21);
    }

    /**
     * Issue #659
     * <p>
     * Test for edge case that causes {@code ContentOrderedEvaluator.traverseFailure(...)} to be invoked during reverse-order evaluation (see
     * {@link ContentOrderedEvaluator#REVERSE})
     * <p>
     * That is, when partial-match is encountered first (as with offsets 21->20 below), the traverseFailure method is invoked and must propagate the subsequent
     * full-match result (3->2->1) back up the recursion stack. Formerly, the fact of the full match was lost and the associated document was omitted from
     * search results
     */
    @Test
    public void evaluate_traverseFailureREVERSETest() {
        // offsets[0].size() > offsets[N-1].size() will trigger reverse-order traversal,
        // evaluating the document for the phrase "c b a", starting with 'c' at offset 21

        givenField("BODY");
        givenDistance(1);
        givenOffsets(1, 10, 100);
        givenOffsets(2, 20);
        givenOffsets(3, 21);
        givenTerms("a", "b", "c");
        givenGatherPhraseOffsets(true);
        givenExcerptFields("BODY", "CONTENT");

        initEvaluator();

        assertEvaluate(true);
        assertPhraseOffsetsContain("BODY", 1, 3);
    }

    /**
     * Assert that a match for the terms is found for offsets 20->21->22.
     */
    @Test
    public void evaluate_alternatingExtremesFORWARDTest() {
        givenField("CONTENT");
        givenDistance(1);
        givenOffsets(1, 10, 20);
        givenOffsets(21, 24, 30);
        givenOffsets(3, 8, 12, 19, 22);
        givenTerms("a", "b", "c");
        givenGatherPhraseOffsets(true);
        givenExcerptFields("CONTENT");

        initEvaluator();

        assertEvaluate(true);
        assertPhraseOffsetsContain("CONTENT", 20, 22);
    }

    /**
     * Assert that a match for the terms is found for offsets 102->101->100.
     */
    @Test
    public void evaluate_alternatingExtremesREVERSETest() {
        givenField("CONTENT");
        givenDistance(1);
        givenOffsets(100, 200, 300, 500, 601);
        givenOffsets(1, 5, 29, 87, 101);
        givenOffsets(102, 400, 434);
        givenTerms("a", "b", "c");
        givenGatherPhraseOffsets(true);
        givenExcerptFields("CONTENT");

        initEvaluator();

        assertEvaluate(true);
        assertPhraseOffsetsContain("CONTENT", 100, 102);
    }

    @Test
    public void evaluate_notEnoughOffsetsTest() {
        givenField("CONTENT");
        givenDistance(1);
        givenTerms("a", "b", "c");
        givenGatherPhraseOffsets(true);
        givenExcerptFields("CONTENT");

        initEvaluator();

        assertEvaluate(false);
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void evaluate_pruneAllTest() {
        givenField("BODY");
        givenDistance(1);
        givenOffsets(1);
        givenOffsets(5);
        givenOffsets(11);
        givenTerms("a", "b", "c");
        givenGatherPhraseOffsets(true);
        givenExcerptFields("CONTENT");

        initEvaluator();

        assertEvaluate(false);
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void evaluate_simpleSuccessDistance1Test() {
        givenField("CONTENT");

        givenDistance(1);
        givenOffsets(1);
        givenOffsets(2);
        givenOffsets(3);
        givenTerms("a", "b", "c");
        givenGatherPhraseOffsets(true);
        givenExcerptFields("CONTENT");

        initEvaluator();

        assertEvaluate(true);
        assertPhraseOffsetsContain("CONTENT", 1, 3);
    }

    @Test
    public void evaluate_simpleSuccessDistance3Test() {
        givenField("CONTENT");
        givenDistance(3);
        givenOffsets(1);
        givenOffsets(2);
        givenOffsets(3);
        givenTerms("a", "b", "c");
        givenGatherPhraseOffsets(true);
        givenExcerptFields("CONTENT");

        initEvaluator();

        assertEvaluate(true);
        assertPhraseOffsetsContain("CONTENT", 1, 3);
    }

    @Test
    public void evaluate_simpleSuccessDistance3FailTest() {
        givenField("CONTENT");
        givenDistance(3);
        givenOffsets(1);
        givenOffsets(5);
        givenOffsets(7);
        givenTerms("a", "b", "c");
        givenGatherPhraseOffsets(true);
        givenExcerptFields("CONTENT");

        initEvaluator();

        assertEvaluate(false);
        assertPhraseOffsetsEmpty();
    }

    @Test
    public void evaluate_pruneTopTest() {
        givenField("CONTENT");
        givenDistance(1);
        givenOffsets(1, 10);
        givenOffsets(10, 11);
        givenOffsets(4, 12);
        givenTerms("a", "b", "c");
        givenGatherPhraseOffsets(true);
        givenExcerptFields("CONTENT");

        initEvaluator();

        assertEvaluate(true);
        assertPhraseOffsetsContain("CONTENT", 10, 12);
    }

    @Test
    public void evaluate_pruneBottomTest() {
        givenField("BODY");
        givenDistance(1);
        givenOffsets(1, 3, 8);
        givenOffsets(3, 4, 44);
        givenOffsets(4, 5, 35);
        givenTerms("a", "b", "c");
        givenGatherPhraseOffsets(true);
        givenExcerptFields("BODY");

        initEvaluator();

        assertEvaluate(true);
        assertPhraseOffsetsContain("BODY", 3, 4); // TODO - do we want the end offset to be 5 in order to be consecutive?
    }

    @Test
    public void evaluate_pruneTopAndBottomTest() {
        givenField("CONTENT");
        givenDistance(1);
        givenOffsets(3, 4, 5, 6);
        givenOffsets(7, 9, 22);
        givenOffsets(8, 11, 13);
        givenTerms("a", "b", "c");
        givenGatherPhraseOffsets(true);
        givenExcerptFields("CONTENT");

        initEvaluator();

        assertEvaluate(true);
        assertPhraseOffsetsContain("CONTENT", 6, 8);
    }

    @Test
    public void evaluate_livePruneTest() {
        givenField("CONTENT");
        givenDistance(2);
        givenOffsets(1, 3);
        givenOffsets(2, 4);
        givenOffsets(5, 6);
        givenTerms("a", "b", "c");
        givenGatherPhraseOffsets(true);
        givenExcerptFields("CONTENT");

        initEvaluator();

        assertEvaluate(true);
        assertPhraseOffsetsContain("CONTENT", 3, 5);
    }

    /**
     * Verify that if gatherPhraseOffsets is false, that even when there is a phrase index for matching excerpt field, it is not recorded.
     */
    @Test
    public void testGatherPhraseOffsetsIsFalse() {
        // offsets[0].size() <= offsets[N-1].size() will trigger forward-order traversal,
        // evaluating the document for the phrase "a b c", starting with 'a' at offset 10

        givenField("CONTENT");
        givenDistance(1);
        givenOffsets(10, 19);
        givenOffsets(11, 20);
        givenOffsets(3, 21, 100);
        givenTerms("a", "b", "c");
        givenGatherPhraseOffsets(false);
        givenExcerptFields("CONTENT");

        initEvaluator();

        assertEvaluate(true);
        assertPhraseOffsetsEmpty();
    }

    /**
     * Verify that if gatherPhraseOffsets is true, if a phrase index for is found for a non-excerpt field, it is not recorded.
     */
    @Test
    public void testNonMatchingExcerptFields() {
        // offsets[0].size() <= offsets[N-1].size() will trigger forward-order traversal,
        // evaluating the document for the phrase "a b c", starting with 'a' at offset 10

        givenField("CONTENT");
        givenDistance(1);
        givenOffsets(10, 19);
        givenOffsets(11, 20);
        givenOffsets(3, 21, 100);
        givenTerms("a", "b", "c");
        givenGatherPhraseOffsets(true);
        givenExcerptFields("BODY");

        initEvaluator();

        assertEvaluate(true);
        assertPhraseOffsetsEmpty();
    }

    private void givenField(String field) {
        this.field = field;
    }

    private void givenOffsets(int... offsets) {
        List<TermWeightPosition> list = new ArrayList<>();
        for (int offset : offsets) {
            list.add(new TermWeightPosition.Builder().setOffset(offset).setZeroOffsetMatch(true).build());
        }
        this.offsets.add(list);
    }

    private void givenDistance(int distance) {
        this.distance = distance;
    }

    private void givenTerms(String... terms) {
        this.terms = terms;
    }

    private void givenGatherPhraseOffsets(boolean gatherPhraseOffsets) {
        termOffsetMap.setGatherPhraseOffsets(gatherPhraseOffsets);
    }

    private void givenExcerptFields(String... fields) {
        termOffsetMap.setExcerptFields(Set.of(fields));
    }

    private void initEvaluator() {
        evaluator = new WrappedContentOrderedEvaluator(null, distance, termOffsetMap, terms);
    }

    private void assertEvaluate(boolean expected) {
        assertEquals("Expected evaluate() to return " + expected, expected, evaluator.evaluate(field, eventId, offsets));
    }

    private void assertPhraseOffsetsContain(String field, int startOffset, int endOffset) {
        Collection<PhraseOffset> phraseOffsets = termOffsetMap.getPhraseIndexes(field);
        boolean found = phraseOffsets.stream()
                        .anyMatch(pair -> pair.getEventId().equals(eventId) && pair.getStartOffset() == startOffset && pair.getEndOffset() == endOffset);
        assertTrue("Expected phrase offset [" + startOffset + ", " + endOffset + "] for field " + field + " and eventId " + eventId.replace('\u0000', '/'),
                        found);
    }

    private void assertPhraseOffsetsEmpty() {
        assertTrue("Expected empty phrase offset map", termOffsetMap.getPhraseIndexes() == null || termOffsetMap.getPhraseIndexes().isEmpty());
    }

    private static class WrappedContentOrderedEvaluator extends ContentOrderedEvaluator {
        public WrappedContentOrderedEvaluator(Set<String> fields, int distance, TermOffsetMap termOffsetMap, String... terms) {
            super(fields, distance, Float.MIN_VALUE, termOffsetMap, terms);
        }

        @Override
        protected boolean evaluate(String field, String eventId, List<List<TermWeightPosition>> offsets) {
            return super.evaluate(field, eventId, offsets);
        }
    }
}
