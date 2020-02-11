package datawave.query.jexl.functions;

import datawave.ingest.protobuf.TermWeightPosition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ContentOrderedEvaluatorTest {
    
    private WrappedContentOrderedEvaluator evaluator;
    
    @Before
    public void setup() {}
    
    private List<TermWeightPosition> asList(boolean zeroOffsetMatch, List<Integer> offsets, List<Integer> skips) {
        if (offsets.size() != skips.size()) {
            Assert.fail("offsets and skips size meed to match");
        }
        List<TermWeightPosition> list = new ArrayList<>();
        for (int i = 0; i < offsets.size(); i++) {
            list.add(getPosition(offsets.get(i), skips.get(i), zeroOffsetMatch));
        }
        return list;
    }
    
    private List<TermWeightPosition> asList(int... offsets) {
        return asList(true, offsets);
    }
    
    private List<TermWeightPosition> asList(boolean zeroOffsetMatch, int... offsets) {
        List<TermWeightPosition> list = new ArrayList<>();
        for (int offset : offsets) {
            list.add(getPosition(offset, zeroOffsetMatch));
        }
        return list;
    }
    
    private TermWeightPosition getPosition(int offset) {
        return new TermWeightPosition.Builder().setOffset(offset).build();
    }
    
    private TermWeightPosition getPosition(int offset, boolean zeroOffsetMatch) {
        return new TermWeightPosition.Builder().setOffset(offset).setZeroOffsetMatch(zeroOffsetMatch).build();
    }
    
    private TermWeightPosition getPosition(int offset, int prevSkips, boolean zeroOffsetMatch) {
        return new TermWeightPosition.Builder().setOffset(offset).setPrevSkips(prevSkips).setZeroOffsetMatch(zeroOffsetMatch).build();
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
        
        List<List<TermWeightPosition>> offsets = new ArrayList<>();
        List<TermWeightPosition> offsetsA = asList(10, 19);
        List<TermWeightPosition> offsetsB = asList(11, 20);
        List<TermWeightPosition> offsetsC = asList(3, 21, 100);
        
        offsets.add(offsetsA);
        offsets.add(offsetsB);
        offsets.add(offsetsC);
        
        evaluator = new WrappedContentOrderedEvaluator(null, 1, new HashMap<>(), "a", "b", "c");
        
        Assert.assertTrue(evaluator.evaluate(offsets));
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
        
        List<List<TermWeightPosition>> offsets = new ArrayList<>();
        List<TermWeightPosition> offsetsA = asList(1, 10, 100);
        List<TermWeightPosition> offsetsB = asList(2, 20);
        List<TermWeightPosition> offsetsC = asList(3, 21);
        
        offsets.add(offsetsA);
        offsets.add(offsetsB);
        offsets.add(offsetsC);
        
        evaluator = new WrappedContentOrderedEvaluator(null, 1, new HashMap<>(), "a", "b", "c");
        
        Assert.assertTrue(evaluator.evaluate(offsets));
    }
    
    @Test
    public void evaluate_alternatingExtremesFORWARDTest() {
        // 20->21->22
        List<List<TermWeightPosition>> offsets = new ArrayList<>();
        List<TermWeightPosition> offsetsA = asList(1, 10, 20);
        List<TermWeightPosition> offsetsB = asList(21, 24, 30);
        List<TermWeightPosition> offsetsC = asList(3, 8, 12, 19, 22);
        
        offsets.add(offsetsA);
        offsets.add(offsetsB);
        offsets.add(offsetsC);
        
        evaluator = new WrappedContentOrderedEvaluator(null, 1, new HashMap<>(), "a", "b", "c");
        
        Assert.assertTrue(evaluator.evaluate(offsets));
    }
    
    @Test
    public void evaluate_alternatingExtremesREVERSETest() {
        // 102->101->100
        List<List<TermWeightPosition>> offsets = new ArrayList<>();
        List<TermWeightPosition> offsetsA = asList(100, 200, 300, 500, 601);
        List<TermWeightPosition> offsetsB = asList(1, 5, 29, 87, 101);
        List<TermWeightPosition> offsetsC = asList(102, 400, 434);
        
        offsets.add(offsetsA);
        offsets.add(offsetsB);
        offsets.add(offsetsC);
        
        evaluator = new WrappedContentOrderedEvaluator(null, 1, new HashMap<>(), "a", "b", "c");
        
        Assert.assertTrue(evaluator.evaluate(offsets));
    }
    
    @Test
    public void evaluate_notEnoughOffsetsTest() {
        List<List<TermWeightPosition>> offsets = new ArrayList<>();
        evaluator = new WrappedContentOrderedEvaluator(null, 1, new HashMap<>(), "a", "b", "c");
        
        Assert.assertTrue(!evaluator.evaluate(offsets));
    }
    
    @Test
    public void evaluate_pruneAllTest() {
        List<List<TermWeightPosition>> offsets = new ArrayList<>();
        List<TermWeightPosition> offsetsA = asList(1);
        List<TermWeightPosition> offsetsB = asList(5);
        List<TermWeightPosition> offsetsC = asList(11);
        
        offsets.add(offsetsA);
        offsets.add(offsetsB);
        offsets.add(offsetsC);
        
        evaluator = new WrappedContentOrderedEvaluator(null, 1, new HashMap<>(), "a", "b", "c");
        
        Assert.assertTrue(!evaluator.evaluate(offsets));
    }
    
    @Test
    public void evaluate_simpleSuccessDistance1Test() {
        List<List<TermWeightPosition>> offsets = new ArrayList<>();
        List<TermWeightPosition> offsetsA = asList(1);
        List<TermWeightPosition> offsetsB = asList(2);
        List<TermWeightPosition> offsetsC = asList(3);
        
        offsets.add(offsetsA);
        offsets.add(offsetsB);
        offsets.add(offsetsC);
        
        evaluator = new WrappedContentOrderedEvaluator(null, 1, new HashMap<>(), "a", "b", "c");
        
        Assert.assertTrue(evaluator.evaluate(offsets));
    }
    
    @Test
    public void evaluate_simpleSuccessDistance3Test() {
        List<List<TermWeightPosition>> offsets = new ArrayList<>();
        List<TermWeightPosition> offsetsA = asList(1);
        List<TermWeightPosition> offsetsB = asList(2);
        List<TermWeightPosition> offsetsC = asList(3);
        
        offsets.add(offsetsA);
        offsets.add(offsetsB);
        offsets.add(offsetsC);
        
        evaluator = new WrappedContentOrderedEvaluator(null, 3, new HashMap<>(), "a", "b", "c");
        
        Assert.assertTrue(evaluator.evaluate(offsets));
    }
    
    @Test
    public void evaluate_simpleSuccessDistance3FailTest() {
        List<List<TermWeightPosition>> offsets = new ArrayList<>();
        List<TermWeightPosition> offsetsA = asList(1);
        List<TermWeightPosition> offsetsB = asList(5);
        List<TermWeightPosition> offsetsC = asList(7);
        
        offsets.add(offsetsA);
        offsets.add(offsetsB);
        offsets.add(offsetsC);
        
        evaluator = new WrappedContentOrderedEvaluator(null, 3, new HashMap<>(), "a", "b", "c");
        
        Assert.assertTrue(!evaluator.evaluate(offsets));
    }
    
    @Test
    public void evaluate_pruneTopTest() {
        List<List<TermWeightPosition>> offsets = new ArrayList<>();
        List<TermWeightPosition> offsetsA = asList(1, 10);
        List<TermWeightPosition> offsetsB = asList(10, 11);
        List<TermWeightPosition> offsetsC = asList(4, 12);
        
        offsets.add(offsetsA);
        offsets.add(offsetsB);
        offsets.add(offsetsC);
        
        evaluator = new WrappedContentOrderedEvaluator(null, 1, new HashMap<>(), "a", "b", "c");
        
        Assert.assertTrue(evaluator.evaluate(offsets));
    }
    
    @Test
    public void evaluate_pruneBottomTest() {
        List<List<TermWeightPosition>> offsets = new ArrayList<>();
        List<TermWeightPosition> offsetsA = asList(1, 3, 8);
        List<TermWeightPosition> offsetsB = asList(3, 4, 44);
        List<TermWeightPosition> offsetsC = asList(4, 5, 35);
        
        offsets.add(offsetsA);
        offsets.add(offsetsB);
        offsets.add(offsetsC);
        
        evaluator = new WrappedContentOrderedEvaluator(null, 1, new HashMap<>(), "a", "b", "c");
        
        Assert.assertTrue(evaluator.evaluate(offsets));
    }
    
    @Test
    public void evaluate_pruneTopAndBottomTest() {
        List<List<TermWeightPosition>> offsets = new ArrayList<>();
        List<TermWeightPosition> offsetsA = asList(3, 4, 5, 6);
        List<TermWeightPosition> offsetsB = asList(7, 9, 22);
        List<TermWeightPosition> offsetsC = asList(8, 11, 13);
        
        offsets.add(offsetsA);
        offsets.add(offsetsB);
        offsets.add(offsetsC);
        
        evaluator = new WrappedContentOrderedEvaluator(null, 1, new HashMap<>(), "a", "b", "c");
        
        Assert.assertTrue(evaluator.evaluate(offsets));
    }
    
    @Test
    public void evaluate_livePruneTest() {
        List<List<TermWeightPosition>> offsets = new ArrayList<>();
        List<TermWeightPosition> offsetsA = asList(1, 3);
        List<TermWeightPosition> offsetsB = asList(2, 4);
        List<TermWeightPosition> offsetsC = asList(5, 6);
        
        offsets.add(offsetsA);
        offsets.add(offsetsB);
        offsets.add(offsetsC);
        
        evaluator = new WrappedContentOrderedEvaluator(null, 2, new HashMap<>(), "a", "b", "c");
        
        Assert.assertTrue(evaluator.evaluate(offsets));
    }
    
    private static class WrappedContentOrderedEvaluator extends ContentOrderedEvaluator {
        public WrappedContentOrderedEvaluator(Set<String> fields, int distance, Map<String,TermFrequencyList> termOffsetMap, String... terms) {
            super(fields, distance, Float.MIN_VALUE, termOffsetMap, terms);
        }
        
        @Override
        protected boolean evaluate(List<List<TermWeightPosition>> offsets) {
            return super.evaluate(offsets);
        }
    }
}
