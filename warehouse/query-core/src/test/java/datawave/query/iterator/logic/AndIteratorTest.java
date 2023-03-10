package datawave.query.iterator.logic;

import com.google.common.collect.Lists;
import datawave.query.iterator.NestedIterator;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AndIteratorTest {
    
    @Before
    public void setup() {}
    
    @Test
    public void testSingleInclude() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a", "b", "c"));
        
        AndIterator<String> iter = new AndIterator<>(includes);
        assertHits(iter, "a", "b", "c");
    }
    
    // A && B, both hit on 'a,b,c'
    @Test
    public void testIncludes_allHits() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a", "b", "c"));
        includes.add(getIter("a", "b", "c"));
        
        AndIterator<String> iter = new AndIterator<>(includes);
        assertHits(iter, "a", "b", "c");
    }
    
    // A && B miss on 'a,b,c,d' and hit on 'e,f'
    @Test
    public void testIncludes_hitsAtEnd() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a", "c", "e", "f"));
        includes.add(getIter("b", "d", "e", "f"));
        
        AndIterator<String> iter = new AndIterator<>(includes);
        assertHits(iter, "e", "f");
    }
    
    // A && B, both hit on 'a,b' and miss 'c,d,e,f'
    @Test
    public void testIncludes_hitsUpFront() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a", "b", "c", "e"));
        includes.add(getIter("a", "b", "d", "f"));
        
        AndIterator<String> iter = new AndIterator<>(includes);
        assertHits(iter, "a", "b");
    }
    
    // A && !B
    @Test
    public void testIncludeExclude_noneExcluded() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a", "b", "c"));
        
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getIter("x", "y", "z"));
        
        AndIterator<String> iter = new AndIterator<>(includes, excludes);
        assertHits(iter, "a", "b", "c");
    }
    
    // A && !B, hits on 'a,d,f', no hits on 'b,c,e'
    @Test
    public void testIncludeExclude_someExcluded() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a", "b", "c", "d", "e", "f"));
        
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getIter("b", "c", "e"));
        
        AndIterator<String> iter = new AndIterator<>(includes, excludes);
        assertHits(iter, "a", "d", "f");
    }
    
    // A && !B, all hits excluded
    @Test
    public void testIncludeExclude_allExcluded() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a", "b", "c"));
        
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getIter("a", "b", "c"));
        
        AndIterator<String> iter = new AndIterator<>(includes, excludes);
        assertNoHits(iter);
    }
    
    // A && (B && C)
    @Test
    public void testIncludeWithNestedIncludes_allHits() {
        Set<NestedIterator<String>> leftIncludes = new HashSet<>();
        leftIncludes.add(getIter("a", "b", "c", "d"));
        leftIncludes.add(getIter("a", "b", "c", "d"));
        AndIterator<String> leftAnd = new AndIterator<>(leftIncludes);
        
        Set<NestedIterator<String>> rightIncludes = new HashSet<>();
        rightIncludes.add(getIter("a", "b", "c", "d"));
        rightIncludes.add(getIter("a", "b", "c", "d"));
        AndIterator<String> rightAnd = new AndIterator<>(rightIncludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(leftAnd);
        includes.add(rightAnd);
        
        AndIterator<String> iter = new AndIterator<>(includes);
        assertHits(iter, "a", "b", "c", "d");
    }
    
    // A && (!B && !C) => A && !(B || C)
    @Test
    public void testSimpleIncludeWithNestedExclude_fullExclusion() {
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getIter("a"));
        excludes.add(getIter("a"));
        AndIterator<String> rightAnd = new AndIterator<>(Collections.emptySet(), excludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a"));
        includes.add(rightAnd);
        
        AndIterator<String> iter = new AndIterator<>(includes);
        assertNoHits(iter);
    }
    
    // A && (!B && !C) => A && !(B || C)
    @Test
    public void testSimpleIncludeWithNestedExclude_partialExclusion() {
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getIter("a"));
        excludes.add(getIter("b"));
        AndIterator<String> rightAnd = new AndIterator<>(Collections.emptySet(), excludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a"));
        includes.add(rightAnd);
        
        AndIterator<String> iter = new AndIterator<>(includes);
        assertNoHits(iter);
    }
    
    // A && (!B && !C) => A && !(B || C)
    @Test
    public void testSimpleIncludeWithNestedExclude_noExclusion() {
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getIter("b"));
        excludes.add(getIter("b"));
        AndIterator<String> rightAnd = new AndIterator<>(Collections.emptySet(), excludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a"));
        includes.add(rightAnd);
        
        AndIterator<String> iter = new AndIterator<>(includes);
        assertHits(iter, "a");
    }
    
    // A && (!B && !C) => A && !(B || C)
    @Test
    public void testIncludeWithNestedExcludes_fullExclusion() {
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getIter("a", "b", "c", "d"));
        excludes.add(getIter("a", "b", "c", "d"));
        AndIterator<String> rightAnd = new AndIterator<>(Collections.emptySet(), excludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a", "b", "c", "d"));
        includes.add(rightAnd);
        
        AndIterator<String> iter = new AndIterator<>(includes);
        assertNoHits(iter);
    }
    
    // A && (!B && !C) => A && !(B || C)
    @Test
    public void testIncludeWithNestedExcludes_partialExclusion() {
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getIter("c", "d"));
        excludes.add(getIter("b", "d"));
        AndIterator<String> rightAnd = new AndIterator<>(Collections.emptySet(), excludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a", "b", "c", "d"));
        includes.add(rightAnd);
        
        AndIterator<String> iter = new AndIterator<>(includes);
        assertHits(iter, "a");
    }
    
    // A && (!B && !C) => A && !(B || C)
    @Test
    public void testIncludeWithNestedExcludes_noExclusion() {
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getIter("t", "u", "v"));
        excludes.add(getIter("x", "y", "z"));
        AndIterator<String> rightAnd = new AndIterator<>(Collections.emptySet(), excludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a", "b", "c", "d"));
        includes.add(rightAnd);
        
        AndIterator<String> iter = new AndIterator<>(includes);
        assertHits(iter, "a", "b", "c", "d");
    }
    
    // (A && B) && (C && D), hits on 'b,c'
    @Test
    public void testDoubleNestedIncludes_overlappingHits() {
        Set<NestedIterator<String>> leftIncludes = new HashSet<>();
        leftIncludes.add(getIter("a", "b", "c"));
        leftIncludes.add(getIter("b", "c", "d"));
        AndIterator<String> leftAnd = new AndIterator<>(leftIncludes);
        
        Set<NestedIterator<String>> rightIncludes = new HashSet<>();
        rightIncludes.add(getIter("a", "b", "c"));
        rightIncludes.add(getIter("b", "c", "d"));
        AndIterator<String> rightAnd = new AndIterator<>(rightIncludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(leftAnd);
        includes.add(rightAnd);
        
        AndIterator<String> iter = new AndIterator<>(includes);
        assertHits(iter, "b", "c");
    }
    
    // (A && B) && (C && D) when each iterator contributes to a miss
    @Test
    public void testDoubleNestedIncludes_noHits() {
        Set<NestedIterator<String>> leftIncludes = new HashSet<>();
        leftIncludes.add(getIter("a", "b", "c"));
        leftIncludes.add(getIter("b", "c", "d"));
        AndIterator<String> leftAnd = new AndIterator<>(leftIncludes);
        
        Set<NestedIterator<String>> rightIncludes = new HashSet<>();
        rightIncludes.add(getIter("c", "d", "e"));
        rightIncludes.add(getIter("d", "e", "f"));
        AndIterator<String> rightAnd = new AndIterator<>(rightIncludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(leftAnd);
        includes.add(rightAnd);
        
        AndIterator<String> iter = new AndIterator<>(includes);
        assertNoHits(iter);
    }
    
    // A && B, hits on 'a,b,e' with trailing non-hit elements.
    @Test
    public void testIncludes_moreOverlappingHits() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a", "b", "e", "g"));
        includes.add(getIter("a", "b", "c", "d", "e", "f"));
        
        AndIterator<String> iter = new AndIterator<>(includes);
        assertHits(iter, "a", "b", "e");
    }
    
    // (A && B) && !C
    // 'a' is excluded, 'b' is common hit. Others are either excluded or do not intersect.
    @Test
    public void testExcludeFirstMatch() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a", "b"));
        includes.add(getIter("a", "b", "c", "d", "e", "f"));
        
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getIter("a", "c", "e", "f"));
        
        AndIterator<String> iterator = new AndIterator<>(includes, excludes);
        assertHits(iterator, "b");
    }
    
    @Test
    public void testExcludeSecondMatch() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a", "b"));
        includes.add(getIter("a", "b", "c", "d", "e", "f"));
        
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getIter("b", "c", "e", "f"));
        
        AndIterator<String> iterator = new AndIterator<>(includes, excludes);
        assertHits(iterator, "a");
    }
    
    @Test
    public void testExcludeLastMatch() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a", "b", "f"));
        includes.add(getIter("a", "b", "c", "d", "e", "f"));
        
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getIter("c", "e", "f"));
        
        AndIterator<String> iterator = new AndIterator<>(includes, excludes);
        assertHits(iterator, "a", "b");
    }
    
    @Test
    public void testExcludeNoMatch() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a", "b", "f"));
        includes.add(getIter("a", "b", "c", "d", "e", "f"));
        
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getIter("g"));
        
        AndIterator<String> iterator = new AndIterator<>(includes, excludes);
        assertHits(iterator, "a", "b", "f");
    }
    
    @Test
    public void testExcludeEmpty() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a", "b", "f"));
        includes.add(getIter("a", "b", "c", "d", "e", "f"));
        
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getIter());
        
        AndIterator<String> iterator = new AndIterator<>(includes, excludes);
        assertHits(iterator, "a", "b", "f");
    }
    
    @Test
    public void testDeferred() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a", "b"));
        includes.add(getIter("b"));
        
        AndIterator<String> iterator = new AndIterator<>(includes);
        assertHits(iterator, "b");
    }
    
    @Test
    public void testNegatedDeferred() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a", "b"));
        
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getIter("b"));
        
        AndIterator<String> iterator = new AndIterator<>(includes, excludes);
        assertHits(iterator, "a");
    }
    
    @Test
    public void testDeferredChild() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("a", "b"));
        includes.add(getIter("b", "c"));
        
        AndIterator<String> unsourcedIterator = new AndIterator<>(includes);
        
        includes = new HashSet<>();
        includes.add(unsourcedIterator);
        includes.add(getIter("c"));
        
        AndIterator<String> iterator = new AndIterator<>(includes);
        assertNoHits(iterator);
    }
    
    // A && (!B && !C) ==> A && !(B || C)
    @Test
    public void testDeferredNegatedChild() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getIter("a", "b"));
        excludes.add(getIter("b", "c"));
        
        AndIterator<String> unsourcedIterator = new AndIterator<>(includes, excludes);
        
        includes = new HashSet<>();
        includes.add(unsourcedIterator);
        includes.add(getIter("c"));
        
        AndIterator<String> iterator = new AndIterator<>(includes);
        assertNoHits(iterator);
    }
    
    @Test
    public void testContextWhenNotRequiredMoveAll() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("c", "d", "e"));
        includes.add(getIter("c", "d", "e"));
        
        AndIterator<String> iterator = new AndIterator<>(includes);
        assertHits(iterator, "c", "d", "e");
    }
    
    @Test
    public void testContextWhenNotRequiredShortCircuit() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("c", "s", "z"));
        includes.add(getIter("c", "z"));
        
        AndIterator<String> iterator = new AndIterator<>(includes);
        assertHits(iterator, "c", "z");
    }
    
    @Test
    public void testContextWhenNotRequiredSkipLowestMoveLowest() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getIter("c", "s", "z"));
        includes.add(getIter("c", "z"));
        
        AndIterator<String> iterator = new AndIterator<>(includes);
        assertHits(iterator, "c", "z");
    }
    
    private void assertHits(NestedIterator<String> iter, String... hits) {
        List<String> listHits = Lists.newArrayList(hits);
        assertHits(iter, listHits);
    }
    
    private void assertHits(NestedIterator<String> iter, List<String> hits) {
        iter.initialize();
        for (String hit : hits) {
            assertTrue(iter.hasNext());
            assertEquals(hit, iter.next());
        }
        assertFalse(iter.hasNext());
    }
    
    private void assertNoHits(NestedIterator<String> iter) {
        iter.initialize();
        assertFalse(iter.hasNext());
        assertNull(iter.next());
    }
    
    private NegationFilterTest.Itr<String> getIter(String... elements) {
        List<String> list = Lists.newArrayList(elements);
        return getIter(list, false);
    }
    
    private NegationFilterTest.Itr<String> getIter(List<String> source, boolean contextRequired) {
        return new NegationFilterTest.Itr<>(source, contextRequired);
    }
}
