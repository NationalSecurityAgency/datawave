package datawave.query.iterator.logic;

import com.google.common.collect.Lists;
import datawave.query.iterator.NestedIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AndOrIteratorTest {
    // X AND (!Y OR !Z)
    @Test
    public void testAndNegatedOr() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();
        
        childExcludes.add(getItr(Lists.newArrayList("b", "c")));
        childExcludes.add(getItr(Lists.newArrayList("a", "b")));
        
        OrIterator childOr = new OrIterator(childIncludes, childExcludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(childOr);
        includes.add(getItr(Lists.newArrayList("a", "b", "c")));
        
        NestedIterator iterator = new AndIterator(includes);
        iterator.initialize();
        
        Assertions.assertFalse(iterator.isContextRequired());
        Assertions.assertTrue(childOr.isContextRequired());
        
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("a", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("c", iterator.next());
        Assertions.assertFalse(iterator.hasNext());
    }
    
    // X AND (Y OR !Z)
    @Test
    public void testAndMixedOr() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();
        
        childIncludes.add(getItr(Lists.newArrayList("b", "c")));
        childExcludes.add(getItr(Lists.newArrayList("a", "b")));
        
        OrIterator childOr = new OrIterator(childIncludes, childExcludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(childOr);
        includes.add(getItr(Lists.newArrayList("a", "b", "c")));
        
        NestedIterator iterator = new AndIterator(includes);
        iterator.initialize();
        
        Assertions.assertFalse(iterator.isContextRequired());
        Assertions.assertTrue(childOr.isContextRequired());
        
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("b", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("c", iterator.next());
        Assertions.assertFalse(iterator.hasNext());
    }
    
    // X AND (!Y AND !Z)
    @Test
    public void testAndNegatedAnd() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();
        
        childExcludes.add(getItr(Lists.newArrayList("b", "c")));
        childExcludes.add(getItr(Lists.newArrayList("a", "b")));
        
        NestedIterator child = new AndIterator(childIncludes, childExcludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(child);
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d")));
        
        NestedIterator iterator = new AndIterator(includes);
        iterator.initialize();
        
        Assertions.assertFalse(iterator.isContextRequired());
        Assertions.assertTrue(child.isContextRequired());
        
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("d", iterator.next());
        Assertions.assertFalse(iterator.hasNext());
    }
    
    // X OR !Y
    @Test
    public void testDeferredOrMissingContext() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        Set<NestedIterator<String>> excludes = new HashSet<>();
        
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d")));
        excludes.add(getItr(Lists.newArrayList("a")));
        
        NestedIterator iterator = new OrIterator(includes, excludes);
        Assertions.assertThrows(IllegalStateException.class, iterator::initialize);
    }
    
    // !X OR !Y
    @Test
    public void testDeferredOr() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        Set<NestedIterator<String>> excludes = new HashSet<>();
        
        excludes.add(getItr(Lists.newArrayList("a", "b", "c", "d")));
        excludes.add(getItr(Lists.newArrayList("a")));
        
        NestedIterator iterator = new OrIterator(includes, excludes);
        Assertions.assertThrows(IllegalStateException.class, iterator::initialize);
    }
    
    // !X AND !Y
    @Test
    public void testDeferredAnd() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        Set<NestedIterator<String>> excludes = new HashSet<>();
        
        excludes.add(getItr(Lists.newArrayList("a", "b", "c", "d")));
        excludes.add(getItr(Lists.newArrayList("a")));
        
        NestedIterator iterator = new AndIterator(includes, excludes);
        Assertions.assertThrows(IllegalStateException.class, iterator::initialize);
    }
    
    // X AND !Y AND (!Z OR !A)
    @Test
    public void testNegatedNonDeferredInteractionWithDeferred() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();
        
        childExcludes.add(getItr(Lists.newArrayList("b", "d")));
        childExcludes.add(getItr(Lists.newArrayList("c")));
        
        NestedIterator<String> child = new OrIterator<>(childIncludes, childExcludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        Set<NestedIterator<String>> excludes = new HashSet<>();
        
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d")));
        excludes.add(getItr(Lists.newArrayList("a")));
        includes.add(child);
        
        NestedIterator iterator = new AndIterator(includes, excludes);
        iterator.initialize();
        
        Assertions.assertTrue(child.isContextRequired());
        Assertions.assertFalse(iterator.isContextRequired());
        
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("b", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("c", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("d", iterator.next());
        Assertions.assertFalse(iterator.hasNext());
    }
    
    // X AND (Y OR !Y)
    @Test
    public void testAndAlwaysTrue() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();
        
        childIncludes.add(getItr(Lists.newArrayList("b", "c")));
        childExcludes.add(getItr(Lists.newArrayList("b", "c")));
        
        NestedIterator child = new OrIterator(childIncludes, childExcludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(child);
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d")));
        
        NestedIterator iterator = new AndIterator(includes);
        iterator.initialize();
        
        Assertions.assertFalse(iterator.isContextRequired());
        Assertions.assertTrue(child.isContextRequired());
        
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("a", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("b", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("c", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("d", iterator.next());
        Assertions.assertFalse(iterator.hasNext());
    }
    
    // X AND (Y AND !Y)
    @Test
    public void testAndNeverTrue() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();
        
        childIncludes.add(getItr(Lists.newArrayList("b", "c")));
        childExcludes.add(getItr(Lists.newArrayList("b", "c")));
        
        NestedIterator child = new AndIterator(childIncludes, childExcludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(child);
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d")));
        
        NestedIterator iterator = new AndIterator(includes);
        iterator.initialize();
        
        Assertions.assertFalse(iterator.isContextRequired());
        Assertions.assertFalse(child.isContextRequired());
        
        Assertions.assertFalse(iterator.hasNext());
    }
    
    // X OR (Y AND Z)
    @Test
    public void testOrAnd() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();
        
        childIncludes.add(getItr(Lists.newArrayList("a", "c", "f")));
        childIncludes.add(getItr(Lists.newArrayList("b", "c", "f")));
        
        NestedIterator child = new AndIterator(childIncludes, childExcludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(child);
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d")));
        
        NestedIterator iterator = new OrIterator(includes);
        iterator.initialize();
        
        Assertions.assertFalse(iterator.isContextRequired());
        Assertions.assertFalse(child.isContextRequired());
        
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("a", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("b", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("c", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("d", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("f", iterator.next());
        Assertions.assertFalse(iterator.hasNext());
    }
    
    // X OR (Y AND !Z)
    @Test
    public void testOrMixedAnd() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();
        
        childIncludes.add(getItr(Lists.newArrayList("a", "b")));
        childExcludes.add(getItr(Lists.newArrayList("b", "f")));
        
        NestedIterator child = new AndIterator(childIncludes, childExcludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(child);
        includes.add(getItr(Lists.newArrayList("c", "d")));
        
        NestedIterator iterator = new OrIterator(includes);
        iterator.initialize();
        
        Assertions.assertFalse(iterator.isContextRequired());
        Assertions.assertFalse(child.isContextRequired());
        
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("a", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("c", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("d", iterator.next());
        Assertions.assertFalse(iterator.hasNext());
    }
    
    // X AND ((!Y OR Z) OR W)
    @Test
    public void testDeferredOrWithAccept() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();
        
        childExcludes.add(getItr(Lists.newArrayList("a", "b", "c", "d")));
        childIncludes.add(getItr(Lists.newArrayList("b", "c")));
        
        NestedIterator child1 = new OrIterator(childIncludes, childExcludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(child1);
        includes.add(getItr(Lists.newArrayList("c", "d")));
        
        NestedIterator child2 = new OrIterator(includes);
        
        includes = new HashSet<>();
        includes.add(child2);
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d")));
        
        NestedIterator iterator = new AndIterator(includes);
        iterator.initialize();
        
        Assertions.assertFalse(iterator.isContextRequired());
        Assertions.assertTrue(child2.isContextRequired());
        Assertions.assertTrue(child1.isContextRequired());
        
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("b", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("c", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("d", iterator.next());
        Assertions.assertFalse(iterator.hasNext());
    }
    
    // X AND (Y OR !Z)
    @Test
    public void testDeferredOrAdvanceInMove() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();
        
        childIncludes.add(getItr(Lists.newArrayList("b", "b1", "b2", "b3", "c")));
        childExcludes.add(getItr(Lists.newArrayList("a", "b")));
        
        OrIterator childOr = new OrIterator(childIncludes, childExcludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(childOr);
        includes.add(getItr(Lists.newArrayList("a", "c")));
        
        NestedIterator iterator = new AndIterator(includes);
        iterator.initialize();
        
        Assertions.assertFalse(iterator.isContextRequired());
        Assertions.assertTrue(childOr.isContextRequired());
        
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("c", iterator.next());
        Assertions.assertFalse(iterator.hasNext());
    }
    
    // X AND (Y OR !Z)
    @Test
    public void testDeferredNoMatches() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();
        
        childIncludes.add(getItr(Lists.newArrayList("b", "b1", "b2", "b3", "c")));
        childExcludes.add(getItr(Lists.newArrayList("z", "z1")));
        
        OrIterator childOr = new OrIterator(childIncludes, childExcludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(childOr);
        includes.add(getItr(Lists.newArrayList("z")));
        
        NestedIterator iterator = new AndIterator(includes);
        iterator.initialize();
        
        Assertions.assertFalse(iterator.isContextRequired());
        Assertions.assertTrue(childOr.isContextRequired());
        
        Assertions.assertFalse(iterator.hasNext());
    }
    
    // X AND !(Y OR !Z)
    @Test
    public void testNegatedDeferred() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();
        
        childIncludes.add(getItr(Lists.newArrayList("b", "b1", "b2", "b3", "c")));
        childExcludes.add(getItr(Lists.newArrayList("z1", "z2")));
        
        OrIterator childOr = new OrIterator(childIncludes, childExcludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        Set<NestedIterator<String>> excludes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("z")));
        excludes.add(childOr);
        
        NestedIterator iterator = new AndIterator(includes, excludes);
        iterator.initialize();
        
        Assertions.assertFalse(iterator.isContextRequired());
        Assertions.assertTrue(childOr.isContextRequired());
        
        Assertions.assertFalse(iterator.hasNext());
    }
    
    private NegationFilterTest.Itr<String> getItr(List<String> source) {
        return new NegationFilterTest.Itr<>(source, false);
    }
}
