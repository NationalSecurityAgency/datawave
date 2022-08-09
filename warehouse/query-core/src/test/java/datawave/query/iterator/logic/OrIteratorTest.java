package datawave.query.iterator.logic;

import com.google.common.collect.Lists;
import datawave.query.iterator.NestedIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OrIteratorTest {
    @Test
    public void testInclude() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));
        
        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();
        
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("a", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("b", iterator.next());
        Assertions.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testMultiInclude() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));
        includes.add(getItr(Lists.newArrayList("b", "d"), false));
        
        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();
        
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("a", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("b", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("d", iterator.next());
        Assertions.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testDeferredInclude() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));
        includes.add(getItr(Lists.newArrayList("b", "d"), true));
        
        OrIterator iterator = new OrIterator(includes);
        iterator.setContext("d");
        iterator.initialize();
        
        Assertions.assertTrue(iterator.hasNext());
        iterator.setContext("d");
        Assertions.assertEquals("a", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        iterator.setContext("d");
        Assertions.assertEquals("b", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        iterator.setContext("d");
        Assertions.assertEquals("d", iterator.next());
        Assertions.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testDeferredExclude() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("b", "d"), true));
        
        OrIterator iterator = new OrIterator(includes);
        iterator.setContext("b");
        iterator.initialize();
        
        Assertions.assertTrue(iterator.hasNext());
        iterator.setContext("d");
        Assertions.assertEquals("b", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        iterator.setContext("e");
        Assertions.assertEquals("d", iterator.next());
        Assertions.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testDeferredExcludeMove() {
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getItr(Lists.newArrayList("b", "d"), true));
        
        OrIterator iterator = new OrIterator(Collections.emptySet(), excludes);
        iterator.setContext("a");
        iterator.initialize();
        
        iterator.setContext("a");
        Assertions.assertEquals("a", iterator.move("a"));
        
        iterator.setContext("b");
        Assertions.assertNull(iterator.move("b"));
        
        iterator.setContext("d");
        Assertions.assertNull(iterator.move("d"));
        
        iterator.setContext("e");
        Assertions.assertEquals("e", iterator.move("e"));
    }
    
    @Test
    public void testMultipleDeferredExcludeMove() {
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getItr(Lists.newArrayList("b", "d"), true));
        excludes.add(getItr(Lists.newArrayList("f"), true));
        
        OrIterator iterator = new OrIterator(Collections.emptySet(), excludes);
        iterator.setContext("a");
        iterator.initialize();
        
        iterator.setContext("a");
        Assertions.assertEquals("a", iterator.move("a"));
        
        // second iterator covers this one
        iterator.setContext("b");
        Assertions.assertEquals("b", iterator.move("b"));
        
        // second iterator covers this one
        iterator.setContext("d");
        Assertions.assertEquals("d", iterator.move("d"));
        
        iterator.setContext("e");
        Assertions.assertEquals("e", iterator.move("e"));
        
        // first iterator covers this one
        iterator.setContext("f");
        Assertions.assertEquals("f", iterator.move("f"));
        
        iterator.setContext("g");
        Assertions.assertEquals("g", iterator.move("g"));
    }
    
    @Test
    public void testFirstDeferredInclude() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), true));
        includes.add(getItr(Lists.newArrayList("b", "d"), false));
        
        OrIterator iterator = new OrIterator(includes);
        iterator.setContext("a");
        iterator.initialize();
        
        iterator.setContext("a");
        Assertions.assertEquals("a", iterator.move("a"));
        
        iterator.setContext("b");
        Assertions.assertEquals("b", iterator.move("b"));
        
        iterator.setContext("c");
        Assertions.assertEquals("d", iterator.move("c"));
        
        iterator.setContext("e");
        Assertions.assertNull(iterator.move("e"));
    }
    
    @Test
    public void testMoveNext() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));
        
        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();
        
        Assertions.assertEquals("a", iterator.move("a"));
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("b", iterator.next());
        Assertions.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testMoveNextPlusOne() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));
        
        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();
        
        Assertions.assertEquals("b", iterator.move("b"));
        Assertions.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testMoveLast() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));
        
        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();
        
        Assertions.assertEquals("c", iterator.move("c"));
        Assertions.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testMoveBeyondLast() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));
        
        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();
        
        Assertions.assertNull(iterator.move("d"));
        Assertions.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testNestedOr() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));
        Set<NestedIterator<String>> includes2 = new HashSet<>();
        includes2.add(getItr(Lists.newArrayList("a", "d", "e"), false));
        includes2.add(getItr(Lists.newArrayList("a", "d", "f"), false));
        includes.add(new OrIterator<>(includes2));
        
        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();
        
        Assertions.assertFalse(iterator.isContextRequired());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("a", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("b", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("c", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("d", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("e", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("f", iterator.next());
        Assertions.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testNestedOrMove() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));
        Set<NestedIterator<String>> includes2 = new HashSet<>();
        includes2.add(getItr(Lists.newArrayList("a", "d", "e"), false));
        includes2.add(getItr(Lists.newArrayList("a", "d", "f"), false));
        includes.add(new OrIterator<>(includes2));
        
        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();
        
        Assertions.assertFalse(iterator.isContextRequired());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("b", iterator.move("a~"));
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertEquals("c", iterator.next());
        Assertions.assertEquals("f", iterator.move("f"));
        Assertions.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testNestedOrMoveNegatedNoContext() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();
        
        childIncludes.add(getItr(Lists.newArrayList("a", "d", "e"), false));
        childExcludes.add(getItr(Lists.newArrayList("f"), false));
        
        OrIterator childOr = new OrIterator(childIncludes, childExcludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(childOr);
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));
        
        OrIterator iterator = new OrIterator(includes);
        
        Assertions.assertTrue(iterator.isContextRequired());
        Assertions.assertTrue(childOr.isContextRequired());
        Assertions.assertThrows(IllegalStateException.class, iterator::initialize);
    }
    
    @Test
    public void testNestedOrMoveNegatedTest() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();
        
        childExcludes.add(getItr(Lists.newArrayList("f"), false));
        
        OrIterator childOr = new OrIterator(childIncludes, childExcludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(childOr);
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));
        
        OrIterator iterator = new OrIterator(includes);
        
        Assertions.assertTrue(iterator.isContextRequired());
        Assertions.assertTrue(childOr.isContextRequired());
        
        iterator.setContext("f");
        iterator.initialize();
        
        Assertions.assertNull(iterator.move("f"));
        Assertions.assertFalse(iterator.hasNext());
        iterator.setContext("g");
        Assertions.assertEquals("g", iterator.move("g"));
        Assertions.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testNestedOrMoveNegatedMultiTermTest() {
        Set<NestedIterator<String>> childIncludes = new HashSet<>();
        Set<NestedIterator<String>> childExcludes = new HashSet<>();
        
        childExcludes.add(getItr(Lists.newArrayList("f", "g"), false));
        
        OrIterator childOr = new OrIterator(childIncludes, childExcludes);
        
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(childOr);
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));
        
        OrIterator iterator = new OrIterator(includes);
        
        Assertions.assertTrue(iterator.isContextRequired());
        Assertions.assertTrue(childOr.isContextRequired());
        
        iterator.setContext("a");
        iterator.initialize();
        
        Assertions.assertTrue(iterator.hasNext());
        iterator.setContext("b");
        Assertions.assertEquals("a", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        iterator.setContext("c");
        Assertions.assertEquals("b", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        iterator.setContext("d");
        Assertions.assertEquals("c", iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        
        iterator.setContext("f");
        Assertions.assertNull(iterator.move("f"));
        Assertions.assertFalse(iterator.hasNext());
        iterator.setContext("g");
        Assertions.assertNull(iterator.move("g"));
        Assertions.assertFalse(iterator.hasNext());
        iterator.setContext("h");
        Assertions.assertEquals("h", iterator.move("h"));
        Assertions.assertFalse(iterator.hasNext());
    }
    
    private NegationFilterTest.Itr<String> getItr(List<String> source, boolean contextRequired) {
        return new NegationFilterTest.Itr<>(source, contextRequired);
    }
}
