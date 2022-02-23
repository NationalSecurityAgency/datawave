package datawave.query.iterator.logic;

import com.google.common.collect.Lists;
import datawave.query.iterator.NestedIterator;
import org.junit.Assert;
import org.junit.Test;

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
        
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("a", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("b", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testMultiInclude() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));
        includes.add(getItr(Lists.newArrayList("b", "d"), false));
        
        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();
        
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("a", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("b", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("d", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testDeferredInclude() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));
        includes.add(getItr(Lists.newArrayList("b", "d"), true));
        
        OrIterator iterator = new OrIterator(includes);
        iterator.setContext("d");
        iterator.initialize();
        
        Assert.assertTrue(iterator.hasNext());
        iterator.setContext("d");
        Assert.assertEquals("a", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        iterator.setContext("d");
        Assert.assertEquals("b", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        iterator.setContext("d");
        Assert.assertEquals("d", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testDeferredExclude() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("b", "d"), true));
        
        OrIterator iterator = new OrIterator(includes);
        iterator.setContext("b");
        iterator.initialize();
        
        Assert.assertTrue(iterator.hasNext());
        iterator.setContext("d");
        Assert.assertEquals("b", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        iterator.setContext("e");
        Assert.assertEquals("d", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testDeferredExcludeMove() {
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getItr(Lists.newArrayList("b", "d"), true));
        
        OrIterator iterator = new OrIterator(Collections.emptySet(), excludes);
        iterator.setContext("a");
        iterator.initialize();
        
        iterator.setContext("a");
        Assert.assertEquals("a", iterator.move("a"));
        
        iterator.setContext("b");
        Assert.assertEquals(null, iterator.move("b"));
        
        iterator.setContext("d");
        Assert.assertEquals(null, iterator.move("d"));
        
        iterator.setContext("e");
        Assert.assertEquals("e", iterator.move("e"));
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
        Assert.assertEquals("a", iterator.move("a"));
        
        // second iterator covers this one
        iterator.setContext("b");
        Assert.assertEquals("b", iterator.move("b"));
        
        // second iterator covers this one
        iterator.setContext("d");
        Assert.assertEquals("d", iterator.move("d"));
        
        iterator.setContext("e");
        Assert.assertEquals("e", iterator.move("e"));
        
        // first iterator covers this one
        iterator.setContext("f");
        Assert.assertEquals("f", iterator.move("f"));
        
        iterator.setContext("g");
        Assert.assertEquals("g", iterator.move("g"));
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
        Assert.assertEquals("a", iterator.move("a"));
        
        iterator.setContext("b");
        Assert.assertEquals("b", iterator.move("b"));
        
        iterator.setContext("c");
        Assert.assertEquals("d", iterator.move("c"));
        
        iterator.setContext("e");
        Assert.assertEquals(null, iterator.move("e"));
    }
    
    @Test
    public void testMoveNext() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));
        
        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();
        
        Assert.assertEquals("a", iterator.move("a"));
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("b", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testMoveNextPlusOne() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));
        
        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();
        
        Assert.assertEquals("b", iterator.move("b"));
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testMoveLast() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));
        
        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();
        
        Assert.assertEquals("c", iterator.move("c"));
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testMoveBeyondLast() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));
        
        OrIterator iterator = new OrIterator(includes);
        iterator.initialize();
        
        Assert.assertEquals(null, iterator.move("d"));
        Assert.assertFalse(iterator.hasNext());
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
        
        Assert.assertFalse(iterator.isContextRequired());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("a", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("b", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("c", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("d", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("e", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("f", iterator.next());
        Assert.assertFalse(iterator.hasNext());
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
        
        Assert.assertFalse(iterator.isContextRequired());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("b", iterator.move("a~"));
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("c", iterator.next());
        Assert.assertEquals("f", iterator.move("f"));
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test(expected = IllegalStateException.class)
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
        
        Assert.assertTrue(iterator.isContextRequired());
        Assert.assertTrue(childOr.isContextRequired());
        
        iterator.initialize();
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
        
        Assert.assertTrue(iterator.isContextRequired());
        Assert.assertTrue(childOr.isContextRequired());
        
        iterator.setContext("f");
        iterator.initialize();
        
        Assert.assertEquals(null, iterator.move("f"));
        Assert.assertFalse(iterator.hasNext());
        iterator.setContext("g");
        Assert.assertEquals("g", iterator.move("g"));
        Assert.assertFalse(iterator.hasNext());
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
        
        Assert.assertTrue(iterator.isContextRequired());
        Assert.assertTrue(childOr.isContextRequired());
        
        iterator.setContext("a");
        iterator.initialize();
        
        Assert.assertTrue(iterator.hasNext());
        iterator.setContext("b");
        Assert.assertEquals("a", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        iterator.setContext("c");
        Assert.assertEquals("b", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        iterator.setContext("d");
        Assert.assertEquals("c", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        
        iterator.setContext("f");
        Assert.assertEquals(null, iterator.move("f"));
        Assert.assertFalse(iterator.hasNext());
        iterator.setContext("g");
        Assert.assertEquals(null, iterator.move("g"));
        Assert.assertFalse(iterator.hasNext());
        iterator.setContext("h");
        Assert.assertEquals("h", iterator.move("h"));
        Assert.assertFalse(iterator.hasNext());
    }
    
    private NegationFilterTest.Itr<String> getItr(List<String> source, boolean contextRequired) {
        return new NegationFilterTest.Itr<>(source, contextRequired);
    }
}
