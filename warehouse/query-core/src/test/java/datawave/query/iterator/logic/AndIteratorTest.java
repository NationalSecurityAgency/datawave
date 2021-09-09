package datawave.query.iterator.logic;

import com.google.common.collect.Lists;
import datawave.query.iterator.NestedIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AndIteratorTest {
    
    @Before
    public void setup() {}
    
    @Test
    public void testSingleInclude() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));
        
        AndIterator iterator = new AndIterator(includes);
        iterator.initialize();
        
        Assert.assertFalse(iterator.isContextRequired());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("a", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("b", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testMultiInclude() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "e", "g"), false));
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d", "e", "f"), false));
        
        AndIterator iterator = new AndIterator(includes);
        iterator.initialize();
        
        Assert.assertFalse(iterator.isContextRequired());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("a", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("b", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("e", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testMultiIncludeContextRequired() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "c"), false));
        includes.add(getItr(Lists.newArrayList("b", "c", "d"), true));
        
        AndIterator iterator = new AndIterator(includes);
        iterator.initialize();
        
        Assert.assertFalse(iterator.isContextRequired());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("b", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("c", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testExcludeFirstMatch() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d", "e", "f"), false));
        
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getItr(Lists.newArrayList("a", "c", "e", "f"), false));
        
        AndIterator iterator = new AndIterator(includes, excludes);
        iterator.initialize();
        
        Assert.assertFalse(iterator.isContextRequired());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("b", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testExcludeSecondMatch() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d", "e", "f"), false));
        
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getItr(Lists.newArrayList("b", "c", "e", "f"), false));
        
        AndIterator iterator = new AndIterator(includes, excludes);
        iterator.initialize();
        
        Assert.assertFalse(iterator.isContextRequired());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("a", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testExcludeLastMatch() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "f"), false));
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d", "e", "f"), false));
        
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getItr(Lists.newArrayList("c", "e", "f"), false));
        
        AndIterator iterator = new AndIterator(includes, excludes);
        iterator.initialize();
        
        Assert.assertFalse(iterator.isContextRequired());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("a", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("b", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testExcludeNoMatch() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "f"), false));
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d", "e", "f"), false));
        
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getItr(Lists.newArrayList("g"), false));
        
        AndIterator iterator = new AndIterator(includes, excludes);
        iterator.initialize();
        
        Assert.assertFalse(iterator.isContextRequired());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("a", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("b", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("f", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testExcludeEmpty() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b", "f"), false));
        includes.add(getItr(Lists.newArrayList("a", "b", "c", "d", "e", "f"), false));
        
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getItr(Lists.newArrayList(), false));
        
        AndIterator iterator = new AndIterator(includes, excludes);
        iterator.initialize();
        
        Assert.assertFalse(iterator.isContextRequired());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("a", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("b", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("f", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testDeferred() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));
        includes.add(getItr(Lists.newArrayList("b"), true));
        
        AndIterator iterator = new AndIterator(includes);
        iterator.initialize();
        
        Assert.assertFalse(iterator.isContextRequired());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("b", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testNegatedDeferred() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), false));
        
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getItr(Lists.newArrayList("b"), true));
        
        AndIterator iterator = new AndIterator(includes, excludes);
        iterator.initialize();
        
        Assert.assertFalse(iterator.isContextRequired());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("a", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testDeferredChild() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("a", "b"), true));
        includes.add(getItr(Lists.newArrayList("b", "c"), true));
        
        AndIterator unsourcedIterator = new AndIterator(includes);
        
        includes = new HashSet<>();
        includes.add(unsourcedIterator);
        includes.add(getItr(Lists.newArrayList("c"), false));
        
        AndIterator iterator = new AndIterator(includes);
        iterator.initialize();
        
        Assert.assertFalse(iterator.isContextRequired());
        Assert.assertTrue(unsourcedIterator.isContextRequired());
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testDeferredNegatedChild() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        Set<NestedIterator<String>> excludes = new HashSet<>();
        excludes.add(getItr(Lists.newArrayList("a", "b"), true));
        excludes.add(getItr(Lists.newArrayList("b", "c"), true));
        
        AndIterator unsourcedIterator = new AndIterator(includes, excludes);
        
        includes = new HashSet<>();
        includes.add(unsourcedIterator);
        includes.add(getItr(Lists.newArrayList("c"), false));
        
        AndIterator iterator = new AndIterator(includes);
        iterator.initialize();
        
        Assert.assertFalse(iterator.isContextRequired());
        Assert.assertTrue(unsourcedIterator.isContextRequired());
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testContextWhenNotRequiredMoveAll() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("c", "d", "e"), false));
        includes.add(getItr(Lists.newArrayList("c", "d", "e"), false));
        
        AndIterator iterator = new AndIterator(includes);
        iterator.initialize();
        
        Assert.assertFalse(iterator.isContextRequired());
        iterator.setContext("e");
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("c", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("e", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testContextWhenNotRequiredShortCircuit() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("c", "s", "z"), false));
        includes.add(getItr(Lists.newArrayList("c", "z"), false));
        
        AndIterator iterator = new AndIterator(includes);
        iterator.initialize();
        
        Assert.assertFalse(iterator.isContextRequired());
        iterator.setContext("d");
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("c", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }
    
    @Test
    public void testContextWhenNotRequiredSkipLowestMoveLowest() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        includes.add(getItr(Lists.newArrayList("c", "s", "z"), false));
        includes.add(getItr(Lists.newArrayList("c", "z"), false));
        
        AndIterator iterator = new AndIterator(includes);
        iterator.initialize();
        
        Assert.assertFalse(iterator.isContextRequired());
        iterator.setContext("z");
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("c", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("z", iterator.next());
        Assert.assertFalse(iterator.hasNext());
    }
    
    private NegationFilterTest.Itr<String> getItr(List<String> source, boolean contextRequired) {
        return new NegationFilterTest.Itr<>(source, contextRequired);
    }
}
