package datawave.query.iterator.logic;

import datawave.query.iterator.NestedIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ArrayIteratorTest implements NestedQueryIteratorTest {
    
    ArrayIterator<String> iterator;
    
    @Before
    public void setup() {
        Set<NestedIterator<String>> includes = new HashSet<>();
        String[] stringArray = {"a", "b", "c", "d", "e", "f"};
        
        iterator = new ArrayIterator(stringArray);
        iterator.initialize();
    }
    
    @Test
    public void testArrayIterator() {
        
        Assert.assertFalse(iterator.isContextRequired());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("a", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("b", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals("c", iterator.next());
        Assert.assertTrue(iterator.hasNext());
        
    }
    
    @Test
    @Override
    public void testElementInLeaves() {
        
    }
    
    @Test
    @Override
    public void testElementInChildren() {
        
    }
    
    @Test
    @Override
    public void testEmptyRange() {
        
    }
    
    @Test
    @Override
    public void testScanMinorRange() {
        
    }
    
    @Test
    @Override
    public void testScanMinorRangeTLD() {
        
    }
    
    @Test
    @Override
    public void testScanPartialRanges() {
        
    }
    
    @Test
    @Override
    public void testScanPartialRangesTLD() {
        
    }
    
    @Test
    @Override
    public void testScanFullRange() {
        
    }
    
    @Test
    @Override
    public void testScanFullRangeTLD() {
        
    }
    
    @Test
    @Override
    public void testEndingFieldMismatch() {
        
    }
    
    @Test
    @Override
    public void testScanFullRangeExclusive() {
        
    }
    
    @Test
    @Override
    public void testScanFullRangeExclusiveTLD() {
        
    }
    
    @Test
    @Override
    public void testScanFullRangeExclusiveEventDataQueryExpressionFilter() {
        
    }
}
