package datawave.query.index.lookup;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ShardEqualityTest {
    
    @Test
    public void testMatchesWithin() {
        assertTrue(ShardEquality.matchesWithin("20190314", "20190314"));
        assertTrue(ShardEquality.matchesWithin("20190314", "20190314_0"));
        assertTrue(ShardEquality.matchesWithin("20190314", "20190314_15"));
        
        assertFalse(ShardEquality.matchesWithin("20190314_0", "20190314"));
        assertFalse(ShardEquality.matchesWithin("20190314_15", "20190314"));
        
        assertFalse(ShardEquality.matchesWithin("20190314", "20190315"));
        assertFalse(ShardEquality.matchesWithin("20190314", "20190315_0"));
        assertFalse(ShardEquality.matchesWithin("20190314", "20200202"));
        assertFalse(ShardEquality.matchesWithin("20190314", "20200202_22"));
    }
    
    @Test
    public void testLessThan() {
        // Shards
        assertTrue(ShardEquality.lessThan("20190314", "20190314_0"));
        assertTrue(ShardEquality.lessThan("20190314_0", "20190314_1"));
        assertTrue(ShardEquality.lessThan("20190314_1", "20190314_11"));
        assertTrue(ShardEquality.lessThan("20190314_1", "20190314_2"));
        
        // Days
        assertTrue(ShardEquality.lessThan("20190314", "20190315_0"));
        assertTrue(ShardEquality.lessThan("20190314_0", "20190315_1"));
        assertTrue(ShardEquality.lessThan("20190314_1", "20190315_11"));
        assertTrue(ShardEquality.lessThan("20190314_1", "20190315_2"));
    }
    
    @Test
    public void testAfter() {
        // Shards
        assertTrue(ShardEquality.greaterThan("20190314_0", "20190314"));
        assertTrue(ShardEquality.greaterThan("20190314_1", "20190314_0"));
        assertTrue(ShardEquality.greaterThan("20190314_11", "20190314_1"));
        assertTrue(ShardEquality.greaterThan("20190314_2", "20190314_1"));
        
        // Days
        assertTrue(ShardEquality.greaterThan("20190315_0", "20190314"));
        assertTrue(ShardEquality.greaterThan("20190315_1", "20190314_0"));
        assertTrue(ShardEquality.greaterThan("20190315_11", "20190314_1"));
        assertTrue(ShardEquality.greaterThan("20190315_2", "20190314_1"));
    }
    
    @Test
    public void testGreaterThan() {
        assertTrue(ShardEquality.greaterThanOrEqual("20190314", "20190314"));
        assertTrue(ShardEquality.greaterThanOrEqual("20190314", "20190301"));
        assertFalse(ShardEquality.greaterThanOrEqual("20190314", "20190314_17"));
    }
    
    @Test
    public void testGreaterThanOrEqual() {
        assertTrue(ShardEquality.greaterThanOrEqual("20190314", "20190314"));
        assertTrue(ShardEquality.greaterThanOrEqual("20190314", "20190301"));
        assertTrue(ShardEquality.greaterThanOrEqual("20190314_17", "20190301_17"));
        assertTrue(ShardEquality.greaterThanOrEqual("20190314_18", "20190301_17"));
        assertTrue(ShardEquality.greaterThanOrEqual("20190314_100", "20190301_17"));
        assertFalse(ShardEquality.greaterThanOrEqual("20190314", "20190314_17"));
    }
    
    @Test
    public void testIsDay() {
        assertTrue(ShardEquality.isDay("20190314"));
        assertTrue(ShardEquality.isDay("20200202"));
        assertTrue(ShardEquality.isDay("99990000"));
        
        assertFalse(ShardEquality.isDay("20190314_15"));
        assertFalse(ShardEquality.isDay("20191224_12"));
        assertFalse(ShardEquality.isDay("20200101_0"));
        assertFalse(ShardEquality.isDay("20200101_"));
    }
    
    @Test
    public void testDaysMatch() {
        assertTrue(ShardEquality.daysMatch("20190314", "20190314"));
        assertTrue(ShardEquality.daysMatch("20190314", "20190314_11"));
        assertFalse(ShardEquality.daysMatch("20190314", "20191224"));
        assertFalse(ShardEquality.daysMatch("20190314", "20190307_117"));
    }
}
