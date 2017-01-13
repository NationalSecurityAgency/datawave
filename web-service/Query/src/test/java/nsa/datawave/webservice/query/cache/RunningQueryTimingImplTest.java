package nsa.datawave.webservice.query.cache;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 */
public class RunningQueryTimingImplTest {
    
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {}
    
    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {}
    
    @Test
    public void testQueryExpirationConfiguration() {
        QueryExpirationConfiguration conf = new QueryExpirationConfiguration();
        conf.setCallTime(10);
        conf.setPageShortCircuitTimeout(9);
        conf.setPageSizeShortCircuitCheckTime(5);
        RunningQueryTimingImpl timing = new RunningQueryTimingImpl(conf);
        
        assertEquals(10 * 60 * 1000, timing.getMaxCallMs());
        assertEquals(9 * 60 * 1000, timing.getPageShortCircuitTimeoutMs());
        assertEquals(5 * 60 * 1000, timing.getPageSizeShortCircuitCheckTimeMs());
    }
    
    @Test
    public void testPageShortCircuit() {
        RunningQueryTimingImpl timing = new RunningQueryTimingImpl(10 * 60 * 1000, 10 * 60 * 1000, 9 * 60 * 1000);
        // if we are already at the max time, then return true if page size > 0
        assertTrue(timing.shoudReturnPartialResults(1, 10, 10 * 60 * 1000));
        assertTrue(timing.shoudReturnPartialResults(1, 1, 10 * 60 * 1000));
        assertTrue(timing.shoudReturnPartialResults(10, 1, 10 * 60 * 1000));
        assertTrue(timing.shoudReturnPartialResults(10, 10, 10 * 60 * 1000));
        assertFalse(timing.shoudReturnPartialResults(0, 10, 10 * 60 * 1000));
        
        // if we are at the page short circuit time, then return true if page size > 0
        assertTrue(timing.shoudReturnPartialResults(1, 10, 9 * 60 * 1000));
        assertTrue(timing.shoudReturnPartialResults(1, 1, 9 * 60 * 1000));
        assertTrue(timing.shoudReturnPartialResults(10, 1, 9 * 60 * 1000));
        assertTrue(timing.shoudReturnPartialResults(10, 10, 9 * 60 * 1000));
        assertTrue(timing.shoudReturnPartialResults(10, 100, 9 * 60 * 1000));
        assertFalse(timing.shoudReturnPartialResults(0, 10, 9 * 60 * 1000));
        
        // if we are before the page short circuit time, then return false
        assertFalse(timing.shoudReturnPartialResults(1, 10, 9 * 60 * 1000 - 1));
        assertFalse(timing.shoudReturnPartialResults(1, 1, 9 * 60 * 1000 - 1));
        assertFalse(timing.shoudReturnPartialResults(10, 1, 9 * 60 * 1000 - 1));
        assertFalse(timing.shoudReturnPartialResults(10, 10, 9 * 60 * 1000 - 1));
        assertFalse(timing.shoudReturnPartialResults(10, 100, 9 * 60 * 1000 - 1));
        assertFalse(timing.shoudReturnPartialResults(0, 10, 9 * 60 * 1000 - 1));
    }
    
    @Test
    public void testPageSizeShortCircuit() {
        RunningQueryTimingImpl timing = new RunningQueryTimingImpl(10 * 60 * 1000, 9 * 60 * 1000, 10 * 60 * 1000);
        // if we are already at the max time, then return true if page size < 90% max page size
        assertTrue(timing.shoudReturnPartialResults(1, 10, 9 * 60 * 1000));
        assertTrue(timing.shoudReturnPartialResults(8, 10, 9 * 60 * 1000));
        assertFalse(timing.shoudReturnPartialResults(9, 10, 9 * 60 * 1000));
        assertFalse(timing.shoudReturnPartialResults(10, 10, 9 * 60 * 1000));
        
        // if we are before the page size short circuit time, then return false
        assertFalse(timing.shoudReturnPartialResults(1, 10, 9 * 60 * 1000 - 1));
        assertFalse(timing.shoudReturnPartialResults(8, 10, 9 * 60 * 1000 - 1));
        assertFalse(timing.shoudReturnPartialResults(9, 10, 9 * 60 * 1000 - 1));
        assertFalse(timing.shoudReturnPartialResults(10, 10, 9 * 60 * 1000 - 1));
    }
    
}
