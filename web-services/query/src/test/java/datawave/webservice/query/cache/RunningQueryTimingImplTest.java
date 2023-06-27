package datawave.webservice.query.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import datawave.microservice.query.config.QueryExpirationProperties;

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
    public void testQueryExpirationPropertiesDefaults() {
        QueryExpirationProperties conf = new QueryExpirationProperties();
        RunningQueryTimingImpl timing = new RunningQueryTimingImpl(conf, -1);

        assertEquals(60 * 60 * 1000, timing.getMaxCallMs());
        assertEquals(58 * 60 * 1000, timing.getPageShortCircuitTimeoutMs());
        assertEquals(30 * 60 * 1000, timing.getPageSizeShortCircuitCheckTimeMs());
    }

    @Test
    public void testQueryExpirationProperties() {
        QueryExpirationProperties conf = new QueryExpirationProperties();
        conf.setCallTimeout(10);
        conf.setShortCircuitTimeout(9);
        conf.setShortCircuitCheckTime(5);
        RunningQueryTimingImpl timing = new RunningQueryTimingImpl(conf, -1);

        assertEquals(10 * 60 * 1000, timing.getMaxCallMs());
        assertEquals(9 * 60 * 1000, timing.getPageShortCircuitTimeoutMs());
        assertEquals(5 * 60 * 1000, timing.getPageSizeShortCircuitCheckTimeMs());
    }

    @Test
    public void testQueryExpirationPropertiesWithTimeout() {
        QueryExpirationProperties conf = new QueryExpirationProperties();
        RunningQueryTimingImpl timing = new RunningQueryTimingImpl(conf, 20);

        assertEquals(20 * 60 * 1000, timing.getMaxCallMs());
        assertEquals(Math.round(0.97 * 20 * 60 * 1000), timing.getPageShortCircuitTimeoutMs());
        assertEquals(10 * 60 * 1000, timing.getPageSizeShortCircuitCheckTimeMs());
    }

    @Test
    public void testPageShortCircuit() {
        RunningQueryTimingImpl timing = new RunningQueryTimingImpl(10 * 60 * 1000, 10 * 60 * 1000, 9 * 60 * 1000);
        // if we are already at the max time, then return true if page size > 0
        assertTrue(timing.shouldReturnPartialResults(1, 10, 10 * 60 * 1000));
        assertTrue(timing.shouldReturnPartialResults(1, 1, 10 * 60 * 1000));
        assertTrue(timing.shouldReturnPartialResults(10, 1, 10 * 60 * 1000));
        assertTrue(timing.shouldReturnPartialResults(10, 10, 10 * 60 * 1000));
        assertFalse(timing.shouldReturnPartialResults(0, 10, 10 * 60 * 1000));

        // if we are at the page short circuit time, then return true if page size > 0
        assertTrue(timing.shouldReturnPartialResults(1, 10, 9 * 60 * 1000));
        assertTrue(timing.shouldReturnPartialResults(1, 1, 9 * 60 * 1000));
        assertTrue(timing.shouldReturnPartialResults(10, 1, 9 * 60 * 1000));
        assertTrue(timing.shouldReturnPartialResults(10, 10, 9 * 60 * 1000));
        assertTrue(timing.shouldReturnPartialResults(10, 100, 9 * 60 * 1000));
        assertFalse(timing.shouldReturnPartialResults(0, 10, 9 * 60 * 1000));

        // if we are before the page short circuit time, then return false
        assertFalse(timing.shouldReturnPartialResults(1, 10, 9 * 60 * 1000 - 1));
        assertFalse(timing.shouldReturnPartialResults(1, 1, 9 * 60 * 1000 - 1));
        assertFalse(timing.shouldReturnPartialResults(10, 1, 9 * 60 * 1000 - 1));
        assertFalse(timing.shouldReturnPartialResults(10, 10, 9 * 60 * 1000 - 1));
        assertFalse(timing.shouldReturnPartialResults(10, 100, 9 * 60 * 1000 - 1));
        assertFalse(timing.shouldReturnPartialResults(0, 10, 9 * 60 * 1000 - 1));
    }

    @Test
    public void testPageSizeShortCircuit() {
        RunningQueryTimingImpl timing = new RunningQueryTimingImpl(10 * 60 * 1000, 9 * 60 * 1000, 10 * 60 * 1000);
        // if we are already at the max time, then return true if page size < 90% max page size
        assertTrue(timing.shouldReturnPartialResults(1, 10, 9 * 60 * 1000));
        assertTrue(timing.shouldReturnPartialResults(8, 10, 9 * 60 * 1000));
        assertFalse(timing.shouldReturnPartialResults(9, 10, 9 * 60 * 1000));
        assertFalse(timing.shouldReturnPartialResults(10, 10, 9 * 60 * 1000));

        // if we are before the page size short circuit time, then return false
        assertFalse(timing.shouldReturnPartialResults(1, 10, 9 * 60 * 1000 - 1));
        assertFalse(timing.shouldReturnPartialResults(8, 10, 9 * 60 * 1000 - 1));
        assertFalse(timing.shouldReturnPartialResults(9, 10, 9 * 60 * 1000 - 1));
        assertFalse(timing.shouldReturnPartialResults(10, 10, 9 * 60 * 1000 - 1));
    }

}
