package datawave.ingest.time;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Created on 10/13/16.
 */
public class NowTest {

    @Test
    public void testSingleton() {
        Now now = Now.getInstance();
        Now now2 = Now.getInstance();
        assertSame(now, now2);
    }

    @Test
    public void testDelegate() {
        Now now = Now.getInstance();
        Now now2 = new Now();
        assertEquals(now, now2);
    }

    @Test
    public void testDelegate2() {
        Now now = new Now();
        Now now2 = new Now();
        assertEquals(now, now2);
    }

    @Test
    public void testDelegate3() {
        Now now = new Now();
        Now now2 = Now.getInstance();
        assertEquals(now, now2);
    }

    @Test
    public void testNow() throws InterruptedException {
        Now now = Now.getInstance();
        for (int i = 0; i < 5; i++) {
            long time = System.currentTimeMillis();
            long nowTime = now.get();
            long time2 = System.currentTimeMillis();
            // added an extra 100 milliseconds to handle time to set AtomicLong within the thread.....
            assertTrue(Math.abs(nowTime - time) < (1100 + time2 - time),
                            "Failed to get a reasonable time from the Now class....may be due running tests on a very busy machine");
            Thread.sleep(1000);
        }
    }
}
