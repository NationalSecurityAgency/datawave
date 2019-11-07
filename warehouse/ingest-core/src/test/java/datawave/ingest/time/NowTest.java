package datawave.ingest.time;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created on 10/13/16.
 */
public class NowTest {
    
    @Test
    public void testSingleton() {
        Now now = Now.getInstance();
        Now now2 = Now.getInstance();
        Assert.assertSame(now, now2);
    }
    
    @Test
    public void testDelegate() {
        Now now = Now.getInstance();
        Now now2 = new Now();
        Assert.assertEquals(now, now2);
    }
    
    @Test
    public void testDelegate2() {
        Now now = new Now();
        Now now2 = new Now();
        Assert.assertEquals(now, now2);
    }
    
    @Test
    public void testDelegate3() {
        Now now = new Now();
        Now now2 = Now.getInstance();
        Assert.assertEquals(now, now2);
    }
    
    @Test
    public void testNow() throws InterruptedException {
        Now now = Now.getInstance();
        for (int i = 0; i < 5; i++) {
            long time = System.currentTimeMillis();
            long nowTime = now.get();
            long time2 = System.currentTimeMillis();
            // added an extra 100 milliseconds to handle time to set AtomicLong within the thread.....
            Assert.assertTrue("Failed to get a reasonable time from the Now class....may be due running tests on a very busy machine",
                            Math.abs(nowTime - time) < (1100 + time2 - time));
            Thread.sleep(1000);
        }
    }
}
