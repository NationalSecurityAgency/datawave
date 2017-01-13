package nsa.datawave.poller.manager;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class RunningAverageTest {
    
    @Test
    public void testRunningAverage() {
        RunningAverage avg = new RunningAverage();
        Random rnd = new Random();
        for (int i = 0; i < 10000; i++) {
            avg.add(rnd.nextDouble() * 1000);
            double total = 0.0d;
            for (Double d : avg.values) {
                total += d;
            }
            double expected = total / avg.values.size();
            double calculated = avg.getAverage();
            double diff = Math.abs(expected - calculated);
            Assert.assertTrue("Unexpected average calculation at step " + i + ": " + expected + " / " + calculated, diff < 0.00000001d);
        }
        
    }
    
}
