package datawave.query.model;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.junit.Test;

public class DateFrequencyMapTest {
    @Test
    public void testSerialization() throws IOException {
        DateFrequencyMap map = new DateFrequencyMap();
        Random rand = new Random(1000);
        for (int i = 0; i < 1000; i++) {
            map.put(randomDate(rand), rand.nextLong());
        }
        byte[] serialized = map.toBytes();
        DateFrequencyMap newMap = new DateFrequencyMap(serialized);
        assertEquals(map, newMap);
    }
    
    private static final SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
    
    private static String randomDate(Random rand) {
        long millis = rand.nextLong() % System.currentTimeMillis();
        Date d = new Date(millis);
        synchronized (format) {
            return format.format(d);
        }
    }
}
