package datawave.query.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.junit.Test;

public class DateFrequencyMapTest {

    @Test
    public void testSetAndGet() {
        DateFrequencyMap map = new DateFrequencyMap();
        assertTrue(map.isEmpty());
        assertEquals(0, map.size());
        assertNull(map.get("20200202"));
        map.put("20200202", new Frequency(10));
        assertFalse(map.isEmpty());
        assertEquals(1, map.size());
        assertEquals(new Frequency(10), map.get("20200202"));
        assertNull(map.get("20200203"));
        map.put("20200203", new Frequency(20));
        assertFalse(map.isEmpty());
        assertEquals(2, map.size());
        assertEquals(new Frequency(10), map.get("20200202"));
        assertEquals(new Frequency(20), map.get("20200203"));
        map.put("20200203", new Frequency(30));
        assertFalse(map.isEmpty());
        assertEquals(2, map.size());
        assertEquals(new Frequency(10), map.get("20200202"));
        assertEquals(new Frequency(30), map.get("20200203"));
        map.remove("20200202");
        assertFalse(map.isEmpty());
        assertEquals(1, map.size());
        assertNull(map.get("20200202"));
        assertEquals(new Frequency(30), map.get("20200203"));
        map.remove("20200203");
        assertTrue(map.isEmpty());
        assertEquals(0, map.size());
        assertNull(map.get("20200202"));
        assertNull(map.get("20200203"));
    }

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
