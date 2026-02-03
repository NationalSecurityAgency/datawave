package datawave.data.type.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class NumericalEncoderTest {

    @Test
    public void testIsPossiblyEncoded() {
        assertFalse(NumericalEncoder.isPossiblyEncoded(null));
        assertFalse(NumericalEncoder.isPossiblyEncoded(""));
        assertFalse(NumericalEncoder.isPossiblyEncoded("1"));
        assertFalse(NumericalEncoder.isPossiblyEncoded("+1"));
        assertFalse(NumericalEncoder.isPossiblyEncoded("!1"));
        assertTrue(NumericalEncoder.isPossiblyEncoded("+aE5.4"));
        assertTrue(NumericalEncoder.isPossiblyEncoded("+ae5.4"));
        assertFalse(NumericalEncoder.isPossiblyEncoded("+aE5.4.4.4.4"));
        assertTrue(NumericalEncoder.isPossiblyEncoded("+AE0"));
        assertFalse(NumericalEncoder.isPossiblyEncoded("+AE0.."));
        assertFalse(NumericalEncoder.isPossiblyEncoded(Long.valueOf(Long.MAX_VALUE).toString()));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode(".0005")));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode("1")));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode("5")));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode("1000")));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode("1001")));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode("10001")));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode("100001")));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode("1000001")));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode("100000001")));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode("100000008")));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode("-.0005")));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode("-1")));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode("-5")));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode("-1000")));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode("-1001")));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode("-10001")));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode("-100001")));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode("-1000001")));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode("-100000001")));
        assertTrue(NumericalEncoder.isPossiblyEncoded(NumericalEncoder.encode("-100000008")));

    }

    @Test
    public void testEncode() {
        assertEquals("+aE5", NumericalEncoder.encode("5"));
        assertEquals("+aE6", NumericalEncoder.encode("6"));
        assertEquals("+dE1", NumericalEncoder.encode("1000"));
        assertEquals("+dE1.001", NumericalEncoder.encode("1001"));
        assertEquals("+eE1.0001", NumericalEncoder.encode("10001"));
        assertEquals("+fE1.00001", NumericalEncoder.encode("100001"));
        assertEquals("+gE1.000001", NumericalEncoder.encode("1000001"));
        assertEquals("+iE1.00000001", NumericalEncoder.encode("100000001"));
        assertEquals("+iE1.00000008", NumericalEncoder.encode("100000008"));
    }

    @Test
    public void testDecode() {
        for (long i = 0; i < 10000; i++) {
            assertEquals(i, NumericalEncoder.decode(NumericalEncoder.encode(Long.valueOf(i).toString())).longValue());
        }

    }

    @Test
    public void testDecodeBigNums() {
        for (long i = 5; i < Long.MAX_VALUE; i *= 1.0002) {
            assertEquals(i, NumericalEncoder.decode(NumericalEncoder.encode(Long.valueOf(i).toString())).longValue());
            i++;
        }
    }

    @Test
    public void testDecodeBigNumsRandomIncrement() {
        int increment = new Random().nextInt(9) + 1;
        for (long i = 1; i < Long.MAX_VALUE; i *= 1.0002) {
            assertEquals(i, NumericalEncoder.decode(NumericalEncoder.encode(Long.valueOf(i).toString())).longValue());
            i += increment;
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testThreadLocalFormatsPreventBlockedThreads() throws ExecutionException, InterruptedException {
        int threads = 100;
        int numIterations = 1_000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        List<Future<Integer>> tasks = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Future<Integer> f = executor.submit(() -> {
                int count = 0;
                try {
                    for (int j = 0; j < numIterations; j++) {
                        String result = NumericalEncoder.encode("1234567890");
                        assertEquals("+jE1.23456789", result);
                        count++;
                    }
                    return count;
                } catch (Exception e) {
                    fail(e.getMessage(), e);
                }
                return count;
            });
            tasks.add(f);
        }

        boolean working = true;
        while (working) {
            for (Future<Integer> f : tasks) {
                if (!f.isDone()) {
                    break;
                } else if (f.isDone() || f.isCancelled()) {
                    assertEquals(numIterations, f.get());
                }
                working = false;
            }
        }
    }
}
