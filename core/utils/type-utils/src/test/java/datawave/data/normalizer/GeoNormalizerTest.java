package datawave.data.normalizer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;

public class GeoNormalizerTest {

    private final int threads = 100;
    private final int iterations = 1000;

    @Test
    public void test() throws Exception {
        GeoNormalizer normalizer = new GeoNormalizer();
        assertEquals("112082..7616172234", normalizer.normalize("38.71123_-77.33276"));
    }

    @Test
    public void testThreadBlocking() throws ExecutionException, InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        List<Future<Integer>> tasks = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Future<Integer> f = executor.submit(() -> {
                int count = 0;
                GeoNormalizer normalizer = new GeoNormalizer();
                for (int j = 0; j < iterations; j++) {
                    String result = normalizer.normalize("38.71123_-77.33276");
                    assertEquals("112082..7616172234", result);
                    count++;
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
                } else {
                    assertEquals(iterations, f.get());
                }
                working = false;
            }
        }
    }
}
