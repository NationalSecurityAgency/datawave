package datawave.core.cache;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ClassCacheIT {

    private final boolean debug = false;
    private final int maxIterations = 1_000;
    private final int lowThreads = 5;
    private final int highThreads = 50;

    @Test
    @Order(1)
    public void testLowThreadsLowVariance() {
        testThreads(lowThreads, lowVariance());
    }

    @Test
    @Order(2)
    public void testLowThreadsHighVariance() {
        testThreads(lowThreads, highVariance());
    }

    @Test
    @Order(3)
    public void testHighThreadsLowVariance() {
        testThreads(highThreads, lowVariance());
    }

    @Test
    @Order(4)
    public void testHighThreadsHighVariance() {
        testThreads(highThreads, highVariance());
    }

    private void testThreads(int threads, List<String> names) {
        List<ClassCache> caches = getCaches();
        List<Pair<Long,String>> outputs = new ArrayList<>();
        for (ClassCache cache : caches) {
            Pair<Long,String> output = testThreads(threads, cache, names);
            outputs.add(output);
        }

        if (debug) {
            Collections.sort(outputs);
            for (Pair<Long,String> output : outputs) {
                System.out.println(output.getRight());
            }
        }
    }

    private Pair<Long,String> testThreads(int threads, ClassCache cache, List<String> names) {
        ExecutorService executor = null;
        try {
            executor = Executors.newFixedThreadPool(threads);
            List<Callable<Long>> callables = createCallables(threads, names, cache);
            List<Future<Long>> futures = executor.invokeAll(callables);

            long total = 0L;
            for (Future<Long> future : futures) {
                long time = future.get();
                total += time;
            }

            long time = total / ((long) threads * names.size());
            time = TimeUnit.NANOSECONDS.toMicros(time);

            String line = "threads: " + threads + " size: " + names.size() + " iterations: " + maxIterations + " took " + time + " micros for " + cache.name();
            return Pair.of(time, line);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }

    private List<Callable<Long>> createCallables(int threads, List<String> names, ClassCache cache) {
        List<Callable<Long>> callables = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final List<String> copy = new ArrayList<>(names);
            Collections.shuffle(copy);
            callables.add(() -> {
                long total = 0L;
                for (int k = 0; k < maxIterations; k++) {
                    for (String name : copy) {
                        long elapsed = System.nanoTime();
                        Class<?> clazz = cache.get(name);
                        total += System.nanoTime() - elapsed;
                    }
                }
                return total;
            });
        }
        return callables;
    }

    private List<ClassCache> getCaches() {
        List<ClassCache> caches = new ArrayList<>();
        caches.add(new CaffeineClassCache());
        caches.add(new GuavaClassCache());
        caches.add(new LRUMapClassCache());
        return caches;
    }

    private List<String> lowVariance() {
        List<String> names = new ArrayList<>();
        names.add(String.class.getTypeName());
        names.add(Integer.class.getTypeName());
        names.add(Long.class.getTypeName());
        names.add(Double.class.getTypeName());
        names.add(Float.class.getTypeName());
        return names;
    }

    private List<String> highVariance() {
        List<String> names = lowVariance();
        names.add(Map.class.getTypeName());
        names.add(List.class.getTypeName());
        names.add(LinkedList.class.getTypeName());
        names.add(Queue.class.getTypeName());
        names.add(Deque.class.getTypeName());
        names.add(ArrayDeque.class.getTypeName());
        return names;
    }
}
