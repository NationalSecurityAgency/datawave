package datawave.query.util.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.reflect.Constructor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import datawave.data.type.NoOpType;
import datawave.data.type.Type;

public class ClassCacheTest {

    private final ClassCache classCache = new ClassCache();

    @Test
    public void testCacheActuallyWorks() throws Exception {
        String name = NoOpType.class.getTypeName();
        ClassCache localCache = new ClassCache();
        Class<?> one = localCache.get(name);
        Class<?> two = localCache.get(name);
        assertSame(one, two);
    }

    private final AtomicInteger successfulTasks = new AtomicInteger(0);

    @Test
    public void testThreadSafety() throws InterruptedException {
        int size = 50;
        ExecutorService executor = Executors.newFixedThreadPool(size);

        for (int i = 0; i < size; i++) {
            Worker worker = new Worker(classCache);
            executor.execute(worker);
        }

        assertFalse(executor.awaitTermination(3, TimeUnit.SECONDS), "Executor was interrupted before it could shut down gracefully");
        assertEquals(size, successfulTasks.get());
    }

    public class Worker implements Runnable {

        private final int maxWork = 10_000;
        private final ClassCache classCache;

        public Worker(ClassCache classCache) {
            this.classCache = classCache;
        }

        @Override
        public void run() {
            int count = 0;
            for (int i = 0; i < maxWork; i++) {
                try {
                    Class<?> clazz = classCache.get(NoOpType.class.getTypeName());
                    Constructor<Type> constructor = (Constructor<Type>) clazz.getDeclaredConstructor();
                    Type<?> type = constructor.newInstance();
                    type.normalize("data");
                    count++;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            assertEquals(maxWork, count);
            successfulTasks.getAndIncrement();
        }
    }

}
