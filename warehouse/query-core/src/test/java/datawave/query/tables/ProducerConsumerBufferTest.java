package datawave.query.tables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ProducerConsumerBufferTest {
    private ProducerConsumerBuffer<Integer> queue;

    @Before
    public void setup() {
        queue = new ProducerConsumerBuffer<>(25);
    }

    @Test
    public void blockOnFullTest() throws InterruptedException {
        for (int i = 0; i < 25; i++) {
            queue.put(i);
        }
        // the next call will block until something is removed form the queue
        long start = System.currentTimeMillis();
        Thread t = new Thread(() -> {
            try {
                // wait a fixed amount of time so we have something to measure the test against
                Thread.sleep(250);
                queue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        t.start();
        queue.put(26);
        long end = System.currentTimeMillis();

        // if the final produce was blocked it had to wait at least as long as the sleep
        assertTrue(end - start >= 250);
    }

    @Test
    public void blockOnEmptyTest() throws InterruptedException {
        long start = System.currentTimeMillis();
        Thread t = new Thread(() -> {
            try {
                // wait a fixed amount of time so we have something to measure the test against
                Thread.sleep(250);
                queue.put(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        t.start();

        queue.take();
        long end = System.currentTimeMillis();
        // if the final produce was blocked it had to wait at least as long as the sleep
        assertTrue(end - start >= 250);
    }

    @Test(expected = InterruptedException.class)
    public void putHandlesInterrupt() throws InterruptedException {
        Thread t = Thread.currentThread();
        for (int i = 0; i < 25; i++) {
            queue.put(i);
        }
        Thread interrupter = new Thread(() -> {
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            t.interrupt();
        });
        interrupter.start();

        // no room for this, so it will block until interrupted
        queue.put(26);
    }

    @Test(expected = InterruptedException.class)
    public void takeHandlesInterrupt() throws InterruptedException {
        Thread t = Thread.currentThread();
        Thread interrupter = new Thread(() -> {
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            t.interrupt();
        });
        interrupter.start();

        // nothing to consume so blocks until interrupted
        queue.take();
    }

    @Test
    public void threadSafeTest() throws InterruptedException {
        Thread producer = new Thread(() -> {
            try {
                for (int i = 0; i < 1000000; i++) {
                    queue.put(i);
                    if (i % 50000 == 0) {
                        Thread.sleep(25);
                    }
                }
                queue.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
        });

        final List<Integer> data = new ArrayList<>();

        Thread consumer = new Thread(() -> {
            try {
                for (int i = 0; i < 1000000; i++) {
                    Integer integer = queue.take();
                    data.add(integer);
                    if (i % 100000 == 0) {
                        Thread.sleep(250);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
        });

        producer.start();
        consumer.start();

        producer.join();
        consumer.join();

        for (int i = 0; i < 1000000; i++) {
            assertTrue(data.get(i) == i);
        }
    }

    @Test
    public void threadSafeMultipleProducersTest() throws InterruptedException {
        List<Thread> producers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            producers.add(createProducer(queue, i, 10, 1000000));
        }
        producers.forEach(Thread::start);

        final List<Integer> data = new ArrayList<>();

        Thread consumer = new Thread(() -> {
            try {
                Integer integer = queue.take();
                int count = 0;
                while (integer != null) {
                    data.add(integer);
                    if (count % 100000 == 0) {
                        Thread.sleep(250);
                    }
                    integer = queue.take();
                    count++;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
        });

        consumer.start();

        // wait for all producers
        producers.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        // let the consumer know there is nothing else coming
        queue.close();
        consumer.join();

        // sort since the data is mixed
        Collections.sort(data);
        for (int i = 0; i < 1000000; i++) {
            assertTrue(data.get(i) == i);
        }
    }

    @Test
    public void multipleConsumerTest() {
        List<Thread> producers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            producers.add(createProducer(queue, i, 10, 1000000));
        }
        producers.forEach(Thread::start);

        final List<Integer> data = Collections.synchronizedList(new ArrayList<>());

        List<Thread> consumers = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            consumers.add(createConsumer(queue, data));
        }
        consumers.forEach(Thread::start);

        producers.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        queue.close();

        consumers.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        Collections.sort(data);
        synchronized (data) {
            for (int i = 0; i < 1000000; i++) {
                assertTrue(data.get(i) == i);
            }
        }
    }

    @Test
    public void minSizeTest() throws InterruptedException {
        queue = new ProducerConsumerBuffer<>(25, new LinkedList<>(), 2);
        Thread current = Thread.currentThread();
        new Thread(() -> {
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            current.interrupt();
        }).start();

        queue.put(new Integer(1));
        InterruptedException caught = null;
        try {
            // will block until interrupted since it's waiting on the second item for min size
            queue.take();
        } catch (InterruptedException e) {
            caught = e;
        }
        assertTrue(caught != null);
        queue.put(new Integer(2));
        queue.take();
    }

    @Test
    public void minSizeClosedTest() throws InterruptedException {
        queue = new ProducerConsumerBuffer<>(25, new LinkedList<>(), 2);
        new Thread(() -> {
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            queue.close();
        }).start();

        queue.put(new Integer(1));
        assertEquals(new Integer(1), queue.take());
    }

    @Ignore
    @Test
    public void overheadTest() throws InterruptedException, ExecutionException {
        ExecutorService pool = Executors.newFixedThreadPool(75);

        long start = System.currentTimeMillis();
        // Thread t = createProducer(queue, 0, 1, 1000000);
        // t.start();
        Future t = pool.submit(producerRunnable(queue, 0, 1, 1000000));
        List<Integer> data = new ArrayList<>();
        // Thread c = createConsumer(queue, data);
        // c.start();
        Future c = pool.submit(consumerRunnable(queue, data));

        // t.join();
        t.get();

        queue.close();
        // c.join();
        c.get();
        long end = System.currentTimeMillis();
        long total = (end - start);

        queue = new ProducerConsumerBuffer<>(25);
        start = System.currentTimeMillis();
        // List<Thread> producers = new ArrayList<>();
        // for (int i = 0; i < 50; i++) {
        // producers.add(createProducer(queue, i, 50, 1000000));
        // }
        // producers.forEach(Thread::start);
        List<Future> producers = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            producers.add(pool.submit(producerRunnable(queue, i, 25, 1000000)));
        }

        data = new ArrayList<>();
        c = pool.submit(consumerRunnable(queue, data));
        // c = createConsumer(queue, data);
        // c.start();

        // producers.forEach(p -> {
        // try {
        // p.join();
        // } catch (InterruptedException e) {
        // throw new RuntimeException(e);
        // }
        // });
        producers.forEach(f -> {
            try {
                f.get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        });
        queue.close();
        // c.join();
        c.get();
        end = System.currentTimeMillis();
        long totalMultiProducer = (end - start);

        start = System.currentTimeMillis();
        data = new ArrayList<>();
        for (int i = 0; i < 1000000; i++) {
            data.add(i);
        }
        end = System.currentTimeMillis();
        System.out.println("1 thread no locking time:" + (end - start));
        System.out.println("1 producer 1 consumer time:" + total);
        System.out.println("25 producers 1 consumer time: " + totalMultiProducer);
    }

    @Test
    public void testArrayBlockingQueue() throws InterruptedException {
        ArrayBlockingQueue<Integer> blockingQueue = new ArrayBlockingQueue<>(25);
        // ProducerConsumerQueue<Integer> queue = new ProducerConsumerQueue<>(new ArrayBlockingQueue<>(25));
        long start = System.currentTimeMillis();
        Thread p = new Thread(() -> {
            for (int i = 0; i < 1000000; i++) {
                try {
                    // queue.produce(i);
                    blockingQueue.put(i);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        p.start();

        List<Integer> data = new ArrayList<>();
        Thread c = new Thread(() -> {
            for (int i = 0; i < 1000000; i++) {
                // Integer val = blockingQueue.poll();
                Integer val = null;
                try {
                    // val = queue.consume();
                    val = blockingQueue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                data.add(val);
            }
        });
        c.start();

        p.join();
        c.join();
        long end = System.currentTimeMillis();
        System.out.println("blocking queue time: " + (end - start));

    }

    private Runnable consumerRunnable(ProducerConsumerBuffer<Integer> queue, List<Integer> threadSafeList) {
        return () -> {
            try {
                Integer i = queue.take();
                while (i != null) {
                    threadSafeList.add(i);
                    i = queue.take();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private Runnable producerRunnable(ProducerConsumerBuffer<Integer> queue, int index, int producers, int total) {
        return () -> {
            for (int i = 0; i < total; i++) {
                if (i % producers == index) {
                    try {
                        queue.put(i);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
    }

    private Thread createConsumer(ProducerConsumerBuffer<Integer> queue, List<Integer> threadSafeList) {
        Thread t = new Thread(consumerRunnable(queue, threadSafeList));
        t.setName("consumer");
        return t;
    }

    private Thread createProducer(ProducerConsumerBuffer<Integer> queue, int index, int producers, int total) {
        Thread t = new Thread(producerRunnable(queue, index, producers, total));
        t.setName("producer " + index);
        return t;
    }
}
