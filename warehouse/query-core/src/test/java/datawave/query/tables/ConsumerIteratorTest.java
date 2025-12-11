package datawave.query.tables;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerIteratorTest {

    private static final Logger log = LoggerFactory.getLogger(ConsumerIteratorTest.class);

    @Test
    public void testConsumerIterator() {
        int expected = 1_000;
        final AtomicInteger reads = new AtomicInteger(0);
        final AtomicInteger writes = new AtomicInteger(0);

        ProducerConsumerBuffer<Integer> queue = new ProducerConsumerBuffer<>(25);

        Thread producer = new Thread(() -> {
            try {
                for (int i = 0; i < expected; i++) {
                    queue.put(i);
                    writes.incrementAndGet();
                }

                log.info("done writing to queue");
                queue.close();
                log.info("added queue close lock");

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("interrupted", e);
            }
        });

        Thread consumer = new Thread(() -> {
            ConsumerIterator<Integer> iterator = new ConsumerIterator<>(queue);
            while (iterator.hasNext()) {
                Integer i = iterator.next();
                reads.incrementAndGet();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                log.info("{}", i);
            }
        });

        producer.start();
        consumer.start();

        while (!queue.isClosed() || consumer.isAlive()) {
            // busy wait
        }

        assertEquals(expected, reads.get());
        assertEquals(expected, writes.get());
    }
}
