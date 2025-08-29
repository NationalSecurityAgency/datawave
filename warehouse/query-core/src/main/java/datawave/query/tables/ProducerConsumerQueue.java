package datawave.query.tables;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class ProducerConsumerQueue<T> {
    private final BlockingQueue<T> queue;

    public ProducerConsumerQueue(int capacity) {
        this(new ArrayBlockingQueue<>(capacity));
    }

    public ProducerConsumerQueue(BlockingQueue<T> queue) {
        this.queue = queue;
    }

    public void produce(T item) throws InterruptedException {
        queue.put(item);
    }

    public T consume() throws InterruptedException {
        return queue.take();
    }
}
