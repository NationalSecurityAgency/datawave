package datawave.query.tables;

import java.io.Closeable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Create a thread-safe buffer between producers and consumers without the need for polling loops to detect the last element. All thread safety is managed by
 * the class, so unless external shared locking for the queue is required a LinkedList
 *
 * @param <T>
 *            The type of object to buffer
 */
public class ProducerConsumerBuffer<T> implements Closeable {
    private final Lock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();

    private boolean doneProducing;
    private long buffered;

    private final Queue<T> queue;
    private final int capacity;
    private final int minInitialBuffer;

    public ProducerConsumerBuffer(int capacity) {
        this(capacity, new LinkedList<>());
    }

    public ProducerConsumerBuffer(int capacity, Queue<T> queue) {
        this(capacity, queue, 1);
    }

    public ProducerConsumerBuffer(int capacity, Queue<T> queue, int minInitialBuffer) {
        this.capacity = capacity;
        this.queue = queue;
        this.minInitialBuffer = minInitialBuffer;

        this.doneProducing = false;
        this.buffered = 0;
    }

    /**
     * Indicates no more objects will be put to the queue. Signals any threads waiting to unblock them
     */
    public void close() {
        lock.lock();
        try {
            if (!doneProducing) {
                doneProducing = true;
                notEmpty.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean isClosed() {
        lock.lock();
        try {
            return doneProducing;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Adds all items from the iterator to the queue blocking until space is available not closing the producer side when done
     *
     * @param items
     *            the items to put on the queue
     * @throws InterruptedException
     *             if interrupted while blocking
     */
    public void putAll(Iterator<T> items) throws InterruptedException {
        putAll(items, false);
    }

    /**
     * Adds all items from the iterator to the queue blocking until space is available. optionally closing the producer side when items is consumed
     *
     * @param items
     *            the items to put on the queue
     * @param closeOnEmpty
     *            when true no future items may be put to the queue and all takes will be signaled
     * @throws InterruptedException
     *             if interrupted while blocking
     */
    public void putAll(Iterator<T> items, boolean closeOnEmpty) throws InterruptedException {
        while (items.hasNext()) {
            T next = items.next();
            put(next);
        }
        if (closeOnEmpty) {
            close();
        }
    }

    /**
     * Add an item to the queue blocking until space is available. Adding an item will signal a waiting take() caller
     *
     * @param item
     *            the item to add
     * @throws InterruptedException
     *             if interrupted while blocking
     */
    public void put(T item) throws InterruptedException {
        lock.lock();
        try {
            if (doneProducing) {
                throw new IllegalStateException("cannot put to a queue that has been closed");
            }
            while (queue.size() == capacity) {
                notFull.await();
            }
            queue.offer(item);

            // track number of offered items up to the minimumInitialBuffer
            if (minInitialBuffer > 1 && minInitialBuffer > buffered) {
                buffered++;
            }
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Take an item from the queue, blocking until it is available or the queue has been closed
     *
     * @return the item from the queue or null if the queue is empty and has been closed
     * @throws InterruptedException
     *             if interrupted while blocking
     */
    public T take() throws InterruptedException {
        lock.lock();
        try {
            // wait until something is on the queue, or there are at least the minimum initial buffer items on the queue unless done producing
            while (!doneProducing && (queue.isEmpty() || (minInitialBuffer > 1 && minInitialBuffer > buffered))) {
                notEmpty.await();
            }

            if (doneProducing && queue.isEmpty()) {
                return null;
            }

            T item = queue.poll();
            notFull.signal();
            return item;
        } finally {
            lock.unlock();
        }
    }
}
