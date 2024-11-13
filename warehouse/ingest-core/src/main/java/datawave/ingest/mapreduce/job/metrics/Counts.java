package datawave.ingest.mapreduce.job.metrics;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import com.google.common.collect.MapMaker;

/**
 * Data structure for storing counts in a thread-safe manner. This data structure supports concurrent writes. The flush() operation will cause all other
 * operations to block though.
 */
public class Counts<K> {

    private static final Logger logger = Logger.getLogger(Counts.class);

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock updateLock = rwLock.readLock();
    private final Lock flushLock = rwLock.writeLock();

    private final MapMaker mapMaker;
    private AtomicInteger numRecords = new AtomicInteger(0);
    private ConcurrentMap<K,AtomicLong> counts;

    public Counts(int initSize) {
        this.mapMaker = new MapMaker().initialCapacity(initSize);
        this.counts = mapMaker.makeMap();
    }

    /**
     * Add to the counter
     *
     * @param key
     *            the key
     * @param value
     *            the value
     */
    public void add(K key, long value) {
        updateLock.lock();
        try {
            AtomicLong current = counts.get(key);

            if (current == null) {
                current = initCounter(key);
                numRecords.getAndIncrement();
            }

            current.getAndAdd(value);

        } finally {
            updateLock.unlock();
        }
    }

    /**
     * @return current size
     */
    public int size() {
        return numRecords.get();
    }

    /**
     * Flush the currently stored counts with the given operation.
     *
     * @param flushOp
     *            the flushOp
     */
    public void flush(FlushOp<K> flushOp) {
        flushLock.lock();
        try {

            if (!counts.isEmpty()) {
                try {
                    flushOp.flush(counts);
                } catch (Exception e) {
                    logger.error("Failed to flush counts", e);
                }

                counts = mapMaker.makeMap();
                numRecords.set(0);
            }

        } finally {
            flushLock.unlock();
        }
    }

    private AtomicLong initCounter(K key) {
        AtomicLong counter = new AtomicLong(0);
        AtomicLong prev = counts.putIfAbsent(key, counter);
        return (prev == null ? counter : prev);
    }

    /**
     * Interface to flush this data structure.
     *
     * @param <K>
     *            the type of the flush operation
     */
    public interface FlushOp<K> {
        void flush(ConcurrentMap<K,AtomicLong> counts);
    }
}
