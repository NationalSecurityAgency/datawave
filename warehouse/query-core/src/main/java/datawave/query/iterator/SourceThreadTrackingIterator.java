package datawave.query.iterator;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class SourceThreadTrackingIterator<K extends WritableComparable<?>,V extends Writable> implements SortedKeyValueIterator<K,V> {
    private static final Logger log = Logger.getLogger(SourceThreadTrackingIterator.class);
    private SortedKeyValueIterator<K,V> source;
    private long threadId = -1;
    private AtomicBoolean inMethod = new AtomicBoolean(false);

    public SourceThreadTrackingIterator(SortedKeyValueIterator<K,V> source) {
        this.source = source;
        if (log.isDebugEnabled())
            log.debug("Starting with " + source + " on thread " + Thread.currentThread().getId());
    }

    public SourceThreadTrackingIterator(SourceThreadTrackingIterator<K,V> other, IteratorEnvironment env) {
        this.source = other.source.deepCopy(env);
        if (log.isDebugEnabled())
            log.debug("Copying " + other.source + " to " + this.source + " on thread " + Thread.currentThread().getId());
    }

    @Override
    public void init(SortedKeyValueIterator<K,V> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (!inMethod.compareAndSet(false, true)) {
            log.error("Concurrently initializing " + source + " on thread " + Thread.currentThread().getId(), new RuntimeException());
        }
        try {
            if (log.isDebugEnabled())
                log.debug("Initializing " + source + " on thread " + Thread.currentThread().getId());
            this.source = source;
        } finally {
            if (!inMethod.compareAndSet(true, false)) {
                log.error("Concurrently initializing " + source + " on thread " + Thread.currentThread().getId(), new RuntimeException());
            }
        }
    }

    @Override
    public boolean hasTop() {
        if (!inMethod.compareAndSet(false, true)) {
            log.error("Concurrently inspecting " + source + " on thread " + Thread.currentThread().getId(), new RuntimeException());
        }
        try {
            if (threadId != Thread.currentThread().getId()) {
                log.error("Using " + source + " on wrong thread: " + threadId + " vs " + Thread.currentThread().getId(), new RuntimeException());
            }
            return source.hasTop();
        } finally {
            if (!inMethod.compareAndSet(true, false)) {
                log.error("Concurrently inspecting " + source + " on thread " + Thread.currentThread().getId(), new RuntimeException());
            }
        }
    }

    @Override
    public void next() throws IOException {
        if (!inMethod.compareAndSet(false, true)) {
            log.error("Concurrently using " + source + " on thread " + Thread.currentThread().getId(), new RuntimeException());
        }
        try {
            if (log.isDebugEnabled())
                log.debug("Using " + source + " on thread " + Thread.currentThread().getId());

            if (threadId != Thread.currentThread().getId()) {
                log.error("Using " + source + " on wrong thread: " + threadId + " vs " + Thread.currentThread().getId(), new RuntimeException());
            }

            source.next();
        } finally {
            if (!inMethod.compareAndSet(true, false)) {
                log.error("Concurrently using " + source + " on thread " + Thread.currentThread().getId(), new RuntimeException());
            }
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (!inMethod.compareAndSet(false, true)) {
            log.error("Concurrently seeking " + source + " on thread " + Thread.currentThread().getId(), new RuntimeException());
        }
        try {
            if (log.isDebugEnabled())
                log.debug("Seeking " + source + " on thread " + Thread.currentThread().getId());
            source.seek(range, columnFamilies, inclusive);
            threadId = Thread.currentThread().getId();
        } finally {
            if (!inMethod.compareAndSet(true, false)) {
                log.error("Concurrently seeking " + source + " on thread " + Thread.currentThread().getId(), new RuntimeException());
            }
        }
    }

    @Override
    public K getTopKey() {
        if (!inMethod.compareAndSet(false, true)) {
            log.error("Concurrently inspecting " + source + " on thread " + Thread.currentThread().getId(), new RuntimeException());
        }
        try {
            if (threadId != Thread.currentThread().getId()) {
                log.error("Using " + source + " on wrong thread: " + threadId + " vs " + Thread.currentThread().getId(), new RuntimeException());
            }
            return source.getTopKey();
        } finally {
            if (!inMethod.compareAndSet(true, false)) {
                log.error("Concurrently inspecting " + source + " on thread " + Thread.currentThread().getId(), new RuntimeException());
            }
        }
    }

    @Override
    public V getTopValue() {
        if (!inMethod.compareAndSet(false, true)) {
            log.error("Concurrently inspecting " + source + " on thread " + Thread.currentThread().getId(), new RuntimeException());
        }
        try {
            if (threadId != Thread.currentThread().getId()) {
                log.error("Using " + source + " on wrong thread: " + threadId + " vs " + Thread.currentThread().getId(), new RuntimeException());
            }
            return source.getTopValue();
        } finally {
            if (!inMethod.compareAndSet(true, false)) {
                log.error("Concurrently inspecting " + source + " on thread " + Thread.currentThread().getId(), new RuntimeException());
            }
        }
    }

    @Override
    public SortedKeyValueIterator<K,V> deepCopy(IteratorEnvironment env) {
        if (!inMethod.compareAndSet(false, true)) {
            log.error("Concurrently copying " + source + " on thread " + Thread.currentThread().getId(), new RuntimeException());
        }
        try {
            return new SourceThreadTrackingIterator(this, env);
        } finally {
            if (!inMethod.compareAndSet(true, false)) {
                log.error("Concurrently copying " + source + " on thread " + Thread.currentThread().getId(), new RuntimeException());
            }
        }
    }
}
