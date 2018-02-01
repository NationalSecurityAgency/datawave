package datawave.core.iterators;

import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import datawave.query.iterator.SourceFactory;

/**
 * This is a pool of sources that is created with a specified size, from which sources can be checked out, and then checked back in for subsequent use. The is
 * needed to alleviate excessive sources being created within the tservers.
 */
public class SourcePool<K extends WritableComparable<?>,V extends Writable> {
    private Logger log = Logger.getLogger(SourcePool.class);
    private SortedKeyValueIterator<K,V>[] sources;
    private volatile int checkedIn = 0;
    private volatile int created = 0;
    private SourceFactory<K,V> sourceFactory;
    private Object monitor = new Object();
    
    public SourcePool(SourceFactory<K,V> sourceFactory, int size) {
        // create the initial deep copied source, used for subsequence deep copies and can be checked out only after <size-1> sources have been created
        this.sourceFactory = sourceFactory;
        this.sources = new SortedKeyValueIterator[size];
    }
    
    public SortedKeyValueIterator<K,V> checkOut() {
        if (log.isTraceEnabled()) {
            log.trace("enter checkOut: " + this);
        }
        SortedKeyValueIterator<K,V> source = null;
        synchronized (monitor) {
            if (checkedIn > 0) {
                log.trace("checking out previously created source");
                source = sources[--checkedIn];
            } else if (created < sources.length) {
                created++;
                log.trace("creating new deepcopy");
                source = sourceFactory.getSourceDeepCopy();
            } else {
                log.trace("no available source to checkout");
            }
        }
        if (log.isTraceEnabled()) {
            log.trace("exit checkOut: " + this + " / " + source);
        }
        return source;
    }
    
    public SortedKeyValueIterator<K,V> checkOut(int waitMs) {
        if (log.isTraceEnabled()) {
            log.trace("enter checkOut(" + waitMs + "): " + this);
        }
        synchronized (monitor) {
            SortedKeyValueIterator<K,V> source = checkOut();
            if (source != null) {
                if (log.isTraceEnabled()) {
                    log.trace("exit checkOut(" + waitMs + "): " + this + " / " + source);
                }
                return source;
            }
            boolean done = false;
            while (!done) {
                try {
                    if (log.isTraceEnabled()) {
                        log.trace("wait checkOut( " + waitMs + "): " + this);
                    }
                    if (waitMs <= 0) {
                        monitor.wait();
                    } else {
                        monitor.wait(waitMs);
                    }
                    if (log.isTraceEnabled()) {
                        log.trace("wakeup checkOut( " + waitMs + "): " + this);
                    }
                } catch (InterruptedException e) {
                    log.warn("Interrupted while waiting for source", e);
                    return null;
                }
                source = checkOut();
                if (source != null || waitMs > 0) {
                    done = true;
                } else {
                    if (log.isTraceEnabled()) {
                        log.error("failure on checkOut( " + waitMs + "): " + this + " / somebody swiped my source");
                    }
                }
            }
            if (log.isTraceEnabled()) {
                log.trace("exit checkOut(" + waitMs + "): " + this + " / " + source);
            }
            return source;
        }
    }
    
    public void checkIn(SortedKeyValueIterator<K,V> source) {
        synchronized (monitor) {
            if (log.isTraceEnabled()) {
                log.trace("enter checkIn: " + this + " / " + source);
            }
            if (checkedIn == created) {
                log.error("Checking in more sources than were created (" + this + ')');
                throw new IllegalStateException("Checking in more sources than were created (" + this + ')');
            }
            sources[checkedIn++] = source;
            if (log.isTraceEnabled()) {
                log.trace("exit checkIn: " + this + " / " + source);
            }
            monitor.notify();
        }
    }
    
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(Thread.currentThread().getId());
        synchronized (monitor) {
            builder.append(": checkedIn/created/capacity = ").append(checkedIn).append('/').append(created).append('/').append(sources.length);
        }
        return builder.toString();
    }
    
    public SourcePool deepCopy() {
        return new SourcePool(this.sourceFactory, this.sources.length);
    }
    
}
