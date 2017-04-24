package nsa.datawave.core.iterators;

import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This iterator takes the source iterator (the one below it in the iterator stack) and puts it in a background thread. The background thread continues
 * processing and fills a queue with the Keys and Values from the source iterator. When seek() is called on this iterator, it pauses the background thread,
 * clears the queue, calls seek() on the source iterator, then resumes the thread filling the queue.
 *
 * Users must be aware of the potential for OutOfMemory errors when using this iterator with large queue sizes or large objects. This iterator copies the Key
 * and Value from the source iterator and puts them into the queue.
 *
 * This iterator introduces some parallelism into the server side iterator stack. One use case for this would be when an iterator takes a relatively long time
 * to process each K,V pair and causes the iterators above it to wait. By putting the longer running iterator in a background thread we should be able to
 * achieve greater throughput.
 */
public class ReadAheadIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber, UncaughtExceptionHandler {
    /** Logging Mechanism for the ReadAheadIterator. */
    private static Logger log = Logger.getLogger(ReadAheadIterator.class);
    
    /**
     * Name of the queue-size option.
     *
     * @deprecated this option really shouldn't be used as there is no reason to have a {@code queue size > 1}.
     */
    @Deprecated
    public static final String QUEUE_SIZE = "queue.size";
    
    /** Name of the timeout option. */
    public static final String TIMEOUT = "timeout";
    
    /** An element that goes on the queue when there is no more data. */
    private static final QueueElement NO_DATA = new QueueElement();
    
    /** The inter-thread communication queue. */
    private final ArrayBlockingQueue<QueueElement> _queue = new ArrayBlockingQueue<>(1);
    
    /** The timeout used when offering the communication queue a new message. */
    private int timeout = 1;
    
    /** The source iterator. */
    private SortedKeyValueIterator<Key,Value> _source;
    
    /** The queue element currently being worked on. */
    private QueueElement _elem = new QueueElement();
    
    /** Runnable that produces data from the source iterator. */
    private final Producer _producer = new Producer();
    
    /** The actual Thread running the producer. */
    private Thread _thread;
    
    /** Ensures that the Iterator's Thread and the Producer's Thread are ready to go. */
    private CountDownLatch _syncLatch;
    
    /** Default Constructor */
    public ReadAheadIterator() {}
    
    /** Deep-Copy Constructor. */
    protected ReadAheadIterator(final ReadAheadIterator other, final IteratorEnvironment env) {
        _source = other._source.deepCopy(env);
    }
    
    @Override
    public void init(final SortedKeyValueIterator<Key,Value> source, final Map<String,String> options, final IteratorEnvironment env) throws IOException {
        validateOptions(options);
        _source = source;
    }
    
    /** Seek to the next matching cell and call next to populate the key and value. */
    @Override
    public void seek(final Range range, final Collection<ByteSequence> cf, final boolean inclusive) throws IOException {
        if (_thread == null || (!_producer.hasError() && !_thread.isAlive()))
            readAhead();
        
        if (_producer.hasError())
            throw new IOException("Background thread has error", _producer.getError());
        
        handleSeek(range, cf, inclusive);
    }
    
    /** Populate the key and value. */
    @Override
    public void next() throws IOException {
        if (_thread == null)
            throw new IllegalStateException("Seek must be called before next can be.");
        
        QueueElement nextElement = null;
        while (nextElement == null) {
            if (_producer.hasError())
                throw new IOException("Background thread has error", _producer.getError());
            
            try {
                nextElement = _queue.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.trace("Polling the queue for the next element was interrupted.", e);
            }
        }
        
        _elem = nextElement;
    }
    
    /**
     * Clean implementation of seek. Ensures proper lock usage, throws any errors, assumes that state has been verified.
     *
     * @param range
     *            The range
     * @param cf
     *            The columnFamily
     * @param inclusive
     *            Whether or not the range is inclusive
     * @throws IOException
     */
    private void handleSeek(final Range range, final Collection<ByteSequence> cf, final boolean inclusive) throws IOException {
        _producer.rLock.lock();
        
        try {
            _queue.clear();
            _source.seek(range, cf, inclusive);
        } finally {
            _producer.rLock.unlock();
        }
        
        awaitThreadSync();
        next();
    }
    
    /**
     * Performs a latch countDown(); then waits for the other Thread to do the same.
     *
     * @return whether or not the Threads have successfully synched up.
     */
    private boolean awaitThreadSync() {
        try {
            _syncLatch.countDown();
            return _syncLatch.await(1l, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }
    
    /** Starts up the Producer/Thread */
    private void readAhead() {
        _syncLatch = new CountDownLatch(2);
        _thread = new Thread(_producer, "ReadAheadIterator-SourceThread");
        _thread.setUncaughtExceptionHandler(this);
        _thread.start();
    }
    
    /** {@inheritDoc} */
    @Override
    public IteratorOptions describeOptions() {
        final Map<String,String> options = new HashMap<>();
        options.put(TIMEOUT, "timeout in seconds before background thread thinks that the client has aborted");
        
        return new IteratorOptions(getClass().getSimpleName(), "Iterator that puts the source in another thread", options, null);
    }
    
    /** {@inheritDoc} */
    @Override
    public boolean validateOptions(final Map<String,String> options) {
        try {
            if (options.containsKey(TIMEOUT))
                timeout = Integer.parseInt(options.get(TIMEOUT));
        } catch (final NumberFormatException nfEx) {
            return false;
        }
        
        return true;
    }
    
    /** {@inheritDoc} */
    @Override
    public void uncaughtException(final Thread t, final Throwable e) {
        log.error("Producer thread threw uncaught exception", e);
    }
    
    /** {@inheritDoc} */
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(final IteratorEnvironment env) {
        return new ReadAheadIterator(this, env);
    }
    
    /** {@inheritDoc} */
    @Override
    public Key getTopKey() {
        return _elem.getKey();
    }
    
    /** {@inheritDoc} */
    @Override
    public Value getTopValue() {
        return _elem.getValue();
    }
    
    /** {@inheritDoc} */
    @Override
    public boolean hasTop() {
        return _elem != NO_DATA && (_elem != null || _queue.size() > 0 || _source.hasTop());
    }
    
    /** Thread that produces data from the source iterator and places the results in a queue. */
    private class Producer implements Runnable {
        /* package */final ReentrantLock rLock = new ReentrantLock();
        
        private Exception _srcEx;
        
        public Producer() {}
        
        @Override
        public void run() {
            awaitThreadSync();
            
            try {
                while (_source.hasTop() || _queue.size() > 0)
                    if (_source.hasTop())
                        sendElement();
                    else
                        Thread.yield();
                
                _queue.put(NO_DATA);
            } catch (Exception ex) {
                _srcEx = ex;
            }
        }
        
        private void sendElement() throws IteratorTimeoutException, IOException, InterruptedException {
            rLock.lock();
            
            try {
                final QueueElement elem = new QueueElement(_source.getTopKey(), _source.getTopValue());
                
                if (!_queue.offer(elem, timeout, TimeUnit.SECONDS))
                    throw new IteratorTimeoutException("Background thread has exceeded wait time of " + timeout + " seconds, aborting...");
                
                _source.next();
            } finally {
                rLock.unlock();
            }
        }
        
        public boolean hasError() {
            return _srcEx != null;
        }
        
        public Exception getError() {
            return _srcEx;
        }
    }
    
    /** Class to hold key and value from the producing thread. */
    private static class QueueElement {
        private final Key _key;
        private final Value _value;
        
        public QueueElement(final Key key, final Value value) {
            _key = new Key(key);
            _value = new Value(value.get(), true);
        }
        
        public QueueElement() {
            _key = null;
            _value = null;
        }
        
        public Key getKey() {
            return _key;
        }
        
        public Value getValue() {
            return _value;
        }
    }
}
