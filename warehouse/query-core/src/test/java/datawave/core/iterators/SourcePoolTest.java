package datawave.core.iterators;

import datawave.query.iterator.SourceFactory;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * Test the source pool logic
 */
public class SourcePoolTest {
    Logger log = Logger.getLogger(RangeSplitterTest.class);
    
    private int concurrentDeepCopies = 0;
    private int copies = 0;
    private MyFactory factory = null;
    
    private class MySource implements SortedKeyValueIterator<Key,Value> {
        
        @Override
        public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {}
        
        @Override
        public boolean hasTop() {
            return false;
        }
        
        @Override
        public void next() throws IOException {}
        
        @Override
        public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {}
        
        @Override
        public Key getTopKey() {
            return null;
        }
        
        @Override
        public Value getTopValue() {
            return null;
        }
        
        @Override
        public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
            concurrentDeepCopies++;
            try {
                if (concurrentDeepCopies != 1) {
                    throw new IllegalStateException("not synchronized around deepCopies");
                }
                copies++;
                Thread.sleep(10);
                if (concurrentDeepCopies != 1) {
                    throw new IllegalStateException("not synchronized around deepCopies");
                }
                return new MySource();
            } catch (InterruptedException ie) {
                throw new IllegalStateException("Interrupted");
            } finally {
                concurrentDeepCopies--;
            }
        }
    }
    
    private class MyFactory implements SourceFactory<Key,Value> {
        MySource source = new MySource();
        
        @Override
        public SortedKeyValueIterator<Key,Value> getSourceDeepCopy() {
            return source.deepCopy(null);
        }
    }
    
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        copies = 0;
        factory = new MyFactory();
    }
    
    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        factory = null;
        copies = 0;
    }
    
    /**
     * Test method for {@link datawave.core.iterators.RangeSplitter#RangeSplitter(org.apache.accumulo.core.data.Range, int)}.
     */
    @Test
    public void testSourcePool() {
        Set<SortedKeyValueIterator<Key,Value>> sources = new HashSet<>();
        SourcePool<Key,Value> pool = new SourcePool(factory, 10);
        for (int i = 0; i < 10; i++) {
            SortedKeyValueIterator<Key,Value> source = pool.checkOut();
            Assert.assertNotNull(source);
            Assert.assertFalse(sources.contains(source));
            sources.add(source);
            Assert.assertEquals(i + 1, copies);
            Assert.assertNotEquals(factory.source, source);
        }
        for (int i = 0; i < 10; i++) {
            SortedKeyValueIterator<Key,Value> source = pool.checkOut();
            Assert.assertNull(source);
        }
        Assert.assertEquals(10, copies);
        for (SortedKeyValueIterator<Key,Value> source : sources) {
            pool.checkIn(source);
        }
        Assert.assertEquals(10, copies);
        Set<SortedKeyValueIterator<Key,Value>> sources2 = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            SortedKeyValueIterator<Key,Value> source = pool.checkOut();
            Assert.assertNotNull(source);
            Assert.assertFalse(sources2.contains(source));
            sources2.add(source);
            Assert.assertNotEquals(factory.source, source);
        }
        Assert.assertEquals(10, copies);
        Assert.assertEquals(sources, sources2);
        for (int i = 0; i < 10; i++) {
            SortedKeyValueIterator<Key,Value> source = pool.checkOut();
            Assert.assertNull(source);
        }
    }
    
    /**
     * Test method for {@link datawave.core.iterators.RangeSplitter#RangeSplitter(org.apache.accumulo.core.data.Range, int)}.
     */
    @Test
    public void testDeepCopySynchronization() {
        final Set<SortedKeyValueIterator<Key,Value>> sources = new HashSet<>();
        final SourcePool<Key,Value> pool = new SourcePool(factory, 100);
        
        final List<Throwable> failed = new ArrayList<Throwable>();
        final List<Thread> threads = new ArrayList<Thread>();
        
        for (int i = 0; i < 100; i++) {
            Thread thread = new Thread(() -> {
                try {
                    SortedKeyValueIterator<Key,Value> source = pool.checkOut();
                    Assert.assertNotNull(source);
                    Assert.assertFalse(sources.contains(source));
                    sources.add(source);
                    Assert.assertNotEquals(factory.source, source);
                } catch (Throwable e) {
                    synchronized (failed) {
                        e.printStackTrace();
                        failed.add(e);
                    }
                }
            });
            thread.setUncaughtExceptionHandler((t, e) -> {
                synchronized (failed) {
                    e.printStackTrace();
                    failed.add(e);
                }
            });
            thread.start();
            threads.add(thread);
        }
        
        for (Thread thread : threads) {
            while (thread.isAlive()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ie) {
                    // Do nothing
                }
            }
        }
        threads.clear();
        
        Assert.assertTrue(failed.isEmpty());
        Assert.assertEquals(100, copies);
        
        for (int i = 0; i < 10; i++) {
            SortedKeyValueIterator<Key,Value> source = pool.checkOut();
            Assert.assertNull(source);
        }
        Assert.assertEquals(100, copies);
        
        for (SortedKeyValueIterator<Key,Value> source : sources) {
            final SortedKeyValueIterator<Key,Value> threadSource = source;
            Thread thread = new Thread(() -> {
                try {
                    pool.checkIn(threadSource);
                } catch (Throwable e) {
                    synchronized (failed) {
                        e.printStackTrace();
                        failed.add(e);
                    }
                }
            });
            thread.setUncaughtExceptionHandler((t, e) -> {
                synchronized (failed) {
                    e.printStackTrace();
                    failed.add(e);
                }
            });
            thread.start();
            threads.add(thread);
        }
        
        for (Thread thread : threads) {
            while (thread.isAlive()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ie) {
                    // Do nothing
                }
            }
        }
        threads.clear();
        
        Assert.assertTrue(failed.isEmpty());
        Assert.assertEquals(100, copies);
    }
    
    /**
     * Test method for {@link datawave.core.iterators.RangeSplitter#RangeSplitter(org.apache.accumulo.core.data.Range, int)}.
     */
    @Test
    public void testSourcePoolWait() throws InterruptedException {
        final Set<SortedKeyValueIterator<Key,Value>> sources = new HashSet<>();
        final SourcePool<Key,Value> pool = new SourcePool(factory, 10);
        for (int i = 0; i < 10; i++) {
            SortedKeyValueIterator<Key,Value> source = pool.checkOut();
            sources.add(source);
        }
        
        for (int i = 0; i < 10; i++) {
            SortedKeyValueIterator<Key,Value> source = pool.checkOut(i + 10);
            Assert.assertNull(source);
        }
        
        final List<Throwable> failed = new ArrayList<Throwable>();
        
        final MutableBoolean waitingForCheckOut = new MutableBoolean(true);
        final SortedKeyValueIterator<Key,Value> threadSource = sources.iterator().next();
        sources.remove(threadSource);
        Thread thread = new Thread(() -> {
            try {
                SortedKeyValueIterator<Key,Value> source = pool.checkOut(-1);
                waitingForCheckOut.setValue(false);
                Assert.assertNotNull(source);
                Assert.assertFalse(sources.contains(source));
                sources.add(source);
                Assert.assertNotEquals(factory.source, source);
            } catch (Throwable e) {
                synchronized (failed) {
                    e.printStackTrace();
                    failed.add(e);
                }
            }
        });
        thread.setUncaughtExceptionHandler((t, e) -> {
            synchronized (failed) {
                e.printStackTrace();
                failed.add(e);
            }
        });
        
        thread.start();
        
        for (int i = 0; i < 10; i++) {
            Thread.sleep(100);
            Assert.assertTrue(waitingForCheckOut.booleanValue());
            Assert.assertTrue(thread.isAlive());
            Assert.assertTrue(failed.isEmpty());
        }
        
        pool.checkIn(threadSource);
        
        int count = 0;
        while (thread.isAlive()) {
            try {
                count++;
                Thread.sleep(10);
                if (count > 1000) {
                    Assert.fail("Expected thread to complete");
                    break;
                }
            } catch (InterruptedException ie) {
                // Do nothing
            }
        }
        
        Assert.assertFalse(waitingForCheckOut.booleanValue());
        Assert.assertTrue(failed.isEmpty());
    }
    
}
