package nsa.datawave.test.helpers;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockScanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.google.common.collect.ForwardingIterator;

/**
 * <p>
 * This mock scanner can be interrupted at any time, refreshing/cloning the iterator and resetting the range. This is to help to test tear down scenarios, the
 * deep copy usage and sequential key output.
 * </p>
 * 
 * <p>
 * Create this scanner with an existing <code>MockScanner</code> from the mock package. At any point in testing a new iterator can be generated using the the
 * interrupt method. The old iterator will still be usable but will not actually test the interruption scenario.</>
 * 
 * <p>
 * Example usage
 * </p>
 * 
 * <pre>
 * Scanner s = conn.createScanner(TABLE_NAME, authorizations);
 * InterruptibleScanner scanner = new InterruptibleScanner(s);
 * // TODO: Configure scanner, set range
 * Iterator&lt;Entry&lt;Key,Value&gt;&gt; iter = scanner.iterator();
 * // TODO: iterator over key values, test some values
 * iter = scanner.interrupt(); // Simulate a tear down
 * // TODO: test this value is the expected next value, has not returned a duplicate
 * </pre>
 */
public class InterruptibleScanner implements Scanner {
    
    // Hold onto the iterator so we can clone it later
    LastKeyIterator inner;
    
    Scanner delegate;
    
    public InterruptibleScanner(Scanner scanner) {
        delegate = scanner;
    }
    
    /**
     * Interrupt the iterator by resetting the range to the last returned key, exclusive
     * 
     * @return
     */
    public Iterator<Entry<Key,Value>> interrupt() {
        Range interruptPoint = new Range(inner.getLastKey(), false, getRange().getEndKey(), getRange().isEndKeyInclusive());
        setRange(interruptPoint);
        return iterator();
    }
    
    /**
     * Wrap the tablet server iterator in a java iterator and maintain a hook so we can get a previous key at any time Build the iterator with the entire range
     * but start the top at the interruption point
     */
    @Override
    public Iterator<Entry<Key,Value>> iterator() {
        inner = new LastKeyIterator(delegate.iterator());
        return inner;
    }
    
    /**
     * Wrap an iterator to save the last emitted key
     */
    public static class LastKeyIterator extends ForwardingIterator<Entry<Key,Value>> {
        Iterator<Entry<Key,Value>> delegate;
        Key lastKey;
        
        public LastKeyIterator(Iterator<Entry<Key,Value>> other) {
            delegate = other;
        }
        
        @Override
        protected Iterator<Entry<Key,Value>> delegate() {
            return delegate;
        }
        
        @Override
        public Entry<Key,Value> next() {
            Entry<Key,Value> entry = delegate.next();
            lastKey = entry.getKey();
            return entry;
        }
        
        public Key getLastKey() {
            return lastKey;
        }
    }
    
    @Override
    public void addScanIterator(IteratorSetting cfg) {
        delegate.addScanIterator(cfg);
    }
    
    @Override
    public void removeScanIterator(String iteratorName) {
        delegate.removeScanIterator(iteratorName);
    }
    
    @Override
    public void updateScanIteratorOption(String iteratorName, String key, String value) {
        delegate.updateScanIteratorOption(iteratorName, key, value);
    }
    
    @Override
    public void fetchColumnFamily(Text col) {
        delegate.fetchColumnFamily(col);
    }
    
    @Override
    public void fetchColumn(Text colFam, Text colQual) {
        delegate.fetchColumn(colFam, colQual);
    }
    
    @Override
    public void clearColumns() {
        delegate.clearColumns();
    }
    
    @Override
    public void clearScanIterators() {
        delegate.clearScanIterators();
    }
    
    @Override
    public void setTimeout(long timeOut, TimeUnit timeUnit) {
        delegate.setTimeout(timeOut, timeUnit);
    }
    
    @Override
    public long getTimeout(TimeUnit timeUnit) {
        return delegate.getTimeout(timeUnit);
    }
    
    @Override
    public void close() {
        delegate.close();
    }
    
    @Override
    @Deprecated
    public void setTimeOut(int timeOut) {
        delegate.setTimeOut(timeOut);
    }
    
    @Override
    @Deprecated
    public int getTimeOut() {
        return delegate.getTimeOut();
    }
    
    @Override
    public void setRange(Range range) {
        delegate.setRange(range);
    }
    
    @Override
    public Range getRange() {
        return delegate.getRange();
    }
    
    @Override
    public void setBatchSize(int size) {
        delegate.setBatchSize(size);
    }
    
    @Override
    public int getBatchSize() {
        return delegate.getBatchSize();
    }
    
    @Override
    public void enableIsolation() {
        delegate.enableIsolation();
    }
    
    @Override
    public void disableIsolation() {
        delegate.disableIsolation();
    }
    
    @Override
    public long getReadaheadThreshold() {
        return delegate.getReadaheadThreshold();
    }
    
    @Override
    public void setReadaheadThreshold(long batches) {
        delegate.setReadaheadThreshold(batches);
    }
    
    @Override
    public void setContext(String ctx) {
        throw new UnsupportedOperationException("not supported for this test");
    }
    
    @Override
    public String getContext() {
        throw new UnsupportedOperationException("not supported for this test");
    }
    
    @Override
    public void clearContext() {
        throw new UnsupportedOperationException("not supported for this test");
    }
}
