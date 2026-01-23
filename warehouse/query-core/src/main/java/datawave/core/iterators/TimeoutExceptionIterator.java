package datawave.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

/**
 * Purpose: Timeout catching exception Iterator that will work in conjunction with an iterator that returns the timeout exception
 */
public class TimeoutExceptionIterator extends WrappingIterator {

    /**
     * Last key that we've received from the iterators below us
     */
    protected Key lastKey = null;

    /**
     * boolean to identify that we've exceeded the time
     */
    boolean exceededTime = false;

    // Exceeded timeout value exception marker
    public static final Value EXCEPTEDVALUE = new Value(new byte[] {0x0d, 0x0e, 0x0a, 0x0d, 0x0b, 0x0e, 0x0e, 0x0f});

    public static boolean exceededTimedValue(Entry<Key,Value> kv) {
        return kv.getValue().equals(EXCEPTEDVALUE);
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
    }

    /**
     * Update internal state to denote a timeout was exceeded
     */
    protected void markTimeoutExceeded() {
        exceededTime = true;
    }

    @Override
    public boolean hasTop() {
        if (exceededTime) {
            return null != lastKey;
        }
        return super.hasTop();
    }

    @Override
    public Value getTopValue() {
        if (exceededTime) {
            return EXCEPTEDVALUE;
        }
        return super.getTopValue();
    }

    @Override
    public Key getTopKey() {
        if (exceededTime) {
            return lastKey;
        }

        lastKey = super.getTopKey();
        return lastKey;
    }

    @Override
    public void next() throws IOException {
        if (exceededTime) {
            return;
        }

        try {
            super.next();
        } catch (IteratorTimeoutException e) {
            markTimeoutExceeded();
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IteratorTimeoutException) {
                markTimeoutExceeded();
            } else {
                throw e;
            }
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {

        lastKey = new Key();

        if (exceededTime) {
            return;
        }

        try {
            super.seek(range, columnFamilies, inclusive);
        } catch (IteratorTimeoutException e) {
            markTimeoutExceeded();
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IteratorTimeoutException) {
                markTimeoutExceeded();
            } else {
                throw e;
            }
        }
    }
}
