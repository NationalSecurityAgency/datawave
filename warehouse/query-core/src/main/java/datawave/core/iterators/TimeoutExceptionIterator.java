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
 * Purpose: Timeout catching exception Iterator that will work in conjunction with an iterator that returns the timeout exception Once a timeout is detected,
 * the last key will be returned again with a value set to EXCEPTEDVALUE.
 */
public class TimeoutExceptionIterator extends WrappingIterator {

    /**
     * Various states of this iterator
     */
    private enum STATE {
        NORMAL, // life as usual
        TIMEOUT, // we have detected a timeout
        COMPLETE // nothing left to do
    }

    // The current state of the iterator
    STATE state = STATE.NORMAL;

    // The next key to return
    Key nextKey;

    // Exceeded timeout value exception marker
    public static final Value EXCEPTEDVALUE = new Value(new byte[] {0x0d, 0x0e, 0x0a, 0x0d, 0x0b, 0x0e, 0x0e, 0x0f});

    public static boolean exceededTimedValue(Entry<Key,Value> kv) {
        return kv.getValue().equals(EXCEPTEDVALUE);
    }

    public static boolean exceededTimedValue(Value value) {
        return value.equals(EXCEPTEDVALUE);
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
    }

    @Override
    public boolean hasTop() {
        switch (state) {
            case NORMAL:
                return super.hasTop();
            case TIMEOUT:
                // we have one more key to return
                return true;
            default:
                return false;
        }
    }

    @Override
    public Value getTopValue() {
        switch (state) {
            case NORMAL:
                return super.getTopValue();
            case TIMEOUT:
                return EXCEPTEDVALUE;
            default:
                return null;
        }
    }

    @Override
    public Key getTopKey() {
        switch (state) {
            case NORMAL:
            case TIMEOUT:
                return nextKey;
            default:
                return null;
        }
    }

    @Override
    public void next() throws IOException {
        move(super::next);
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // setup a next key in case we timeout immediately
        if (range.getStartKey() == null) {
            nextKey = new Key();
        } else if (range.isStartKeyInclusive()) {
            nextKey = range.getStartKey();
        } else {
            nextKey = range.getStartKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME);
        }

        move(() -> super.seek(range, columnFamilies, inclusive));
    }

    private void move(IOAction a) throws IOException {
        switch (state) {
            case NORMAL:
                try {
                    a.call();
                    if (super.hasTop()) {
                        nextKey = super.getTopKey();
                    } else {
                        state = STATE.COMPLETE;
                    }
                } catch (IteratorTimeoutException e) {
                    state = STATE.TIMEOUT;
                } catch (RuntimeException e) {
                    if (e.getCause() instanceof IteratorTimeoutException) {
                        state = STATE.TIMEOUT;
                    } else {
                        state = STATE.COMPLETE;
                        throw e;
                    }
                }
                break;
            case TIMEOUT:
                state = STATE.COMPLETE;
        }
    }

    private interface IOAction {
        void call() throws IOException;
    }
}
