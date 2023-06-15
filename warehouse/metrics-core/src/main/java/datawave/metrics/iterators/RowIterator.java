package datawave.metrics.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

/**
 * This iterator groups several entries from a single row into a single entry.<br>
 * One must take care using this on data where a single row will not fit efficiently into memory.<br>
 * This iterator is ideal for constructing a row iterators that perform SQL-like functions or that transform an entire row.<br>
 *
 */
public abstract class RowIterator extends WrappingIterator implements OptionDescriber {

    private static final Logger log = Logger.getLogger(RowIterator.class);

    private boolean bypass = false;
    private Key topKey = null;
    private Value topValue = null;
    private SortedMap<Key,Value> queue = new TreeMap<>();

    @Override
    public Key getTopKey() {
        return bypass ? super.getTopKey() : topKey;
    }

    @Override
    public Value getTopValue() {
        return bypass ? super.getTopValue() : topValue;
    }

    @Override
    public boolean hasTop() {
        if (bypass)
            return super.hasTop();
        try {
            // try to update the queue to see if there will be more data to share
            checkAndUpdateTop();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return topKey != null;
    }

    @Override
    public void next() throws IOException {

        if (bypass) {
            super.next();
            return;
        }

        // clear the old top
        topKey = null;
        topValue = null;

        // update the queue with another row, if necessary
        checkAndUpdateTop();
    }

    private void checkAndUpdateTop() throws IOException {
        while (queue.isEmpty() && getSource().hasTop()) {
            do {
                // get the source entry
                Key k = getSource().getTopKey();
                Value v = getSource().getTopValue();

                // check to see if it goes to the same row as the current row (or create
                // a new one)
                // otherwise, jump out of the loop and process the current row
                if (queue.isEmpty() || queue.firstKey().getRow().equals(k.getRow())) {
                    queue.put(new Key(k), new Value(v));
                    getSource().next();
                } else
                    break;
            } while (getSource().hasTop());

            // perform the processing of the entire row
            if (!queue.isEmpty())
                processRow(queue);
        }
        // update the top from the queue
        if (topKey == null && !queue.isEmpty()) {
            topKey = queue.firstKey();
            topValue = queue.remove(topKey);
        }
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        // MockAccumulo seems to throw in a null env, so this check is here
        if (env == null) {
            bypass = false;
        } else {
            IteratorScope scope = env.getIteratorScope();
            bypass = scope.equals(IteratorScope.minc) || (scope.equals(IteratorScope.majc) && !env.isFullMajorCompaction());
        }
        if (bypass)
            log.info(this.getClass().getSimpleName() + " only works for scans and full major compactions;"
                            + " Note that full major compactions only work if the entire row is contained in a single locality group.");
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        super.seek(range, columnFamilies, inclusive);
        if (!bypass)
            queue.clear();
    }

    /**
     * Perform processing on the row<br>
     * Note: the processor *can* modify the row in the process<br>
     * The state of the row when this method ends is the set of key/value pairs to pass up the stack<br>
     *
     * This method can do selection, projection, rename columns, combine columns, etc. Note: this may affect the sort order; to ensure that it doesn't, avoid
     * modifying the row portion of the keys
     *
     * @param row
     *            All entries for a given row in the order they appear in the underlying source; initially non-empty
     */
    protected abstract void processRow(SortedMap<Key,Value> row);

}
