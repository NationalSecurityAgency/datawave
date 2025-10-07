package datawave.metrics.iterators;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * Scans over a row and returns the maximum column family and column qualifier values.
 *
 * For simplicity's sake, this iterator assumes that the column family and column qualifier are integers.
 *
 */
public class MaxAttributeIterator extends RowIterator {

    private static final Map<String,String> esm = Collections.emptyMap();
    private static final List<String> esl = Collections.emptyList();
    private static final IteratorOptions OPTIONS = new IteratorOptions("MaxAttributeIterator",
                    "This iterator scans a row and returns a Key with the maximal column family and qualifier values.", esm, esl);
    private static final Value emptyValue = new Value(new byte[0]);

    /**
     * Returns a description and no configuration options.
     */
    @Override
    public IteratorOptions describeOptions() {
        return OPTIONS;
    }

    /**
     * There are no options for this iterator.
     */
    @Override
    public boolean validateOptions(Map<String,String> arg0) {
        return true;
    }

    @Override
    protected void processRow(SortedMap<Key,Value> row) {
        String rowId = row.firstKey().getRow().toString();
        long fMax = Long.MIN_VALUE, qMax = Long.MIN_VALUE;
        for (Key k : row.keySet()) {
            long f = Long.parseLong(k.getColumnFamily().toString());
            if (f > fMax) {
                fMax = f;
            }
            long q = Long.parseLong(k.getColumnQualifier().toString());
            if (q > qMax) {
                qMax = q;
            }
        }
        Key myKey = new Key(rowId, Long.toString(fMax), Long.toString(qMax));
        row.clear();
        row.put(myKey, emptyValue);
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        MaxAttributeIterator itr = new MaxAttributeIterator();
        itr.setSource(this.getSource().deepCopy(env));
        return itr;
    }

}
