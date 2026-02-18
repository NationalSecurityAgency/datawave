package datawave.query.index.lookup;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An iterator that can scan a global index of truncated keys in the form:
 *
 * <pre>
 *     value FIELD:yyyyMMdd0x00datatype VIZ (bitset value)
 * </pre>
 *
 * This iterator will aggregate bitsets for a given day across all datatypes and visibilities, returning the combined bitset as a single value
 */
public class TruncatedIndexIterator implements SortedKeyValueIterator<Key,Value> {

    private static final Logger log = LoggerFactory.getLogger(TruncatedIndexIterator.class);

    protected SortedKeyValueIterator<Key,Value> source;
    protected Key tk;
    protected Value tv = null;
    protected BitSet bitset = null;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
    }

    @Override
    public boolean hasTop() {
        return tk != null;
    }

    @Override
    public void next() throws IOException {
        this.tk = null;
        this.tv = null;
        this.bitset = null;
        if (source.hasTop()) {
            tk = source.getTopKey();

            String cq = tk.getColumnQualifier().toString();
            String date = cq.substring(0, cq.indexOf('\u0000'));

            bitset = BitSet.valueOf(source.getTopValue().get());
            source.next();

            while (source.hasTop() && sameDay(date, source.getTopKey())) {
                BitSet candidate = BitSet.valueOf(source.getTopValue().get());
                bitset.or(candidate);
                source.next();
            }

            tv = new Value(bitset.toByteArray());
        }
    }

    protected boolean sameDay(String date, Key next) {
        String nextCQ = next.getColumnQualifier().toString();
        return nextCQ.startsWith(date);
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (!range.isStartKeyInclusive()) {
            Key start = range.getStartKey();
            Key next = new Key(start.getRow(), start.getColumnFamily(), new Text(start.getColumnQualifier() + "\u0000\uFFFF"));
            range = new Range(next, true, range.getEndKey(), range.isEndKeyInclusive());
        }

        source.seek(range, columnFamilies, inclusive);
        next();
    }

    @Override
    public Key getTopKey() {
        return tk;
    }

    @Override
    public Value getTopValue() {
        return tv;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        TruncatedIndexIterator copy = new TruncatedIndexIterator();
        copy.source = source.deepCopy(env);
        return copy;
    }
}
