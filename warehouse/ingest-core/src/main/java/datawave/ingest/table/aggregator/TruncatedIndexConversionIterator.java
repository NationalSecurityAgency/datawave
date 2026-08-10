package datawave.ingest.table.aggregator;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.ingest.table.aggregator.util.TruncatedIndexKeyParser;

/**
 * This iterator can transform a global index table to a truncated index where the shard number becomes an index in a bitset
 * <p>
 * Although the underlying {@link TruncatedIndexKeyParser} supports parsing sharded keys this iterator only assumes two key formats: standard and truncated.
 */
public class TruncatedIndexConversionIterator implements SortedKeyValueIterator<Key,Value> {

    private static final Logger log = LoggerFactory.getLogger(TruncatedIndexConversionIterator.class);

    protected SortedKeyValueIterator<Key,Value> delegate;
    protected final TreeMap<Key,BitSet> buffer = new TreeMap<>();
    protected Key tk = null;
    protected Value tv = null;

    // dedicated parser for the first key seen
    private final TruncatedIndexKeyParser tkParser = new TruncatedIndexKeyParser();
    // dedicated parser for all subsequent matching keys
    private final TruncatedIndexKeyParser nextParser = new TruncatedIndexKeyParser();

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> option, IteratorEnvironment env) throws IOException {
        this.delegate = source;
    }

    @Override
    public boolean hasTop() {
        if (tk == null && buffer.isEmpty() && delegate.hasTop()) {
            try {
                fillBuffer();
                next();
            } catch (IOException e) {
                log.error("failed to fill buffer");
                log.error(e.getMessage(), e);
            }
        }

        return tk != null;
    }

    @Override
    public void next() throws IOException {
        if (!buffer.isEmpty()) {
            Map.Entry<Key,BitSet> entry = buffer.pollFirstEntry();
            tk = entry.getKey();
            tv = new Value(entry.getValue().toByteArray());
        } else {
            tk = null;
            tv = null;
        }
    }

    protected void fillBuffer() throws IOException {
        if (delegate.hasTop()) {
            Key top = delegate.getTopKey();
            tkParser.parse(top);

            buffer.clear();
            if (tkParser.isTruncatedKey()) {
                addToBuffer(top, BitSet.valueOf(delegate.getTopValue().get()));
            } else {
                addToBuffer(tkParser.convert(), tkParser.getBitset());
            }

            delegate.next();

            while (delegate.hasTop()) {
                Key k = delegate.getTopKey();
                nextParser.parse(k);

                if (!tkParser.getDate().equals(nextParser.getDate())) {
                    break; // dates don't match, stop aggregating keys
                }

                if (nextParser.isTruncatedKey()) {
                    addToBuffer(k, BitSet.valueOf(delegate.getTopValue().get()));
                } else {
                    Key truncated = nextParser.convert();
                    addToBuffer(truncated, nextParser.getBitset());
                }

                delegate.next();
            }
        }
    }

    protected void addToBuffer(Key key, BitSet bitset) {
        if (buffer.containsKey(key)) {
            BitSet original = buffer.get(key);
            original.or(bitset);
            buffer.put(key, original);
        } else {
            buffer.put(key, bitset);
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        delegate.seek(range, columnFamilies, inclusive);
        hasTop();
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
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment iteratorEnvironment) {
        TruncatedIndexConversionIterator copy = new TruncatedIndexConversionIterator();
        copy.delegate = delegate.deepCopy(iteratorEnvironment);
        copy.tk = tk;
        copy.tv = tv;
        copy.buffer.putAll(buffer);
        return copy;
    }
}
