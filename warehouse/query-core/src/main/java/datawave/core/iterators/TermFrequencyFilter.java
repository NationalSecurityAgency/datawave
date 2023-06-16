package datawave.core.iterators;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.SeekingFilter;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Alternate implementation of the {@link TermFrequencyIterator} that operates on precomputed column qualifiers.
 *
 * Term Frequency keys take the form {shard:tf:datatype\0uid\0value\0field}.
 *
 * This iterator fetches offsets for known datatype, uid, value, and field combinations. It does not support fetching offsets for a partial column qualifier.
 */
public class TermFrequencyFilter extends SeekingFilter {

    // A set of full column qualifiers that describe the offset's exact location (datatype\0uid\0value\0FIELD)
    private final SortedSet<Text> searchSpace;
    // The last hint which is past the end of the seek range
    private Key lastHint;

    private static final Collection<ByteSequence> SEEK_CFS = Collections.singleton(new ArrayByteSequence("tf".getBytes()));
    private static final Map<String,String> OPTIONS = Collections.singletonMap(NEGATE, "false");

    public TermFrequencyFilter(SortedSet<Text> searchSpace) {
        this.searchSpace = searchSpace;
    }

    public TermFrequencyFilter(TermFrequencyFilter other, IteratorEnvironment env) {
        this.searchSpace = other.searchSpace;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, OPTIONS, env);
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        lastHint = range.getEndKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL);
        super.seek(range, SEEK_CFS, true);
    }

    @Override
    public FilterResult filter(Key k, Value v) {
        return new FilterResult(searchSpace.contains(k.getColumnQualifier()), AdvanceResult.USE_HINT);
    }

    @Override
    public Key getNextKeyHint(Key k, Value v) {
        Text nextCQ = ((TreeSet<Text>) searchSpace).higher(k.getColumnQualifier());
        if (nextCQ == null)
            return lastHint;

        return new Key(k.getRow(), k.getColumnFamily(), nextCQ);
    }

    // other overrides

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new TermFrequencyFilter(this, env);
    }
}
