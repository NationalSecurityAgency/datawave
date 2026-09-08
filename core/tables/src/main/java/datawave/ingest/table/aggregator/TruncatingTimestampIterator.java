package datawave.ingest.table.aggregator;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import datawave.util.CompositeTimestamp;

public class TruncatingTimestampIterator implements SortedKeyValueIterator<Key,Value> {

    private final SortedKeyValueIterator<Key,Value> delegate;

    public TruncatingTimestampIterator(SortedKeyValueIterator<Key,Value> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        delegate.init(source, options, env);
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new TruncatingTimestampIterator(delegate.deepCopy(env));
    }

    @Override
    public Key getTopKey() {
        return getTruncatedTimestampKey(delegate.getTopKey());
    }

    @Override
    public Value getTopValue() {
        return delegate.getTopValue();
    }

    @Override
    public boolean hasTop() {
        return delegate.hasTop();
    }

    @Override
    public void next() throws IOException {
        delegate.next();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        delegate.seek(range, columnFamilies, inclusive);
    }

    /**
     * Return the key with a truncated timestamp
     *
     * @param key
     *            The key
     * @return The key with a truncated timestamp
     */
    public static Key getTruncatedTimestampKey(Key key) {
        if (key == null) {
            return key;
        }

        long timestamp = key.getTimestamp();
        long newTimestamp = truncateTimestamp(timestamp);
        if (timestamp == newTimestamp) {
            return key;
        }

        return new Key(key.getRow().getBytes(), key.getColumnFamily().getBytes(), key.getColumnQualifier().getBytes(), key.getColumnVisibility().getBytes(),
                        newTimestamp, key.isDeleted(), false);
    }

    /**
     * truncate a timestamp (possibly composite timestamp) to the beginning of the day.
     *
     * @param timestamp
     *            timestamp (possibly composite timestamp)
     * @return timestamp with the event date truncated to the beginning of the day
     */
    public static long truncateTimestamp(long timestamp) {
        if (CompositeTimestamp.isCompositeTimestamp(timestamp)) {
            long eventDate = CompositeTimestamp.getEventDate(timestamp);
            int ageOffDeltaDays = CompositeTimestamp.getAgeOffDeltaDays(timestamp);
            return CompositeTimestamp.getCompositeDeltaTimeStamp(beginningOfDay(eventDate), ageOffDeltaDays);
        } else {
            return beginningOfDay(timestamp);
        }
    }

    /**
     * Return the beginning of the day for a timestamp
     *
     * @param timestamp
     *            timestamp
     * @return beginning of the day
     */
    public static long beginningOfDay(long timestamp) {
        Instant truncated = Instant.ofEpochMilli(timestamp).truncatedTo(ChronoUnit.DAYS);
        return truncated.toEpochMilli();
    }

}
