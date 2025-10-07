package datawave.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import datawave.ingest.protobuf.Uid;

/**
 *
 * Iterator that will return unique terms and their aggregated count for a range over the global index
 *
 */
public class GlobalIndexRangeSamplingIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {

    private static Logger log = Logger.getLogger(GlobalIndexRangeSamplingIterator.class);

    private SortedKeyValueIterator<Key,Value> iterator = null;
    private Key key = null;
    private Value value = null;

    public GlobalIndexRangeSamplingIterator() {}

    public GlobalIndexRangeSamplingIterator(GlobalIndexRangeSamplingIterator iter, IteratorEnvironment env) {
        this.iterator = iter.deepCopy(env);
    }

    public IteratorOptions describeOptions() {
        Map<String,String> options = Collections.emptyMap();
        return new IteratorOptions(getClass().getSimpleName(), "returns aggregated index ranges for each day", options, null);
    }

    public boolean validateOptions(Map<String,String> options) {
        return true;
    }

    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (!validateOptions(options))
            throw new IOException("Iterator options are not correct");
        this.iterator = source;
    }

    public boolean hasTop() {
        return (key != null);
    }

    public void next() throws IOException {
        key = null;
        value = null;
        if (this.iterator.hasTop())
            findTop();
    }

    public void findTop() throws IOException {

        long count = 0;

        // Sum the Uid.List (value) count for this term (row)
        // Copy the starting key or this won't work....
        Key startKey = new Key(this.iterator.getTopKey());

        do {
            // Get the shard id and datatype from the colq
            String colq = this.iterator.getTopKey().getColumnQualifier().toString();
            // Parse the UID.List object from the value
            Uid.List uidList = null;
            try {
                uidList = Uid.List.parseFrom(this.iterator.getTopValue().get());
                // Add the count for this shard to the total count for the term.
                count += uidList.getCOUNT();
            } catch (InvalidProtocolBufferException e) {
                count = Long.MAX_VALUE;
                log.debug("Error deserializing Uid.List at: " + this.iterator.getTopKey());
                break;
            }

            this.iterator.next();
        } while (this.iterator.hasTop() && startKey.equals(this.iterator.getTopKey(), PartialKey.ROW));

        key = new Key(startKey);
        value = new Value(Long.toString(count).getBytes());
    }

    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.iterator.seek(range, columnFamilies, inclusive);
        if (this.iterator.hasTop())
            findTop();
    }

    public Key getTopKey() {
        return key;
    }

    public Value getTopValue() {
        return value;
    }

    public GlobalIndexRangeSamplingIterator deepCopy(IteratorEnvironment env) {
        return new GlobalIndexRangeSamplingIterator(this, env);
    }

}
