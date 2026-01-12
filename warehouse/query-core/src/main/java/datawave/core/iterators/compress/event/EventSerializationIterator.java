package datawave.core.iterators.compress.event;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.iterators.compress.KeyGroup;
import datawave.query.tld.TLD;

/**
 * An iterator that serializes event keys into the {@link Value}, or acts as pass-through for non-event keys.
 * <p>
 * Event keys are grouped according to the row and column family, and further partitioned by the {@link EventKeyComparator}.
 * <p>
 * The {@link EventSerializationUtil} handles serialization. The serialization strategy is configurable depending on a system's specific needs.
 * <p>
 * The serialized events can be further compressed if the total size exceeds a specific byte threshold.
 */
public class EventSerializationIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {

    private static final Logger log = LoggerFactory.getLogger(EventSerializationIterator.class);

    private SortedKeyValueIterator<Key,Value> source;

    public static final String VERSION_OPT = "version";
    public static final String THRESHOLD_OPT = "threshold";
    public static final String ALGORITHM_OPT = "algorithm";

    private final EventSerializationUtil serializationUtil = new EventSerializationUtil();

    private Key tk;
    private Value tv;
    private final TreeSet<Pair<Key,Value>> buffer = new TreeSet<>();

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
        validateOptions(options);
    }

    @Override
    public boolean hasTop() {
        return tk != null;
    }

    @Override
    public void next() throws IOException {
        tk = null;
        tv = null;

        if (!buffer.isEmpty()) {
            Pair<Key,Value> top = buffer.pollFirst();
            if (top != null) {
                this.tk = top.getKey();
                this.tv = top.getValue();
            }
            return;
        }

        if (source.hasTop()) {
            // pass-through
            tk = source.getTopKey();
            tv = source.getTopValue();

            if (EventSerializationUtil.isEventKey(tk)) {

                // NOTE: this call WILL advance the source iterator.
                KeyGroup compressed = serializationUtil.serialize(source);
                this.buffer.addAll(compressed.getKeyValues());

                Pair<Key,Value> top = buffer.pollFirst();
                if (top != null) {
                    this.tk = top.getKey();
                    this.tv = top.getValue();
                }

                if (log.isTraceEnabled()) {
                    log.trace("compressed document: {}", tv.get().length);
                }
            } else {
                source.next();
            }
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (EventSerializationUtil.isEventKey(range.getStartKey()) && range.getStartKey().getColumnQualifier().getLength() > 0) {
            this.tk = null;
            this.tv = null;
            this.buffer.clear();
            Key next = rebuildStartKey(range.getStartKey());
            range = new Range(next, true, range.getEndKey(), range.isEndKeyInclusive());
        }
        source.seek(range, columnFamilies, inclusive);
        next();
    }

    /**
     * When aggregating an event the seek range must be rebuilt to encompass the entire event
     *
     * @param start
     *            the start key
     * @return a key that maps to the start of an event
     */
    private Key rebuildStartKey(Key start) {
        String cf = start.getColumnFamily().toString();
        String root = TLD.getRootUid(cf);
        return new Key(start.getRow(), new Text(root));
    }

    @Override
    public Key getTopKey() {
        if (log.isTraceEnabled()) {
            log.trace("get tk: {}", tk);
        }
        return tk;
    }

    @Override
    public Value getTopValue() {
        if (log.isTraceEnabled()) {
            log.trace("get tv: {}", tv);
        }
        return tv;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        EventSerializationIterator copy = new EventSerializationIterator();
        copy.source = source.deepCopy(env);
        return copy;
    }

    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        options.put(VERSION_OPT, "an integer denoting the serialization version applied to documents");
        options.put(THRESHOLD_OPT, "an integer denoting the threshold in bytes after which a serialized event is compressed");
        options.put(ALGORITHM_OPT, "the algorithm used for compression (gzip, zstd), or 'raw' for no compression");
        return new IteratorOptions(this.getClass().getSimpleName(), "Iterator that serializes a document in the Value", options, null);
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {

        String versionOpt = options.get(VERSION_OPT);
        if (versionOpt == null) {
            throw new IllegalArgumentException("Missing option: " + VERSION_OPT);
        } else {
            try {
                int version = Integer.parseInt(versionOpt);
                serializationUtil.setSerializationVersion(version);
            } catch (NumberFormatException e) {
                log.error("Invalid version number {}", versionOpt);
                throw new IllegalArgumentException("Invalid version number: " + versionOpt, e);
            }
        }

        String thresholdOpt = options.get(THRESHOLD_OPT);
        if (thresholdOpt != null) {
            try {
                serializationUtil.setCompressionThreshold(Integer.parseInt(thresholdOpt));
            } catch (NumberFormatException e) {
                log.error("Invalid compression threshold {}", thresholdOpt);
                throw new IllegalArgumentException("Invalid compression threshold: " + thresholdOpt, e);
            }
        }

        String algorithmOpt = options.get(ALGORITHM_OPT);
        if (algorithmOpt != null) {
            serializationUtil.setCompressionAlgorithm(algorithmOpt);
        }

        serializationUtil.validateOptions();
        return true;
    }
}
