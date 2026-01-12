package datawave.core.iterators.compress.event;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An iterator that sits above the {@link datawave.core.iterators.compress.event.EventSerializationIterator} in priority to provide an uncompressed view of the
 * world to the application layer.
 * <p>
 * Serialized events are deserialized via the {@link EventSerializationUtil}.
 */
public class EventDeserializationIterator implements SortedKeyValueIterator<Key,Value> {

    private static final Logger log = LoggerFactory.getLogger(EventDeserializationIterator.class);

    private SortedKeyValueIterator<Key,Value> source;

    private final EventSerializationUtil serializationUtil = new EventSerializationUtil();

    private Key tk;
    private Value tv;

    private final TreeSet<Pair<Key,Value>> buffer = new TreeSet<>();

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
                Iterator<Map.Entry<Key,Value>> iterator = serializationUtil.deserialize(source);
                while (iterator.hasNext()) {
                    Map.Entry<Key,Value> entry = iterator.next();
                    this.buffer.add(Pair.of(entry.getKey(), entry.getValue()));
                }

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

        if (log.isTraceEnabled()) {
            log.trace("seek: {}", range);
        }

        boolean isEventKey = EventSerializationUtil.isEventKey(range.getStartKey());
        if (isEventKey) {
            if (tk != null && tk.compareTo(range.getStartKey()) == 0) {
                // top key matches, nothing to do
                return;
            }

            // otherwise clear state and pass the seek down
            this.tk = null;
            this.tv = null;
            this.buffer.clear();
        }

        // the original seek range must be passed to the source iterator
        source.seek(range, columnFamilies, inclusive);
        next();

        // the seek range might land in the middle of an event.
        // the full event is returned by the underlying iterator, so iterate to the correct position.
        while (hasTop() && range.beforeStartKey(getTopKey())) {
            next();
        }
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
        return tv;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        EventDeserializationIterator copy = new EventDeserializationIterator();
        copy.source = source.deepCopy(env);
        return copy;
    }
}
