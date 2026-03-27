package datawave.query.index.day;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import datawave.ingest.table.aggregator.BitSetCombiner;
import datawave.query.data.parsers.DayIndexKey;

/**
 * Iterator that scans the DayIndex table.
 * <p>
 * The executable fields and values are used to seek within a tablet to gather all relevant shard offset information. The client simply configures this iterator
 * and makes a single next call.
 * <p>
 * See {@link DayIndexKey} for details on the underlying key structure.
 * <p>
 * See {@link BitSetCombiner} for the table combiner.
 */
public class DayIndexEntryIterator implements SortedKeyValueIterator<Key,Value> {

    private static final Logger log = LoggerFactory.getLogger(DayIndexEntryIterator.class);

    public static final String VALUES_AND_FIELDS = "values.and.fields";

    private SortedKeyValueIterator<Key,Value> source;
    private Collection<ByteSequence> columnFamilies;
    private Range range;
    private Key tk;
    private Value tv;

    private TreeMultimap<String,String> valuesAndFields;

    private final DayIndexKey parser = new DayIndexKey();
    private final BitSetIndexEntrySerializer serDe = new BitSetIndexEntrySerializer();
    private final Map<String,BitSet> shards = new HashMap<>();

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;

        if (options.containsKey(VALUES_AND_FIELDS)) {
            valuesAndFields = mapFromString(options.get(VALUES_AND_FIELDS));
        } else {
            throw new IllegalArgumentException("Iterator was not passed required option: " + VALUES_AND_FIELDS);
        }

        // TODO -- add datatype filter
    }

    @Override
    public boolean hasTop() {
        return tk != null;
    }

    @Override
    public void next() throws IOException {
        tk = null;
        tv = null;
        shards.clear();

        while (source.hasTop()) {
            Key top = source.getTopKey();
            parser.parse(top);

            if (log.isDebugEnabled()) {
                log.debug("next: {}", top.toStringNoTime());
            }

            if (accepted()) {
                if (log.isDebugEnabled()) {
                    log.debug("key accepted {}", top.toStringNoTime());
                }
                // do the stuff
                String field = parser.getField();
                BitSet bits = BitSet.valueOf(source.getTopValue().get());
                shards.put(field + " == '" + parser.getValue() + "'", bits);
                source.next();
            } else {
                Range seekRange = getNextSeekRange();
                if (seekRange != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("key rejected, next seek range: {}", seekRange.getStartKey().toStringNoTime());
                    }
                    source.seek(seekRange, columnFamilies, true);
                } else {
                    log.debug("key rejected, no seek range available. call next");
                    source.next();
                }
            }
        }

        // build final entry
        if (!shards.isEmpty()) {
            String day = trimRowToDate(range.getStartKey());
            BitSetIndexEntry entry = new BitSetIndexEntry(day, shards);
            tk = new Key(day);
            tv = new Value(serDe.serialize(entry));
        }
    }

    private boolean accepted() {
        return valueAccepted() && fieldAccepted();
    }

    private boolean valueAccepted() {
        return valuesAndFields.containsKey(parser.getValue());
    }

    private boolean fieldAccepted() {
        return valuesAndFields.get(parser.getValue()).contains(parser.getField());
    }

    private Range getNextSeekRange() {

        if (!valueAccepted()) {
            return seekToNextValue();
        }

        if (!fieldAccepted()) {
            return seekToNextField();
        }

        return null;
    }

    private Range seekToNextField() {
        log.debug("seek to next field");
        String nextField = valuesAndFields.get(parser.getValue()).higher(parser.getField());

        if (nextField == null) {
            return seekToNextValue();
        }

        Text row = new Text(parser.getShard() + '\u0000' + parser.getValue());
        Key start = new Key(row, new Text(nextField));
        return new Range(start, true, range.getEndKey(), range.isEndKeyInclusive());
    }

    private Range seekToNextValue() {
        log.debug("seek to next value");

        // done with all fields for current value, seek to next value
        String nextValue = valuesAndFields.keySet().higher(parser.getValue());

        if (nextValue == null) {
            // we're done
            return null;
        }

        String firstField = valuesAndFields.get(nextValue).first();

        String nextRow = parser.getShard() + '\u0000' + nextValue;
        Text row = new Text(nextRow);
        Key start = new Key(row, new Text(firstField));
        return new Range(start, true, range.getEndKey(), range.isEndKeyInclusive());
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.range = range;
        this.columnFamilies = columnFamilies;

        // the incoming range is just the day. This iterator could handle narrowing the seek range to the precise value/field/datatype
        narrowSeekRange();

        source.seek(this.range, columnFamilies, inclusive);
        next();
    }

    /**
     * We should be handed a range where the row is just a date in 'yyyyMMdd' format
     */
    private void narrowSeekRange() {
        String data = trimRowToDate(range.getStartKey());

        String firstValue = valuesAndFields.keySet().first();
        Text row = new Text(data + '\u0000' + firstValue);
        Key start = new Key(row, new Text(valuesAndFields.get(firstValue).first()));

        Text endRow = new Text(data + '\u0000' + '\uffff');
        Key end = new Key(endRow);

        range = new Range(start, true, end, false);
    }

    private String trimRowToDate(Key key) {
        String date = key.getRow().toString();
        int index = date.indexOf('\u0000');
        if (index != -1) {
            date = date.substring(0, index); // trim to just the date
        }
        return date;
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
        DayIndexEntryIterator iterator = new DayIndexEntryIterator();
        iterator.source = source.deepCopy(env);
        return iterator;
    }

    public static TreeMultimap<String,String> mapFromString(String data) {
        TreeMultimap<String,String> multimap = TreeMultimap.create();
        for (String element : Splitter.on(';').split(data)) {
            int index = element.indexOf(':');
            String key = element.substring(0, index);
            String values = element.substring(index + 1);
            multimap.putAll(key, Splitter.on(',').split(values));
        }
        return multimap;
    }

    public static String mapToString(Multimap<String,String> multimap) {
        Preconditions.checkArgument(!multimap.isEmpty());
        StringBuilder sb = new StringBuilder();
        SortedSet<String> elements = new TreeSet<>();
        for (String key : new TreeSet<>(multimap.keySet())) {
            // key
            sb.append(key).append(':');
            // values
            sb.append(Joiner.on(',').join(new TreeSet<>(multimap.get(key))));
            elements.add(sb.toString());
            sb.setLength(0);
        }
        return Joiner.on(';').join(elements);
    }
}
