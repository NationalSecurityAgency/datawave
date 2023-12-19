package datawave.experimental.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.TreeMultimap;

import datawave.query.data.parsers.FieldIndexKey;
import datawave.query.data.parsers.KeyParser;
import datawave.webservice.common.logging.ThreadConfigurableLogger;

/**
 * Given a sorted set of fields and values, perform a rolling scan through a field index
 */
public class FieldIndexScanIterator implements SortedKeyValueIterator<Key,Value> {

    private static final Logger log = ThreadConfigurableLogger.getLogger(FieldIndexScanIterator.class);

    public static final String FIELD_VALUES = "field.values";

    private SortedKeyValueIterator<Key,Value> source;
    private IteratorEnvironment env;

    private Key tk;
    private final Value tv = new Value();

    private final FieldIndexKey parser = new FieldIndexKey();
    private TreeMultimap<String,String> fieldValues;
    private Range seekRange;
    private Collection<ByteSequence> columnFamilies;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
        this.env = env;

        if (options.containsKey(FIELD_VALUES)) {
            this.fieldValues = deserializeFieldValues(options.get(FIELD_VALUES));
        }
    }

    @Override
    public boolean hasTop() {
        return tk != null;
    }

    @Override
    public void next() throws IOException {
        tk = null;
        while (tk == null && source.hasTop()) {
            tk = source.getTopKey();
            parser.parse(tk);
            if (accepted(parser)) {
                // log.info(parser.getField() + " " + parser.getValue() + " " + parser.getUid());
                source.next();
            } else {
                tk = null;
                Range nextRange = createNextRange(parser);
                if (nextRange != null) {
                    source.seek(nextRange, columnFamilies, true);
                }
            }

        }
    }

    private boolean accepted(KeyParser parser) {
        return fieldValues.containsKey(parser.getField()) && fieldValues.get(parser.getField()).contains(parser.getValue());
    }

    /**
     * Update the seek range's start key or return null if no such start key exists
     *
     * @param parser
     *            a key parser
     * @return a range for the next hit, or null if no such hit exists
     */
    private Range createNextRange(KeyParser parser) {

        if (fieldValues.containsKey(parser.getField())) {
            // potentially building a seek range to the next value for a field
            String nextValue = fieldValues.get(parser.getField()).higher(parser.getValue());
            if (nextValue != null) {
                // log.info("seeking to next value for field " + parser.getField());
                Key start = new Key(seekRange.getStartKey().getRow(), new Text("fi\0" + parser.getField()), new Text(nextValue + '\u0000'));
                return new Range(start, false, seekRange.getEndKey(), seekRange.isEndKeyInclusive());
            }
            // if the value is null then skip to the next field
        }

        String nextField = fieldValues.keySet().higher(parser.getField());
        if (nextField != null) {
            // log.info("seeking to next field " + nextField);
            String nextValue = fieldValues.get(nextField).first();
            Key start = new Key(seekRange.getStartKey().getRow(), new Text("fi\0" + nextField), new Text(nextValue + '\u0000'));
            return new Range(start, true, seekRange.getEndKey(), seekRange.isEndKeyInclusive());
        }

        // somehow a key was found that lies outside the initial seek range
        log.error("found a key beyond the initial range");
        return null;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.seekRange = computeRange(range);
        this.columnFamilies = columnFamilies;
        this.source.seek(seekRange, columnFamilies, inclusive);
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
        FieldIndexScanIterator iter = new FieldIndexScanIterator();
        iter.env = env;
        return iter;
    }

    public Range computeRange(Range range) {
        String firstField = fieldValues.keySet().first();
        String firstValue = fieldValues.get(firstField).first();
        String lastField = fieldValues.keySet().last();
        String lastValue = fieldValues.get(lastField).last();
        String row = range.getStartKey().getRow().toString();
        Key start = new Key(row, "fi\0" + firstField, firstValue + '\0');
        Key end = new Key(row, "fi\0" + lastField, lastValue + '\1');
        return new Range(start, false, end, false);
    }

    public static String serializeFieldValue(TreeMultimap<String,String> fieldValues) {
        SortedSet<String> parts = new TreeSet<>();
        for (String field : fieldValues.keySet()) {
            parts.add(field + ':' + Joiner.on(',').join(fieldValues.get(field)));
        }
        return Joiner.on(';').join(parts);
    }

    public static TreeMultimap<String,String> deserializeFieldValues(String serialized) {

        if (StringUtils.isBlank(serialized)) {
            return TreeMultimap.create();
        }

        TreeMultimap<String,String> fieldValues = TreeMultimap.create();
        for (String part : Splitter.on(';').split(serialized)) {
            int index = part.indexOf(':');
            String field = part.substring(0, index);
            fieldValues.putAll(field, Splitter.on(',').split(part.substring(index + 1)));
        }
        return fieldValues;
    }
}
