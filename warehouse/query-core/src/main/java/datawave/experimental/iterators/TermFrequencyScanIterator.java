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
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.TreeMultimap;

import datawave.query.data.parsers.KeyParser;
import datawave.query.data.parsers.TermFrequencyKey;
import datawave.webservice.common.logging.ThreadConfigurableLogger;

/**
 * An iterator that aggregates term frequency offsets for a given document
 * <p>
 * The initial seek range takes the form <code>row:tf:datatype\0uid</code>
 * <p>
 * The two modes of operation are filtering and seeking. Filter mode will scan the full TF but apply a field and value filter to all keys. Seek mode will seek
 * to the next possible hit on the first key that fails the field-value filter.
 */
public class TermFrequencyScanIterator implements SortedKeyValueIterator<Key,Value> {

    private static final Logger log = ThreadConfigurableLogger.getLogger(TermFrequencyScanIterator.class);

    public static final String FIELD_VALUES = "field.values";
    public static final String MODE = "mode"; // 'filter' or 'seek'

    private SortedKeyValueIterator<Key,Value> source;
    private IteratorEnvironment env;
    private Range range;
    private Collection<ByteSequence> columnFamilies;

    private TreeMultimap<String,String> fieldValues;
    private boolean seekModeEnabled;

    private TermFrequencyKey parser;

    private Key tk;
    private Value tv;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
        this.env = env;

        if (options.containsKey(FIELD_VALUES)) {
            this.fieldValues = deserializeFieldValues(options.get(FIELD_VALUES));
        }

        if (options.containsKey(MODE)) {
            String value = options.getOrDefault(MODE, "filter");
            seekModeEnabled = value.equals("seek");
        }

        this.parser = new TermFrequencyKey();
    }

    @Override
    public boolean hasTop() {
        return tk != null;
    }

    @Override
    public void next() throws IOException {
        tk = null;
        tv = null;
        while (tk == null && source.hasTop()) {
            tk = source.getTopKey();
            tv = source.getTopValue();
            parser.parse(tk);
            if (!accepted(parser)) {
                tk = null;
                tv = null;
                if (seekModeEnabled) {
                    Range nextRange = createNextRange(parser);
                    if (nextRange != null) {
                        source.seek(nextRange, columnFamilies, true);
                    }
                }
            }
            source.next();
        }
    }

    private boolean accepted(KeyParser parser) {
        return fieldValues.containsKey(parser.getField()) && fieldValues.get(parser.getField()).contains(parser.getValue());
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.range = range;
        this.columnFamilies = columnFamilies;
        this.source.seek(range, columnFamilies, inclusive);
        next();
    }

    private Range createNextRange(KeyParser parser) {
        return null;
    }

    private String getNextField(String field) {
        return null;
    }

    private String getNextValue(String value) {
        return null;
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
        log.warn("DEEP COPY");
        TermFrequencyScanIterator iter = new TermFrequencyScanIterator();
        iter.env = env;
        iter.fieldValues = this.fieldValues;
        return iter;
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
