package datawave.core.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.SeekingFilter;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import datawave.query.Constants;
import datawave.query.data.parsers.ShardIndexKey;

/**
 * Attempts to expand an unfielded term into all possible fields
 * <p>
 * Performs date range filtering by default. Optionally applies datatype filtering.
 * <p>
 * Optionally restricts the search space to a set of fields
 */
public class FieldExpansionIterator extends SeekingFilter implements OptionDescriber {

    private static final Logger log = LoggerFactory.getLogger(FieldExpansionIterator.class);

    public static final String START_DATE = "start.date";
    public static final String END_DATE = "end.date";
    public static final String FIELDS = "fields";
    public static final String DATATYPES = "dts";

    // required
    private String startDate;
    private String endDate;

    private Set<String> fields;
    private TreeSet<String> datatypes;

    // track which fields this iterator has seen and returned. this collection is not persisted between teardown and rebuilds, so unique return values
    // are only guaranteed within a single non-interrupted scan session
    private final Set<String> found = new HashSet<>();

    private final ShardIndexKey parser = new ShardIndexKey();

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (!validateOptions(options)) {
            throw new IllegalStateException("FieldExpansionIterator not configured with correct options");
        }

        if (options.containsKey(FIELDS)) {
            fields = new HashSet<>(Splitter.on(',').splitToList(options.get(FIELDS)));
        }

        if (options.containsKey(DATATYPES)) {
            datatypes = new TreeSet<>(Splitter.on(',').splitToList(options.get(DATATYPES)));
        }

        startDate = options.get(START_DATE);
        endDate = options.get(END_DATE) + Constants.MAX_UNICODE_STRING;

        super.init(source, options, env);
    }

    @Override
    public FilterResult filter(Key k, Value v) {

        // keep it simple for now
        if (log.isTraceEnabled()) {
            log.trace("tk: {}", k.toStringNoTime());
        }

        parser.parse(k);

        // if field does not match, skip to next field
        if ((fields != null && !fields.contains(parser.getField())) || found.contains(parser.getField())) {
            log.trace("field not in set of expansion fields, or already seen this field. advancing to next field");
            return new FilterResult(false, AdvanceResult.NEXT_CF);
        }

        // ensure key falls within the date range
        String date = parser.getShard();
        if (date.compareTo(startDate) < 0) {
            // advance to start date
            log.trace("Key before start date: {} < {}", date, startDate);
            return new FilterResult(false, AdvanceResult.USE_HINT);
        }

        if (date.compareTo(endDate) > 0) {
            // advance to next field
            log.trace("Key after end date: {} > {}", date, endDate);
            return new FilterResult(false, AdvanceResult.NEXT_CF);
        }

        if (datatypes != null && !datatypes.contains(parser.getDatatype())) {

            String lower = datatypes.lower(parser.getDatatype());
            if (lower != null) {
                // advance to next field
                return new FilterResult(false, AdvanceResult.NEXT_CF);
            }

            String higher = datatypes.higher(parser.getDatatype());
            if (higher != null) {
                // current datatype sorts before next possible hit, advance via next
                return new FilterResult(false, AdvanceResult.NEXT);
            }
        }

        log.trace("key accepted, advancing to next CF");
        found.add(parser.getField());
        return new FilterResult(true, AdvanceResult.NEXT_CF);
    }

    /**
     * This method is only called when the top key's date range lies before the configured start date
     *
     * @param k
     *            a key
     * @param v
     *            a value
     * @return the start key for a seek range
     */
    @Override
    public Key getNextKeyHint(Key k, Value v) {
        String shard = startDate + "_0";

        Text cq;
        if (datatypes == null || datatypes.isEmpty()) {
            cq = new Text(shard);
        } else {
            cq = new Text(shard + '\u0000' + datatypes.first());
        }

        return new Key(k.getRow(), k.getColumnFamily(), cq, k.getTimestamp());
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions options = new IteratorOptions(getClass().getSimpleName(), "Iterator that expands unfielded terms using the global index", null, null);
        options.addNamedOption(START_DATE, "The start date");
        options.addNamedOption(END_DATE, "The end date");
        options.addNamedOption(FIELDS, "(optional) A comma-delimited set of fields that defines the search space");
        options.addNamedOption(DATATYPES, "(optional) A set of datatypes used to restrict the search space");
        return options;
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        return options.containsKey(START_DATE) && options.containsKey(END_DATE);
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (!range.isStartKeyInclusive()) {
            Preconditions.checkNotNull(range.getStartKey(), "FieldExpansionIterator expected a non-null start key");
            Preconditions.checkNotNull(range.getStartKey().getColumnFamily(), "FieldExpansionIterator expected a non-null column qualifier");
            // need to skip to next column family
            Key skip = range.getStartKey().followingKey(PartialKey.ROW_COLFAM);
            if (skip.compareTo(range.getEndKey()) > 0) {
                // handles the case where appending a null byte would cause the start key to be greater than the end key
                Range skipRange = new Range(range.getEndKey(), true, range.getEndKey(), range.isEndKeyInclusive());
                super.seek(skipRange, columnFamilies, inclusive);
            } else {
                Range skipRange = new Range(skip, true, range.getEndKey(), range.isEndKeyInclusive());
                super.seek(skipRange, columnFamilies, inclusive);
            }
        } else {
            super.seek(range, columnFamilies, inclusive);
        }
    }
}
