package datawave.core.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

import com.google.common.base.Splitter;

import datawave.query.Constants;
import datawave.query.data.parsers.ShardIndexKey;

/**
 * Attempts to expand an unfielded regex into discrete fields and values
 * <p>
 * Supports date and datatype filtering by default
 */
public class UnfieldedRegexExpansionIterator extends SeekingFilter implements OptionDescriber {

    private static final Logger log = LoggerFactory.getLogger(UnfieldedRegexExpansionIterator.class);

    public static final String START_DATE = "start.date";
    public static final String END_DATE = "end.date";
    public static final String DATATYPES = "dts";
    public static final String PATTERN = "pattern";
    public static final String REVERSE = "reverse";

    private String startDate;
    private String endDate;
    private Pattern pattern;
    private boolean reverse;
    private final StringBuilder sb = new StringBuilder();

    private Set<String> datatypes;

    private String previousMatch = null;

    private Text columnQualifierDate;
    private Text columnQualifierDateAndDatatype;

    // used to track the unique field value pairs
    private final Set<String> foundPairs = new HashSet<>();

    private final ShardIndexKey parser = new ShardIndexKey();

    enum HINT_TYPE {
        NONE, FIELD, DATE
    }

    private HINT_TYPE hint = HINT_TYPE.NONE;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {

        if (!validateOptions(options)) {
            throw new IllegalArgumentException("Iterator was not configured correctly");
        }

        super.init(source, options, env);

        if (options.containsKey(START_DATE)) {
            this.startDate = options.get(START_DATE);
            this.columnQualifierDate = new Text(startDate + "_0");
        } else {
            throw new IllegalArgumentException("Iterator requires START_DATE option");
        }

        if (options.containsKey(END_DATE)) {
            this.endDate = options.get(END_DATE) + Constants.MAX_UNICODE_STRING;
        } else {
            throw new IllegalArgumentException("Iterator requires END_DATE option");
        }

        if (options.containsKey(PATTERN)) {
            String option = options.get(PATTERN);
            this.pattern = Pattern.compile(option);
        } else {
            throw new IllegalArgumentException("Iterator requires PATTERN option");
        }

        if (options.containsKey(DATATYPES)) {
            String option = options.get(DATATYPES);
            this.datatypes = new HashSet<>(Splitter.on(',').splitToList(option));

            List<String> tmp = new ArrayList<>(datatypes);
            Collections.sort(tmp);
            this.columnQualifierDateAndDatatype = new Text(startDate + "_0\u0000" + tmp.get(0));
        }

        if (options.containsKey(REVERSE)) {
            this.reverse = Boolean.parseBoolean(options.get(REVERSE));
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (!range.isStartKeyInclusive()) {
            // need to make the start key inclusive because filters operate slightly differently
            Range seekRange = new Range(range.getStartKey(), true, range.getEndKey(), range.isEndKeyInclusive());
            super.seek(seekRange, columnFamilies, inclusive);
        } else {
            super.seek(range, columnFamilies, inclusive);
        }
    }

    @Override
    public FilterResult filter(Key k, Value v) {
        if (log.isDebugEnabled()) {
            log.debug("tk: {}", k.toStringNoTime());
        }

        // parse key and reset hint
        parser.parse(k);
        hint = HINT_TYPE.NONE;

        if (previousMatch == null || !previousMatch.equals(parser.getValue())) {
            Matcher matcher;
            if (reverse) {
                sb.setLength(0);
                sb.append(parser.getValue());
                sb.reverse();
                matcher = pattern.matcher(sb);
            } else {
                matcher = pattern.matcher(parser.getValue());
            }

            if (!matcher.matches()) {
                // advance to next row
                log.debug("pattern does not match, advance to next row");
                return new FilterResult(false, AdvanceResult.NEXT_ROW);
            }
            previousMatch = parser.getValue();
        }

        String candidate = parser.getValue() + parser.getField();
        boolean firstSeen = foundPairs.add(candidate);
        if (!firstSeen) {
            // advance to next field
            foundPairs.clear();
            log.debug("Found duplicate field, advance to next field");
            return new FilterResult(false, AdvanceResult.NEXT_CF);
        }

        String date = parser.getShard();
        if (date.compareTo(startDate) < 0) {
            // advance to start date
            log.debug("start date {} is before start date {}, advance to date {}", parser.getShard(), startDate, startDate);
            hint = HINT_TYPE.DATE;
            return new FilterResult(false, AdvanceResult.USE_HINT);
        } else if (date.compareTo(endDate) > 0) {
            // advance to next row
            log.debug("date {} sorts after end date {}, advance to next row", date, endDate);
            return new FilterResult(false, AdvanceResult.NEXT_ROW);
        }

        if (datatypes != null && !datatypes.contains(parser.getDatatype())) {
            log.debug("datatype {} does not match, advance to next key", parser.getDatatype());
            return new FilterResult(false, AdvanceResult.NEXT);
        }

        log.debug("key accepted, advancing to next column family");
        return new FilterResult(true, AdvanceResult.NEXT_CF);
    }

    @Override
    public Key getNextKeyHint(Key k, Value v) {
        switch (hint) {
            case FIELD:
                // advance to next field
                return k.followingKey(PartialKey.ROW_COLFAM);
            case DATE:
                if (columnQualifierDateAndDatatype != null) {
                    return new Key(k.getRow(), k.getColumnFamily(), columnQualifierDateAndDatatype);
                } else {
                    return new Key(k.getRow(), k.getColumnFamily(), columnQualifierDate);
                }
            case NONE:
            default:
                throw new IllegalStateException("Unhandled hint type: " + hint);
        }
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions options = new IteratorOptions(getClass().getSimpleName(), "Iterator that expands an unfielded regex into discrete fields and values",
                        null, null);
        options.addNamedOption(START_DATE, "The start date");
        options.addNamedOption(END_DATE, "The end date");
        options.addNamedOption(PATTERN, "The pattern");
        options.addNamedOption(DATATYPES, "(optional) A comma-delimited set of datatypes used to restrict the search space");
        options.addNamedOption(REVERSE, "(optional) true if this scan is for the shard reverse index");
        return options;
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        //  @formatter:off
        return options.containsKey(START_DATE) &&
                options.containsKey(END_DATE) &&
                options.containsKey(PATTERN);
        //  @formatter:on
    }
}
