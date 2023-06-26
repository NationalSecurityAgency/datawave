package datawave.core.iterators;

import java.io.IOException;
import java.util.Collection;
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

import datawave.core.iterators.filter.GlobalIndexTermMatchingFilter;

public class GlobalIndexTermMatchingIterator extends GlobalIndexTermMatchingFilter implements SortedKeyValueIterator<Key,Value>, OptionDescriber {

    public static final String UNIQUE_TERMS_IN_FIELD = "term.unique";
    private static final Logger log = Logger.getLogger(GlobalIndexTermMatchingIterator.class);

    private SortedKeyValueIterator<Key,Value> source;

    private boolean foundMatch = false;

    private Range scanRange;
    private Collection<ByteSequence> scanCFs;
    private boolean scanInclusive;

    protected boolean uniqueTermsOnly = false;

    public GlobalIndexTermMatchingIterator() throws IOException {}

    public GlobalIndexTermMatchingIterator deepCopy(IteratorEnvironment env) {
        return new GlobalIndexTermMatchingIterator(this, env);
    }

    private GlobalIndexTermMatchingIterator(GlobalIndexTermMatchingIterator other, IteratorEnvironment env) {
        setSource(other.getSource().deepCopy(env));
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        if (!validateOptions(options))
            throw new IOException("Iterator options are not correct");
        setSource(source);
    }

    @Override
    public Key getTopKey() {
        Key key = null;
        if (foundMatch) {
            key = getSource().getTopKey();
        }
        return key;
    }

    @Override
    public Value getTopValue() {
        if (foundMatch) {
            return getSource().getTopValue();
        } else {
            return null;
        }
    }

    @Override
    public boolean hasTop() {
        return foundMatch && getSource().hasTop();
    }

    @Override
    public void next() throws IOException {
        if (foundMatch && uniqueTermsOnly) {
            advance(getSource().getTopKey());
        } else
            getSource().next();
        findTopEntry();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.scanRange = range;
        this.scanCFs = columnFamilies;
        this.scanInclusive = inclusive;
        getSource().seek(range, columnFamilies, inclusive);

        // if we have been reseeked after being torn down, and we are only returning unique terms, then advance to the next unique row/cf
        if (!range.isStartKeyInclusive() && uniqueTermsOnly && getSource().hasTop()) {
            Key start = range.getStartKey();
            // verify this was actually from being torn down in that this key looks like a global index term
            if (start.getColumnFamily().getLength() > 0 && start.getColumnQualifier().getLength() > 0
                            && start.getColumnQualifier().toString().indexOf('\0') > 0) {
                advance(start);
            }
        }

        findTopEntry();
    }

    private void findTopEntry() throws IOException {
        foundMatch = false;
        if (log.isTraceEnabled())
            log.trace("has top ? " + getSource().hasTop());
        while (!foundMatch && getSource().hasTop()) {
            Key top = getSource().getTopKey();
            if (log.isTraceEnabled())
                log.trace("top key is " + top);
            if (accept(top, getSource().getTopValue())) {
                foundMatch = true;
            } else {
                getSource().next();
            }
        }
    }

    /**
     * Advances to the next top key
     *
     * @param top
     *            current key that we see
     * @throws IOException
     *             for issues with read/write
     */
    protected void advance(final Key top) throws IOException {
        /*
         * nexts a few times for giving up and seek'ing for the following row, column family
         */
        Key endKey = scanRange.getEndKey();
        Key next = top.followingKey(PartialKey.ROW_COLFAM);
        // we've surpassed the end range
        if (null != endKey && next.compareTo(endKey) > 0) {
            next = scanRange.getEndKey();
            if (log.isTraceEnabled())
                log.trace("new next is " + next + " top key is " + top);
        } else {
            if (log.isTraceEnabled())
                log.trace("advance to " + next + " top key is " + top);
        }
        if (getSource().hasTop() && getSource().getTopKey().compareTo(next) < 0) {
            if (log.isTraceEnabled())
                log.trace("seeking to " + next);
            getSource().seek(new Range(next, true, scanRange.getEndKey(), scanRange.isEndKeyInclusive()), scanCFs, scanInclusive);
        } else {
            if (log.isTraceEnabled())
                log.trace("not seeking to " + next);
        }
    }

    protected void setSource(SortedKeyValueIterator<Key,Value> source) {
        this.source = source;
    }

    protected SortedKeyValueIterator<Key,Value> getSource() {
        return source;
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = super.describeOptions();
        io.addNamedOption(LITERAL + "i", "A literal value to match");
        io.addNamedOption(PATTERN + "i", "A regex value to match");
        io.addNamedOption(REVERSE_INDEX, "Boolean denoting whether we are matching against a reverse index");
        io.addNamedOption(UNIQUE_TERMS_IN_FIELD, "Advances the term when one is found, ignoring the fact that the term may exist on multiple shards");
        io.setDescription("GlobalIndexTermMatchingIterator uses a set of literals and regexs to match global index keys");
        return io;
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        boolean valid = super.validateOptions(options);

        if (options.containsKey(UNIQUE_TERMS_IN_FIELD)) {
            /*
             * note that boolean will ONLY return true if the value in the map is 'true' or 'TRUE'. we don't need the above conditional, but we are being
             * defensive.
             */
            uniqueTermsOnly = new Boolean(options.get(UNIQUE_TERMS_IN_FIELD));

        }
        return valid;
    }

}
