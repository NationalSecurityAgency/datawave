package datawave.query.iterators;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import datawave.query.Constants;

/**
 * Assumptions: columnQualifier always begins with date as yyyyMMdd
 *
 * Output if anything is found: Key will be rowId, columnFamily, lastSeenDate + MAX_UNICODE_STRING Value is of type {@link FirstAndLastSeenDate}
 */
public class FirstAndLastSeenIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
    protected static final Logger log = Logger.getLogger(FirstAndLastSeenIterator.class);
    public static final String END_OF_DATA_MARKER = Constants.MAX_UNICODE_STRING;
    public static final String SHOW_RESULT_PER_ROW_COL_FAM = "result.per.row.and.col.family.enabled";
    protected SortedKeyValueIterator<Key,Value> iterator;
    protected Map.Entry<Key,Value> result;
    private Key firstSeenKey = null;
    private Key lastSeenKey = null;
    private boolean shouldShowResultForEachRowColFam;
    private Key keyForFollowingRowColFam = null;

    public FirstAndLastSeenIterator(FirstAndLastSeenIterator iter, IteratorEnvironment env) {
        this.iterator = iter.iterator.deepCopy(env);
        this.result = iter.result;
    }

    public FirstAndLastSeenIterator() {}

    public static Range createRange(String rowId, String columnFamily, String startDate, String endDate) {
        return new Range(new Key(rowId, columnFamily, startDate), true, new Key(rowId, columnFamily, endDate + END_OF_DATA_MARKER), true);
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        String showResultPerRowColFam = options.get(SHOW_RESULT_PER_ROW_COL_FAM);
        this.shouldShowResultForEachRowColFam = (StringUtils.isEmpty(showResultPerRowColFam) ? false : Boolean.valueOf(showResultPerRowColFam));

        if (!validateOptions(options))
            throw new IOException("Iterator options are not correct");
        this.iterator = source;
    }

    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        return new IteratorOptions(getClass().getSimpleName(), "returns the first and last keys isAlreadyFinished for the specified value and date range",
                        options, null);
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        return true;
    }

    @Override
    public boolean hasTop() {
        return null != result;
    }

    @Override
    public Key getTopKey() {
        return result.getKey();
    }

    @Override
    public Value getTopValue() {
        return result.getValue();
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new FirstAndLastSeenIterator(this, env);
    }

    @Override
    public void next() throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("next called");
        }
        firstSeenKey = keyForFollowingRowColFam;
        keyForFollowingRowColFam = null;
        lastSeenKey = null;
        result = null;
        findTop();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("seek called: " + range);
        }
        this.iterator.seek(range, columnFamilies, inclusive);
        next();
    }

    protected void findTop() throws IOException {
        while (this.iterator.hasTop()) {
            Key currentKey = this.iterator.getTopKey();

            // if should return a result per row and this is a new row
            if (shouldShowResultForEachRowColFam && firstSeenKey != null && isNewRowColFam(currentKey)) {
                this.result = constructResult(firstSeenKey, lastSeenKey); // return the current result
                this.keyForFollowingRowColFam = currentKey;
                return;
            } else {
                updateFirstAndLastSeen(currentKey);
                this.iterator.next();
            }
        }
        result = constructResult(firstSeenKey, lastSeenKey);
    }

    private boolean isNewRowColFam(Key currentKey) {
        return 0 != currentKey.compareTo(this.firstSeenKey, PartialKey.ROW_COLFAM);
    }

    private void updateFirstAndLastSeen(Key currentKey) {
        if (null == firstSeenKey) {
            firstSeenKey = currentKey;
        }
        lastSeenKey = currentKey;
    }

    private AbstractMap.SimpleEntry constructResult(Key firstSeenKey, Key lastSeenKey) {
        if (null == firstSeenKey) {
            return null; // no data for range
        }
        String firstSeenDate = extractDateFromKey(firstSeenKey);
        String lastSeenDate = extractDateFromKey(lastSeenKey);
        Key resultKey = createResultKey(lastSeenKey, lastSeenDate);
        Value resultValue = constructValueForResult(firstSeenDate, lastSeenDate);
        return new AbstractMap.SimpleEntry(resultKey, resultValue);
    }

    String extractDateFromKey(Key lastKey) {
        return lastKey.getColumnQualifier().toString().substring(0, 8);
    }

    Key createResultKey(Key lastSeenKey, String lastSeenDate) {
        return new Key(constructRowIdForResult(lastSeenKey, lastSeenDate), constructColFamilyForResult(lastSeenKey, lastSeenDate),
                        constructColQualifierForResult(lastSeenKey, lastSeenDate));
    }

    Text constructColQualifierForResult(Key lastSeenKey, String lastDay) {
        return new Text(lastDay + END_OF_DATA_MARKER);
    }

    private Text constructColFamilyForResult(Key lastSeenKey, String lastSeenDate) {
        return lastSeenKey.getColumnFamily();
    }

    private Text constructRowIdForResult(Key lastSeenKey, String lastSeenDate) {
        return lastSeenKey.getRow();
    }

    private Value constructValueForResult(String firstSeenDate, String lastSeenDate) {
        return new Value(new FirstAndLastSeenDate(firstSeenDate, lastSeenDate).toString().getBytes());
    }
}
