package datawave.query.index.lookup;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import datawave.ingest.protobuf.Uid;
import datawave.query.tld.TLD;
import datawave.query.util.Tuple3;
import datawave.query.util.Tuples;

/**
 * Iterator that finds the first instance of a unique value that may exist in one of several fields
 */
public class FindFirstUidIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {

    private static final Logger log = LoggerFactory.getLogger(CreateUidsIterator.class);

    public static final String COLLAPSE_OPT = "collapse";
    public static final String IS_TLD_OPT = "is.tld";
    public static final String FIELDS_OPT = "fields";
    public static final String START_DATE = "start.date";
    public static final String END_DATE = "end.date";

    protected boolean collapseUids = false;
    protected boolean parseTldUids = false;

    protected SortedKeyValueIterator<Key,Value> src;
    protected Range range;
    protected Collection<ByteSequence> columnFamilies;

    protected Key tk;
    protected IndexInfo tv;

    private TreeSet<String> fields;
    private String startDate;
    private String endDate;

    private boolean foundFirst = false;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        src = source;

        if (options == null) {
            return;
        }

        final String collapseOpt = options.get(COLLAPSE_OPT);
        if (null != collapseOpt) {
            collapseUids = Boolean.parseBoolean(collapseOpt);
        }

        final String parseTldUidsOption = options.get(IS_TLD_OPT);
        if (null != parseTldUidsOption) {
            parseTldUids = Boolean.parseBoolean(parseTldUidsOption);
        }

        final String fieldsOpt = options.get(FIELDS_OPT);
        if (StringUtils.isNotBlank(fieldsOpt)) {
            fields = new TreeSet<>(Splitter.on(',').splitToList(fieldsOpt));
        } else {
            throw new IllegalArgumentException("FindFirstUidIterator must be given a set of fields");
        }

        final String startDateOpt = options.get(START_DATE);
        if (StringUtils.isNotBlank(startDateOpt)) {
            startDate = startDateOpt;
        } else {
            throw new IllegalArgumentException("FindFirstUidIterator must be given a start date");
        }

        final String endDateOpt = options.get(END_DATE);
        if (StringUtils.isNotBlank(endDateOpt)) {
            endDate = endDateOpt;
        } else {
            throw new IllegalArgumentException("FindFirstUidIterator must be given an end date");
        }
    }

    @Override
    public boolean hasTop() {
        return tk != null;
    }

    @Override
    public void next() throws IOException {
        tk = null;
        if (src.hasTop() && !foundFirst) {

            // while this iterator is designed for the use case of a very low cardinality value with just a few fields, it is possible to have a value that is
            // actually high cardinality across a large date range. The keys may fall outside the query's date range, so we need to check for this and advance
            // the source iterator to the next possible match.
            checkForMiss();

            if (!src.hasTop() || src.getTopKey() == null) {
                foundFirst = true; // mark that we tried and found nothing
                return;
            }

            Key reference = makeRootKey(src.getTopKey());
            SortedSet<String> uids = new TreeSet<>();
            long count = 0L;

            while (src.hasTop() && sameShard(reference, src.getTopKey())) {
                Key nextTop = src.getTopKey();
                Tuple3<Long,Boolean,List<String>> uidInfo = parseUids(nextTop, src.getTopValue());
                count += uidInfo.first();
                if (!collapseUids)
                    for (String uid : uidInfo.third()) {
                        if (log.isTraceEnabled())
                            log.trace("Adding uid {}", StringUtils.split(uid, '\u0000')[1]);
                        uids.add(uid);
                    }
                src.next();
            }

            if (collapseUids || uids.isEmpty()) {
                tv = new IndexInfo(count);
            } else if (parseTldUids) {
                // For each uid in the list of uids, parse out the tld portion from the whole uid.
                SortedSet<String> rootUids = uids.stream().map(TLD::parseRootPointerFromId).collect(Collectors.toCollection(TreeSet::new));
                tv = new IndexInfo(List.of(rootUids.first()));
            } else {
                tv = new IndexInfo(List.of(uids.first()));
            }

            tk = reference;
            foundFirst = true;
        }
    }

    /**
     * Check for a key miss. In the event of a miss, advance the source iterator to the next possible match.
     * <p>
     * The expectation is that fewer than ten keys exist for a given low cardinality value, so simply calling next on the source should be sufficient. However,
     * this iterator should be able to handle high-cardinality terms via a seek.
     */
    private void checkForMiss() throws IOException {

        if (fields == null || fields.isEmpty()) {
            throw new IllegalStateException("FindFirstUidIterator requires fields to operate");
        }

        String field;
        String date;

        int misses = 0;

        while (src.hasTop()) {
            Key key = src.getTopKey();

            // check field
            field = key.getColumnFamily().toString();
            int seekByNextThreshold = 10;
            if (!fields.contains(field)) {

                if (misses < seekByNextThreshold) {
                    log.trace("field miss: next");
                    misses++;
                    src.next();
                } else {
                    log.trace("field miss: seek");
                    String nextField = fields.higher(field);
                    if (nextField == null) {
                        log.trace("hit end of range");
                        break;
                    }
                    log.trace("seek to next field");
                    Range seekRange = getRangeForField(nextField);
                    src.seek(seekRange, columnFamilies, true);
                    misses = 0; // reset misses on a seek
                }
                continue;
            }

            // check date
            if (startDate != null && endDate != null) {
                date = key.getColumnQualifier().toString();
                date = date.substring(0, date.indexOf('_'));

                if (date.compareTo(startDate) < 0) {
                    if (misses < seekByNextThreshold) {
                        log.trace("date miss[before]: next");
                        misses++;
                        src.next();
                        continue;
                    }

                    log.trace("date miss[before]: seek to start date");
                    // the top key shard is before the start of the query date range, update the start date
                    Range seekRange = getRangeForField(field);
                    src.seek(seekRange, columnFamilies, true);
                    misses = 0; // reset misses on a seek
                    continue;
                }

                if (date.compareTo(endDate) > 0) {
                    if (misses < seekByNextThreshold) {
                        log.trace("date miss[after]: next");
                        misses++;
                        src.next();
                        continue;
                    }

                    log.trace("date miss[after]: seek to next field");
                    // the top key shard is after the end of the query date range, seek to next field
                    String nextField = fields.higher(field);
                    Range seekRange = getRangeForField(nextField);
                    src.seek(seekRange, columnFamilies, true);
                    misses = 0; // reset misses on a seek
                    continue;
                }

                // field matches and date is within start and stop bounds
                break;
            }
        }
    }

    /**
     * Generates a range given a field. Can be used to update the start date for a key by passing in the same field.
     *
     * @param field
     *            the field
     * @return a range
     */
    private Range getRangeForField(String field) {
        Key start = new Key(range.getStartKey().getRow(), new Text(field), new Text(startDate));
        return new Range(start, true, range.getEndKey(), range.isEndKeyInclusive());
    }

    /**
     * This iterator returns a top level key whose column qualifier includes only the shard. In that event, we must ensure we skip to the next sorted entry,
     * using skipkey
     */
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.range = range;
        this.columnFamilies = columnFamilies;

        if (!range.isStartKeyInclusive()) {
            this.range = skipKey(range);
        }

        src.seek(this.range, this.columnFamilies, inclusive);
        next();
    }

    /**
     * Method that ensures if we have to skip the current key, we do so with the contract provided by the create UID iterator.
     *
     * @param range
     *            the range
     * @return a range
     */
    protected Range skipKey(Range range) {
        Key startKey = range.getStartKey();
        Key newKey = new Key(startKey.getRow(), startKey.getColumnFamily(), new Text(startKey.getColumnQualifier() + "\u0000\uffff"));
        return new Range(newKey, true, range.getEndKey(), range.isEndKeyInclusive());
    }

    @Override
    public Key getTopKey() {
        return tk;
    }

    @Override
    public Value getTopValue() {
        return new Value(WritableUtils.toByteArray(this.getValue()));
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        FindFirstUidIterator itr = new FindFirstUidIterator();
        itr.src = src.deepCopy(env);
        itr.fields = this.fields;
        itr.startDate = this.startDate;
        itr.endDate = this.endDate;
        return itr;
    }

    public IndexInfo getValue() {
        return tv;
    }

    public static boolean sameShard(Key ref, Key test) {
        ByteSequence refCq = ref.getColumnQualifierData();
        ByteSequence testCq = test.getColumnQualifierData();
        if (testCq.length() < refCq.length()) {
            return false;
        }
        for (int i = 0; i < refCq.length(); ++i) {
            if (refCq.byteAt(i) != testCq.byteAt(i)) {
                return false;
            }
        }

        return testCq.byteAt(refCq.length()) == 0x00;
    }

    public static Tuple3<Long,Boolean,List<String>> parseUids(Key k, Value v) throws IOException {
        final String dataType = parseDataType(k);
        Uid.List docIds = Uid.List.parseFrom(v.get());
        final boolean ignore = docIds.getIGNORE();
        List<String> uids = ignore || docIds.getUIDList() == null ? Collections.emptyList()
                        : Lists.transform(docIds.getUIDList(), s -> dataType + "\u0000" + s.trim());
        return Tuples.tuple(docIds.getCOUNT(), ignore, uids);
    }

    public static String parseDataType(Key k) {
        ByteSequence colq = k.getColumnQualifierData();
        return new String(colq.subSequence(lastNull(colq) + 1, colq.length()).toArray());
    }

    public static int lastNull(ByteSequence bs) {
        int pos = bs.length();
        while (--pos > 0 && bs.byteAt(pos) != 0x00)
            ;
        return pos;
    }

    /**
     * When skipKey is false, we produce the key based upon k, removing any data type
     *
     * When skipKey is true, we will produce a key producing a skipkey from the root key. This will be helpful when we are being torn down.
     *
     * @param k
     *            a key
     * @return a key
     */
    public static Key makeRootKey(Key k) {
        ByteSequence cq = k.getColumnQualifierData();
        ByteSequence strippedCq = cq.subSequence(0, lastNull(cq));
        final ByteSequence row = k.getRowData(), cf = k.getColumnFamilyData(), cv = k.getColumnVisibilityData();
        return new Key(row.getBackingArray(), row.offset(), row.length(), cf.getBackingArray(), cf.offset(), cf.length(), strippedCq.getBackingArray(),
                        strippedCq.offset(), strippedCq.length(), cv.getBackingArray(), cv.offset(), cv.length(), k.getTimestamp());
    }

    /*
     * The following methods were implemented to allow this iterator to be used in the shell.
     */
    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptions("", "", Collections.emptyMap(), Collections.emptyList());
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        return true;
    }
}
