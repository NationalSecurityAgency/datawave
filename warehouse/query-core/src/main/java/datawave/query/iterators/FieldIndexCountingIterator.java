package datawave.query.iterators;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.math.LongRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import datawave.iterators.IteratorSettingHelper;
import datawave.marking.MarkingFunctions;
import datawave.query.Constants;
import datawave.util.CompositeTimestamp;
import datawave.util.TextUtil;

/**
 *
 * An iterator for the Datawave shard table, it searches FieldIndex keys and returns Event keys (its topKey must be an Event key).
 *
 * FieldIndex keys: fi\0{fieldName}:{fieldValue}\0datatype\0uid
 *
 * Return key: Row - ShardId Fam - FieldName Qual - FieldValue - Datatype - count
 *
 */
public class FieldIndexCountingIterator extends WrappingIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {

    protected static final Logger log = Logger.getLogger(FieldIndexCountingIterator.class);
    // Config constants
    public static final String START_TIME = "FieldIndexCountingIterator.START_TIME";
    public static final String STOP_TIME = "FieldIndexCountingIterator.STOP_TIME";
    public static final String START_TIME_INCL = "FieldIndexCountingIterator.startInclusive";
    public static final String STOP_TIME_INCL = "FieldIndexCountingIterator.endInclusive";
    public static final String FIELD_NAMES = "FieldIndexCountingIterator.FIELD_NAMES";
    public static final String FIELD_VALUES = "FieldIndexCountingIterator.FIELD_VALUES";
    public static final String DATA_TYPES = "FieldIndexCountingIterator.DATA_TYPES";
    public static final String UNIQ_BY_DATA_TYPE = "FieldIndexCountingIterator.UNIQ_BY_DATA_TYPE";
    public static final String SEP = ",";

    // Timestamp variables
    private final SimpleDateFormat dateParser = initDateParser();
    private long stamp_start;
    private long stamp_end;
    private LongRange stampRange = null;
    // Wrapping iterator only accesses its private source in setSource and getSource
    // Since this class overrides these methods, it's safest to keep the source declaration here
    protected SortedKeyValueIterator<Key,Value> source;
    private boolean uniqByDataTypeOption = false; // returning counts per data type or not
    private TreeSet<String> fieldNameFilter;
    private TreeSet<String> fieldValueFilter; // Let us do specific field values.
    private TreeSet<String> dataTypeFilter; // hold ordered set of data types to include

    protected Key topKey = null;
    protected Value topValue = null;
    protected Range parentRange;
    protected TreeSet<ByteSequence> seekColumnFamilies; // This iterator can only have a columnFamilies of the form fi\x00FIELDNAME

    private long count = 0L;
    private long maxTimeStamp = 0L;
    private Text currentRow = null;
    private Text currentFieldName = null;
    private String currentFieldValue = null;
    private String currentDataType = null;
    private StringBuilder dataTypeStringBuilder = new StringBuilder();
    private StringBuilder fieldValueStringBuilder = new StringBuilder();

    public static final String ONE_BYTE_STRING = "\u0001";
    public static final String DATE_FORMAT_STRING = "yyyyMMddHHmmss";
    public static final Text fi_PREFIX_TEXT = new Text("fi\u0000");

    private Set<Text> visibilitySet = new HashSet<>();

    // -------------------------------------------------------------------------
    // ------------- Constructors
    public FieldIndexCountingIterator() {
        visibilitySet = new HashSet<>();
    }

    public FieldIndexCountingIterator(FieldIndexCountingIterator other, IteratorEnvironment env) {
        this();
        this.source = other.getSource().deepCopy(env);
        this.seekColumnFamilies = new TreeSet<>(other.seekColumnFamilies);
        this.parentRange = new Range(other.parentRange);

        this.stamp_start = other.stamp_start;
        this.stamp_end = other.stamp_end;
        this.stampRange = new LongRange(stamp_start, stamp_end);

        this.uniqByDataTypeOption = other.uniqByDataTypeOption;
        if (null != other.fieldNameFilter && !other.fieldNameFilter.isEmpty()) {
            this.fieldNameFilter = new TreeSet<>(other.fieldNameFilter);
        }
        if (null != other.fieldValueFilter && !other.fieldValueFilter.isEmpty()) {
            this.fieldValueFilter = new TreeSet<>(other.fieldValueFilter);
        }
        if (null != other.dataTypeFilter && !other.dataTypeFilter.isEmpty()) {
            this.dataTypeFilter = new TreeSet<>(other.dataTypeFilter);
        }

        if (log.isTraceEnabled()) {
            for (ByteSequence bs : this.seekColumnFamilies) {
                log.trace("seekColumnFamilies for this iterator: " + (new String(bs.toArray())).replaceAll(Constants.NULL_BYTE_STRING, "%00"));
            }
        }
    }

    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        options.put(START_TIME, "The GMT start time for the scan using the format " + FieldIndexCountingIterator.DATE_FORMAT_STRING);
        options.put(STOP_TIME, "The GMT stop time for the scan using the format " + FieldIndexCountingIterator.DATE_FORMAT_STRING);
        options.put(START_TIME_INCL, "Boolean value denoting whether the start time is inclusive");
        options.put(STOP_TIME_INCL, "Boolean value denoting whether the stop time is inclusive");
        options.put(FIELD_NAMES, "The (optional) field names to count separated by \"" + SEP + "\"");
        options.put(FIELD_VALUES, "The (optional) field values to count separated by \"" + SEP + "\"");
        options.put(DATA_TYPES, "The (optional) data types to filter by");

        return new IteratorOptions(getClass().getSimpleName(), "An iterator used to count items in the field index", options, null);
    }

    /**
     * Get an IteratorSetting given a hadoop configuration
     *
     * @param conf
     *            Configuration object containing the options values for this iterator.
     * @return the iterator setting
     */
    public static IteratorSetting getIteratorSetting(Configuration conf) {
        Map<String,String> summaryIteratorSettings = new HashMap<>();
        putIfNotNull(summaryIteratorSettings, FieldIndexCountingIterator.START_TIME, conf.get(FieldIndexCountingIterator.START_TIME, null));
        putIfNotNull(summaryIteratorSettings, FieldIndexCountingIterator.STOP_TIME, conf.get(FieldIndexCountingIterator.STOP_TIME, null));
        putIfNotNull(summaryIteratorSettings, FieldIndexCountingIterator.START_TIME_INCL, conf.get(FieldIndexCountingIterator.START_TIME_INCL, null));
        putIfNotNull(summaryIteratorSettings, FieldIndexCountingIterator.STOP_TIME_INCL, conf.get(FieldIndexCountingIterator.STOP_TIME_INCL, null));
        putIfNotNull(summaryIteratorSettings, FieldIndexCountingIterator.FIELD_NAMES, conf.get(FieldIndexCountingIterator.FIELD_NAMES, null));
        putIfNotNull(summaryIteratorSettings, FieldIndexCountingIterator.FIELD_VALUES, conf.get(FieldIndexCountingIterator.FIELD_VALUES, null));
        putIfNotNull(summaryIteratorSettings, FieldIndexCountingIterator.DATA_TYPES, conf.get(FieldIndexCountingIterator.DATA_TYPES, null));
        putIfNotNull(summaryIteratorSettings, FieldIndexCountingIterator.UNIQ_BY_DATA_TYPE, conf.get(FieldIndexCountingIterator.UNIQ_BY_DATA_TYPE, null));
        return new IteratorSetting(IteratorSettingHelper.BASE_ITERATOR_PRIORITY + 40, "FieldIndexCountingIterator", FieldIndexCountingIterator.class.getName(),
                        summaryIteratorSettings);
    }

    private static void putIfNotNull(Map<String,String> map, String key, String value) {
        if (key != null && value != null) {
            map.put(key, value);
        }
    }

    // -------------------------------------------------------------------------
    // ------------- Overrides
    @Override
    public void init(SortedKeyValueIterator<Key,Value> src, Map<String,String> options, IteratorEnvironment env) throws IOException {
        if (!this.validateOptions(options)) {
            throw new IllegalArgumentException("options not set properly");
        }

        this.source = src;
        this.seekColumnFamilies = new TreeSet<>();
        if (this.fieldNameFilter != null && !this.fieldNameFilter.isEmpty()) {
            for (String fn : this.fieldNameFilter) {
                this.seekColumnFamilies.add(new ArrayByteSequence(Constants.FIELD_INDEX_PREFIX + fn));
            }
        }
        if (log.isTraceEnabled()) {
            for (ByteSequence bs : this.seekColumnFamilies) {
                log.trace("seekColumnFamilies for this iterator: " + (new String(bs.toArray())).replaceAll(Constants.NULL_BYTE_STRING, "%00"));
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("timestamp range: " + this.stampRange);
        }
    }

    @Override
    protected void setSource(SortedKeyValueIterator<Key,Value> src) {
        this.source = src;
    }

    @Override
    protected SortedKeyValueIterator<Key,Value> getSource() {
        return source;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new FieldIndexCountingIterator(this, env);
    }

    @Override
    public Key getTopKey() {
        return topKey;
    }

    @Override
    public Value getTopValue() {
        return topValue;
    }

    @Override
    public boolean hasTop() {
        return (topKey != null);
    }

    @Override
    public void next() throws IOException {
        if (!source.hasTop()) {
            this.topKey = null;
            return;
        }
        findTop();
    }

    @Override
    public void seek(Range r, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.parentRange = new Range(r);

        this.topKey = null;
        this.topValue = null;

        if (log.isTraceEnabled()) {
            log.trace("begin seek, range: " + parentRange);
            for (ByteSequence bs : this.seekColumnFamilies) {
                log.trace("seekColumnFamilies for this iterator: " + (new String(bs.toArray())).replaceAll(Constants.NULL_BYTE_STRING, "%00"));
            }
        }

        Key pStartKey = parentRange.getStartKey();

        // If we have a null start key or empty starting row, lets seek our underlying source so we can get the first row
        // Once we have the row, we can build our first fi\x00FIELDNAME properly
        // The goal is to build the proper starting key which we will update the parentRange with at the end of this logic.
        // This handles the infinite start key / empty range case.
        if (null == pStartKey || null == pStartKey.getRow() || pStartKey.getRow().toString().trim().length() <= 0) {

            source.seek(parentRange, this.seekColumnFamilies, !seekColumnFamilies.isEmpty());

            if (!source.hasTop()) { // early out
                return;
            }

            Text cFam = new Text(Constants.FIELD_INDEX_PREFIX);
            if (null != this.fieldNameFilter && !this.fieldNameFilter.isEmpty()) {
                TextUtil.textAppendNoNull(cFam, this.fieldNameFilter.first());
            }

            Text cQual = new Text();
            if (null != this.fieldValueFilter && !this.fieldValueFilter.isEmpty()) {
                TextUtil.textAppendNoNull(cQual, this.fieldValueFilter.first());
                if (null != this.dataTypeFilter && !this.dataTypeFilter.isEmpty()) {
                    TextUtil.textAppend(cQual, this.dataTypeFilter.first());
                }
            }
            pStartKey = new Key(source.getTopKey().getRow(), cFam, cQual);

            if (log.isTraceEnabled()) {
                log.trace("pStartKey was null or row was empty, parentRange: " + parentRange + "  source.getTopKey(): " + source.getTopKey());
            }

        } else { // We have a valid start key and row

            // Check if we are recovering from IterationInterruptedException (verify that we have the right key parts
            // and that the start key is NOT inclusive).
            if (null != pStartKey.getColumnFamily() && !pStartKey.getColumnFamily().toString().trim().isEmpty() && null != pStartKey.getColumnQualifier()
                            && !pStartKey.getColumnQualifier().toString().trim().isEmpty() && !parentRange.isStartKeyInclusive()) {

                // Iteration interrupted case, need to seek to the end of this FN:FV range. IterationInterruptedException
                // should always seek with the previously returned top key but with the inclusivity bit set to false.
                // i.e. Key-> Row:000 CFAM:fi\x00COLOR CQ:red, inclusive:False
                // we want to seek to the end of 'red' to the next unknown value, so CQ: red\u0001 should get us there.
                pStartKey = new Key(pStartKey.getRow(), pStartKey.getColumnFamily(), new Text(pStartKey.getColumnQualifier() + ONE_BYTE_STRING));
            }

            // for anything else we'll seek the underlying source and let findTop sort it out.
        }

        // Verify pStartKey is still in the range, if not we're done
        if (!parentRange.contains(pStartKey)) {
            return;
        }

        // Update the parent range and seek the underlying source
        // NOTE: In the case of non-inclusive start key, we've modified it accordingly so inclusivity should now be true.
        parentRange = new Range(pStartKey, true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
        source.seek(parentRange, this.seekColumnFamilies, !this.seekColumnFamilies.isEmpty());

        // advance to the field index if needed
        if (null == fieldNameFilter || fieldNameFilter.isEmpty()) {
            advanceToFieldIndex();
        }

        // if the start key of our bounding range > parentKey.endKey we can stop
        if (!source.hasTop() || !parentRange.contains(source.getTopKey())) {
            if (log.isTraceEnabled()) {
                log.trace("startKey is outside parentRange, done.");
            }
            return;
        }

        // now get the top key
        findTop();

        if (log.isTraceEnabled()) {
            log.trace("seek, topKey : " + ((null == topKey) ? "null" : topKey));
        }
    }

    @Override
    public String toString() {
        return "FieldIndexCountingIterator{" + "stamp_start=" + stamp_start + ", stamp_end=" + stamp_end + ", uniqByDataTypeOption=" + uniqByDataTypeOption
                        + ", currentRow=" + currentRow + ", currentFieldValue=" + currentFieldValue + ", currentDataType=" + currentDataType + '}';
    }

    // -------------------------------------------------------------------------
    // ------------- other stuff

    /**
     * Given a Key to consume, update any necessary counters etc.
     *
     * @param key
     *            The field index key to consume, should already satisfy all matching conditions.
     */
    private void consume(Key key) {

        if (log.isTraceEnabled()) {
            log.trace("consume, key: " + key);
        }

        if (!this.visibilitySet.contains(key.getColumnVisibility())) {
            this.visibilitySet.add(key.getColumnVisibility());
        }

        // update current count
        this.count += 1;

        // set most recent timestamp
        this.maxTimeStamp = (this.maxTimeStamp > CompositeTimestamp.getEventDate(key.getTimestamp())) ? maxTimeStamp
                        : CompositeTimestamp.getEventDate(key.getTimestamp());
    }

    private boolean acceptTimestamp(Key k) {
        return this.stampRange.containsLong(CompositeTimestamp.getEventDate(k.getTimestamp()));
    }

    private boolean isFieldIndexKey(Key key) {
        Text cf = key.getColumnFamily();
        return (cf.getLength() >= 3 && cf.charAt(0) == 'f' && cf.charAt(1) == 'i' && cf.charAt(2) == '\0');
    }

    /**
     * NOTE: This method is most likely never invoked if 1. At least one FieldName has been configured on the iterator (populates seekColumnFamilies) 2. The
     * underlying iterator is honoring the seek column families
     *
     * If the key's CQ is behind the fi\x00, then we seek to the first fi\x00FN in our list. If the key's CQ is after the fi\x00, then we seek to the first
     * fi\x00FN in the next row!
     *
     * This needs to also handle the condition where there are no FI keys in a given row, in that case it should continue moving rows until it there is no top
     * key, it's out of the range, or it finally finds one.
     *
     * @return True if and only if we have a top key that is a field index key in the parent range.
     * @throws IOException
     *             for issues with read/write
     */
    protected boolean advanceToFieldIndex() throws IOException {
        log.trace("advanceToFieldIndex");

        Key key = source.hasTop() ? source.getTopKey() : null; // Grab the source top key and update the key as we go
        while (null != key && !isFieldIndexKey(key) && parentRange.contains(key)) {

            log.trace("advanceToFieldIndex.key: " + key);

            // We know this is not a field index key, it is either before or after the fi\x00FN block
            if (key.compareColumnFamily(fi_PREFIX_TEXT) > 0) { // after fi\x00 segment, we need to move to then next row
                if (!advanceToNextRow()) { // this moves the source & updates parent range
                    // source doesn't have top or it's outside the parent range, we can return early
                    return false;
                }
            }

            // Seek to the first FieldName in our list
            key = new Key(source.getTopKey().getRow(),
                            new Text(Constants.FIELD_INDEX_PREFIX + ((null != fieldNameFilter && !fieldNameFilter.isEmpty()) ? fieldNameFilter.first() : "")));
            if (parentRange.contains(key)) {
                parentRange = new Range(key, true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
                source.seek(parentRange, seekColumnFamilies, !seekColumnFamilies.isEmpty());
                key = source.getTopKey();
            } else {
                // move the source to an empty state
                Range range = new Range(parentRange.getEndKey(), true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
                source.seek(range, seekColumnFamilies, !seekColumnFamilies.isEmpty());
            }
        }

        return (null != key) && parentRange.contains(key) && isFieldIndexKey(key);
    }

    /**
     * Build a key for the following row and attempt to seek the underlaying source iterator if the following row is in the parent range.
     *
     * @return true if and only if the source has top AND it is in the parent range.
     * @throws IOException
     *             for issues with read/write
     */
    protected boolean advanceToNextRow() throws IOException {
        log.trace("advanceToNextRow");
        if (!source.hasTop()) {
            return false;
        }

        // Build a key that is the following row
        Key followingRowKey = new Key(source.getTopKey().followingKey(PartialKey.ROW));

        if (log.isTraceEnabled()) {
            log.trace("advanceToNextRow source.topKey: " + source.getTopKey());
            log.trace("advanceToNextRow followingRowKey: " + followingRowKey);
        }

        // If that following row is in the parent range, update the range and seek underlying source iterator
        if (parentRange.contains(followingRowKey)) {
            parentRange = new Range(followingRowKey, true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
            source.seek(parentRange, seekColumnFamilies, !seekColumnFamilies.isEmpty());
        } else {
            // move the source to an empty state
            Range range = new Range(parentRange.getEndKey(), true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
            source.seek(range, seekColumnFamilies, !seekColumnFamilies.isEmpty());
        }

        // if the start key of our bounding range > parentKey.endKey we can stop
        if (log.isTraceEnabled()) {
            if (source.hasTop()) {
                log.trace("advanceToNextRow source.topKey: " + source.getTopKey() + " :: in parent range? " + parentRange.contains(source.getTopKey()));
            }
        }

        return source.hasTop() && parentRange.contains(source.getTopKey());
    }

    private void advanceToNextDataType(String fieldVal, String dType) throws IOException {
        // seek the source iterator to the next datatype on the list
        dataTypeStringBuilder.delete(0, dataTypeStringBuilder.length());
        Key sKey = this.source.getTopKey();

        // Given a datatype, get the next, highest one on the set.
        dType = this.dataTypeFilter.higher(dType);
        if (log.isTraceEnabled()) {
            log.trace("\t\tnext data type: " + dType);
            log.trace("\t\tcurrentFieldValue: " + this.currentFieldValue);
        }

        dataTypeStringBuilder.append(fieldVal);
        if (null == dType) {
            // no more valid datatypes in this particular field value, skip to next
            dataTypeStringBuilder.append(ONE_BYTE_STRING);
        } else {
            dataTypeStringBuilder.append(Constants.NULL_BYTE_STRING).append(dType);
        }

        sKey = new Key(sKey.getRow(), sKey.getColumnFamily(), new Text(dataTypeStringBuilder.toString()));
        if (log.isTraceEnabled()) {
            log.trace("\t\tadvanceToNextDataType, startKey: " + sKey);
        }

        if (parentRange.contains(sKey)) {
            parentRange = new Range(sKey, true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
            source.seek(parentRange, seekColumnFamilies, !seekColumnFamilies.isEmpty());
        } else {
            // move the source to an empty state
            parentRange = new Range(parentRange.getEndKey(), true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
            source.seek(parentRange, seekColumnFamilies, (!this.seekColumnFamilies.isEmpty()));
        }
    }

    private void advanceToNextFieldValue(String fieldVal) throws IOException {
        // seek the source iterator to the next fieldValue on the list

        fieldValueStringBuilder.delete(0, fieldValueStringBuilder.length());
        Key sKey = this.source.getTopKey();

        // Given a datatype, get the next, highest one on the set.
        fieldVal = this.fieldValueFilter.higher(fieldVal);

        if (log.isTraceEnabled()) {
            log.trace("\t\tcurrentFieldValue: " + this.currentFieldValue);
        }

        if (null == fieldVal) {
            // end of our fieldValues move to next row.
            sKey = new Key(sKey.followingKey(PartialKey.ROW));
        } else {
            fieldValueStringBuilder.append(fieldVal);

            // if we have datatypes, start back at the first one.
            if (null != this.dataTypeFilter) {
                fieldValueStringBuilder.append(Constants.NULL_BYTE_STRING);
                fieldValueStringBuilder.append(this.dataTypeFilter.first());
            }
            sKey = new Key(sKey.getRow(), sKey.getColumnFamily(), new Text(fieldValueStringBuilder.toString()));
        }
        if (log.isTraceEnabled()) {
            log.trace("\t\tadvanceToNextFieldValue, startKey: " + sKey);
        }

        if (parentRange.contains(sKey)) {
            parentRange = new Range(sKey, true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
            source.seek(parentRange, seekColumnFamilies, !seekColumnFamilies.isEmpty());
        } else {
            // move the source to an empty state
            parentRange = new Range(parentRange.getEndKey(), true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
            source.seek(parentRange, seekColumnFamilies, (!this.seekColumnFamilies.isEmpty()));
        }
    }

    /**
     *
     * @return true if we have a new key to return, false if the count is empty. This also resets current counters etc.
     * @throws IOException
     *             for issues with read/write
     */
    private boolean wrapUpCurrent() throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("wrapUpCurrent(), count:" + this.count);
        }
        this.topKey = null;
        this.topValue = null;

        // -----------------------------
        // if our key is empty i.e. all values outside of timestamp
        // we need to call find top again
        if (count == 0) {
            if (log.isTraceEnabled()) {
                log.trace("wrapUpCurrent count was empty, tossing key");
            }
            resetCurrentMarkers();
        } else {
            // otherwise return key/setTop
            this.topKey = this.buildReturnKey();
            this.topValue = this.buildReturnValue();
            resetCurrentMarkers();
        }

        return hasTop();
    }

    /**
     * Row : shardId Fam : fieldName Qual: fieldValue \x00 datatype
     *
     * @return our new top key with the aggregated columnVisibility
     */
    private Key buildReturnKey() {
        if (log.isTraceEnabled()) {
            log.trace("buildReturnKey, currentRow: " + this.currentRow);
            log.trace("buildReturnKey, currentFieldName: " + this.currentFieldName);
            log.trace("buildReturnKey, currentFieldValue: " + this.currentFieldValue);
        }

        Text cq = new Text(this.currentFieldValue);
        if (this.uniqByDataTypeOption) {
            TextUtil.textAppend(cq, this.currentDataType);
        }

        // Combine the column visibilities into a single one
        // NOTE: key.getColumnVisibility actually returns a Text object so we need to convert them
        Set<ColumnVisibility> columnVisibilities = new HashSet<>();
        for (Text t : this.visibilitySet) {
            columnVisibilities.add(new ColumnVisibility(t));
        }
        ColumnVisibility cv;
        try {
            cv = MarkingFunctions.Factory.createMarkingFunctions().combine(columnVisibilities);
        } catch (MarkingFunctions.Exception e) {
            log.error("Could not combine visibilities: " + visibilitySet + "  " + e);
            return null;
        }

        return new Key(this.currentRow, new Text(this.currentFieldName), cq, new Text(cv.getExpression()), this.maxTimeStamp);
    }

    /* TODO: make this a mutable long, also check wrap up current method */
    private Value buildReturnValue() {
        return new Value(Long.toString(count).getBytes());
    }

    /**
     * Reset all of our "current" state variables back to null/empty/zero. These are the current markers with respect to the current FieldName/FieldValue etc.
     * key we are trying to create.
     */
    private void resetCurrentMarkers() {
        this.currentRow = null;
        this.currentFieldName = null;
        this.currentFieldValue = null;
        this.currentDataType = null;
        this.count = 0;
        this.maxTimeStamp = 0;
        this.visibilitySet.clear();
    }

    /**
     * Basic method to find our topKey which matches our given FieldName,FieldValue.
     *
     * @throws IOException
     *             for issues with read/write
     */
    protected void findTop() throws IOException {
        resetCurrentMarkers();
        String cq;
        String fv;
        this.topKey = null;
        this.topValue = null;

        while (true) {
            if (!source.hasTop()) {
                log.trace("findTop: source does not have top");
                wrapUpCurrent();
                return;
            }

            Key key = source.getTopKey();
            if (log.isTraceEnabled()) {
                log.trace("findTop examining key: " + key);
            }

            // check that the key is within the parent range
            if (!parentRange.contains(key)) {
                if (log.isTraceEnabled()) {
                    log.trace("key is not in parentRange: " + key);
                }
                wrapUpCurrent();
                return;
            }

            // -------- Check ROW --------
            if (null == currentRow) {
                currentRow = key.getRow();
            }

            int rowCompare = currentRow.compareTo(key.getRow());
            if (rowCompare < 0) { // current row is behind
                if (log.isTraceEnabled()) {
                    log.trace("row changed current: " + currentRow + " , new: " + key.getRow());
                }
                if (wrapUpCurrent()) {// sets top key/val and resets counters
                    // we had something with a valid count we're done for this iteration
                    return;
                }

                // we had no current top key, so we reset the current markers, loop again.
                continue;
            } else if (rowCompare > 0) { // current row is ahead
                // issue seek to move key ahead.
                // is this condition really possible?
                throw new IllegalArgumentException("source iterator is behind us, how did this happen? Our currentRow: " + currentRow + " Source key- Row:"
                                + key.getRow() + " CF: " + key.getColumnFamily() + " CQ: " + key.getColumnQualifier() + " VIS: " + key.getColumnVisibility());

            } else { // same row - Compare CQ

                /**
                 * The only time we should trigger the following is if: 1. No field name was passed in (resulting in empty seekColumnFamilies) 2. Underlying
                 * iterator is NOT honoring seekColumnFamilies & inclusivity
                 */
                // if not a field index key, then advance to the next field index
                if (!isFieldIndexKey(key)) {

                    // If the underlying iterator honors seekColumnFamilies then this code will never be executed

                    if (wrapUpCurrent()) { // if we were working on something finish up and reset markers.
                        return;
                    }

                    if (!advanceToFieldIndex()) {
                        return;
                    }

                    continue;
                }

                // -------- Check FIELD NAME (COLFAM) --------
                if (null == currentFieldName) {
                    currentFieldName = key.getColumnFamily(); // seekColFams should ensure we are only on valid field names.
                }

                int fnCompare = currentFieldName.compareTo(key.getColumnFamily());
                if (fnCompare < 0) { // current field name is behind
                    if (log.isTraceEnabled()) {
                        log.trace("fieldName changed, current: " + currentFieldName + " , new: " + key.getColumnFamily());
                    }
                    if (wrapUpCurrent()) {// sets top key/val and resets counters
                        return;
                    }
                } else if (fnCompare > 0) { // current is ahead
                    throw new IllegalArgumentException("source iterator is behind us, how did this happen?" + " Iterator Row: " + currentRow + " FieldName: "
                                    + currentFieldName + " Source key- Row:" + key.getRow() + " CF: " + key.getColumnFamily() + " CQ: "
                                    + key.getColumnQualifier() + " VIS: " + key.getColumnVisibility());
                } else { // same field name

                    // check FIELD VALUE, there are multiple conditions here
                    // 1. we were given a set of field values to look for
                    // 2. we are doing all field values.

                    // parse column qualifier (from the end incase we have a nasty field value)
                    cq = key.getColumnQualifier().toString();
                    int idx2 = cq.lastIndexOf(Constants.NULL_BYTE_STRING);
                    if (idx2 < 0) {
                        log.error("Found an invalid field index key; expected cq of the form fv%00dt%00uid? " + key);
                        throw new IllegalArgumentException("Found an invalid field index key; expected cq of the form fv%00dt%00uid? " + key);
                    }
                    int idx1 = cq.lastIndexOf(Constants.NULL_BYTE_STRING, idx2 - 1);
                    if (idx1 < 0) {
                        log.error("Found an invalid field index key; expected cq of the form fv%00dt%00uid? " + key);
                        throw new IllegalArgumentException("Found an invalid field index key; expected cq of the form fv%00dt%00uid? " + key);
                    }
                    fv = cq.substring(0, idx1);

                    // if we're looking for spedific field values, this logic will skip ones
                    // not in our list.
                    if (null != this.fieldValueFilter && !this.fieldValueFilter.contains(fv)) {
                        if (log.isTraceEnabled()) {
                            log.trace("found field value I don't care about: " + fv);
                        }
                        // skip this key, seek to next value.
                        advanceToNextFieldValue(fv); // will issue a seek
                        continue;
                    }

                    if (null == this.currentFieldValue) {
                        this.currentFieldValue = fv;
                    }

                    if (!this.currentFieldValue.equals(fv)) { // we have a new field value
                        if (log.isTraceEnabled()) {
                            log.trace("field Value changed, current: " + currentFieldValue + " , new: " + fv);
                        }
                        if (wrapUpCurrent()) {// sets top key/val and resets counters
                            return;
                        }
                        continue;
                    } else { // same field value

                        // see if we care about data type
                        if (this.uniqByDataTypeOption || this.dataTypeFilter != null) {
                            if (log.isTraceEnabled()) {
                                log.trace("I care about data type");
                            }
                            // parse out the datatype
                            String dataType = cq.substring(idx1 + 1, idx2);

                            if (!uniqByDataTypeOption) { // we are returning aggregate types
                                // we only care if it's in our filter list, if not skip
                                // to the next one we care about.
                                if (!dataTypeFilter.contains(dataType)) {
                                    if (log.isTraceEnabled()) {
                                        log.trace("bad data type, skipping: " + key);
                                    }
                                    // it's not in our list, skip it
                                    advanceToNextDataType(fv, dataType); // will issue a seek
                                    continue;
                                }
                                // else -> we are good fall through & check timestamp.

                            } else { // we are returning uniq data types
                                if (null == this.currentDataType) {
                                    if (null == this.dataTypeFilter) { // if the dataTypeFilter is null then just accept it.
                                        this.currentDataType = dataType;
                                    } else {
                                        // check that it's in the filter
                                        if (dataTypeFilter.contains(dataType)) {
                                            this.currentDataType = dataType;
                                        } else { // it's not a datatype that we care about.
                                            if (log.isTraceEnabled()) {
                                                log.trace("uniqByDataTypeOption\t\tdropping data type: " + key);
                                            }
                                            if (wrapUpCurrent()) {
                                                return;
                                            }
                                            if (log.isTraceEnabled()) {
                                                log.trace("uniqByDataTypeOption\t count for previous key was zero, advancing datatype");
                                            }
                                            advanceToNextDataType(fv, dataType);
                                            continue;
                                        }
                                    }
                                } else if (!this.currentDataType.equals(dataType)) {
                                    if (log.isTraceEnabled()) {
                                        log.trace("data type changed: " + this.currentDataType + " , " + dataType);
                                    }
                                    // wrap up current
                                    if (wrapUpCurrent()) {
                                        return;
                                    }
                                    continue;
                                }
                            }
                        }

                        // check if this is a deleted key, and ignore if so
                        if (key.isDeleted()) {
                            if (log.isTraceEnabled()) {
                                log.trace("ignoring deleted key: " + key);
                            }
                        }
                        // check the timestamp.
                        // final acceptance test
                        else if (acceptTimestamp(key)) {
                            consume(key);
                            if (log.isTraceEnabled()) {
                                log.trace("consumed key: " + key);
                                log.trace("\tcurrentFieldValue: " + this.currentFieldValue);
                                log.trace("\tcurrentDataType: " + this.currentDataType);
                            }
                        } else {
                            if (log.isTraceEnabled()) {
                                log.trace("ignoring key outside time range: " + key);
                            }
                        }

                        // advance
                        source.next();
                    }
                }

            }

        }
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {

        // -----------------------------
        // set up timestamp range
        try {
            this.stamp_start = dateParser.parse(options.get(START_TIME)).getTime();
            this.stamp_end = dateParser.parse(options.get(STOP_TIME)).getTime();
            this.stamp_end += 999; // Since you only have the ability to tell us seconds, but our keys are in millis, make sure we give you the full second.

            // Note LongRange is inclusive only, so if start/stop is marked as
            // exclusive, inc/decrement by 1.
            if (options.get(START_TIME_INCL) != null) {
                if (!Boolean.parseBoolean(options.get(START_TIME_INCL))) {
                    this.stamp_start += 1;
                }
            }
            if (options.get(STOP_TIME_INCL) != null) {
                if (!Boolean.parseBoolean(options.get(STOP_TIME_INCL))) {
                    this.stamp_end -= 1000;
                }
            }
            this.stampRange = new LongRange(stamp_start, stamp_end);
        } catch (Exception e) {
            log.error("Invalid time range for " + FieldIndexCountingIterator.class.getName());
            return false;
        }

        // -----------------------------
        // Set the field names
        if (null == options.get(FIELD_NAMES)) {
            log.warn("FIELD_NAME not specified for " + FieldIndexCountingIterator.class.getName());
        } else {
            fieldNameFilter = new TreeSet<>(Arrays.asList(options.get(FIELD_NAMES).split(SEP)));
            if (fieldNameFilter.isEmpty()) {
                log.warn("FIELD_NAME empty for " + FieldIndexCountingIterator.class.getName());
                fieldNameFilter = null;
            }
            if (log.isDebugEnabled()) {
                StringBuilder b = new StringBuilder();
                for (String f : fieldNameFilter) {
                    b.append(f).append(" ");
                }

                log.debug("Iter configured with FieldNames: " + b);
            }
        }

        // -----------------------------
        // Set the field values
        if (null != options.get(FIELD_VALUES) && !options.get(FIELD_VALUES).trim().isEmpty()) {
            fieldValueFilter = new TreeSet<>(Arrays.asList(options.get(FIELD_VALUES).split(SEP)));
            if (fieldValueFilter.isEmpty()) {
                fieldValueFilter = null;
            }
        }

        // -----------------------------
        // See if we have a data type filter
        if (null != options.get(DATA_TYPES) && !options.get(DATA_TYPES).trim().isEmpty()) {
            this.dataTypeFilter = new TreeSet<>(Arrays.asList(options.get(DATA_TYPES).split(SEP)));
            if (log.isTraceEnabled()) {
                log.trace("data types: " + options.get(DATA_TYPES));
            }
        }

        // -----------------------------
        // See if we need to return counts per datatype
        if (null != options.get(UNIQ_BY_DATA_TYPE)) {
            this.uniqByDataTypeOption = Boolean.parseBoolean(options.get(UNIQ_BY_DATA_TYPE));
            if (log.isTraceEnabled()) {
                log.trace(UNIQ_BY_DATA_TYPE + ":" + uniqByDataTypeOption);
            }
        }

        return true;
    }

    // --------------------------------------------------------------------------
    // All timestamp stuff, taken from org.apache.accumulo.core.iterators.user.TimestampFilter
    private static SimpleDateFormat initDateParser() {
        SimpleDateFormat dateParser = new SimpleDateFormat(FieldIndexCountingIterator.DATE_FORMAT_STRING);
        dateParser.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateParser;
    }

    /**
     * A convenience method for setting the range of timestamps accepted by the timestamp filter.
     *
     * @param is
     *            the iterator setting object to configure
     * @param start
     *            the start timestamp, inclusive (yyyyMMddHHmmssz)
     * @param end
     *            the end timestamp, inclusive (yyyyMMddHHmmssz)
     */
    public static void setRange(IteratorSetting is, String start, String end) {
        setRange(is, start, true, end, true);
    }

    /**
     * A convenience method for setting the range of timestamps accepted by the timestamp filter.
     *
     * @param is
     *            the iterator setting object to configure
     * @param start
     *            the start timestamp (yyyyMMddHHmmssz)
     * @param startInclusive
     *            boolean indicating whether the start is inclusive
     * @param end
     *            the end timestamp (yyyyMMddHHmmssz)
     * @param endInclusive
     *            boolean indicating whether the end is inclusive
     */
    public static void setRange(IteratorSetting is, String start, boolean startInclusive, String end, boolean endInclusive) {
        setStart(is, start, startInclusive);
        setEnd(is, end, endInclusive);
    }

    /**
     * A convenience method for setting the start timestamp accepted by the timestamp filter.
     *
     * @param is
     *            the iterator setting object to configure
     * @param start
     *            the start timestamp (yyyyMMddHHmmssz)
     * @param startInclusive
     *            boolean indicating whether the start is inclusive
     */
    public static void setStart(IteratorSetting is, String start, boolean startInclusive) {
        is.addOption(START_TIME, start);
        is.addOption(START_TIME_INCL, Boolean.toString(startInclusive));
    }

    /**
     * A convenience method for setting the end timestamp accepted by the timestamp filter.
     *
     * @param is
     *            the iterator setting object to configure
     * @param end
     *            the end timestamp (yyyyMMddHHmmssz)
     * @param endInclusive
     *            boolean indicating whether the end is inclusive
     */
    public static void setEnd(IteratorSetting is, String end, boolean endInclusive) {
        is.addOption(STOP_TIME, end);
        is.addOption(STOP_TIME_INCL, Boolean.toString(endInclusive));
    }

    /**
     * A convenience method for setting the range of timestamps accepted by the timestamp filter.
     *
     * @param is
     *            the iterator setting object to configure
     * @param start
     *            the start timestamp, inclusive
     * @param end
     *            the end timestamp, inclusive
     */
    public static void setRange(IteratorSetting is, long start, long end) {
        setRange(is, start, true, end, true);
    }

    /**
     * A convenience method for setting the range of timestamps accepted by the timestamp filter.
     *
     * @param is
     *            the iterator setting object to configure
     * @param start
     *            the start timestamp
     * @param startInclusive
     *            boolean indicating whether the start is inclusive
     * @param end
     *            the end timestamp
     * @param endInclusive
     *            boolean indicating whether the end is inclusive
     */
    public static void setRange(IteratorSetting is, long start, boolean startInclusive, long end, boolean endInclusive) {
        setStart(is, start, startInclusive);
        setEnd(is, end, endInclusive);
    }

    /**
     * A convenience method for setting the start timestamp accepted by the timestamp filter.
     *
     * @param is
     *            the iterator setting object to configure
     * @param start
     *            the start timestamp
     * @param startInclusive
     *            boolean indicating whether the start is inclusive
     */
    public static void setStart(IteratorSetting is, long start, boolean startInclusive) {
        SimpleDateFormat dateParser = initDateParser();
        is.addOption(START_TIME, dateParser.format(new Date(start)));
        is.addOption(START_TIME_INCL, Boolean.toString(startInclusive));
    }

    /**
     * A convenience method for setting the end timestamp accepted by the timestamp filter.
     *
     * @param is
     *            the iterator setting object to configure
     * @param end
     *            the end timestamp
     * @param endInclusive
     *            boolean indicating whether the end is inclusive
     */
    public static void setEnd(IteratorSetting is, long end, boolean endInclusive) {
        SimpleDateFormat dateParser = initDateParser();
        is.addOption(STOP_TIME, dateParser.format(new Date(end)));
        is.addOption(STOP_TIME_INCL, Boolean.toString(endInclusive));
    }
}
