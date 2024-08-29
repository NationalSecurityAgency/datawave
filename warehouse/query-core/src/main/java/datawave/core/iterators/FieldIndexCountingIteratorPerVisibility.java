package datawave.core.iterators;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
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
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

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
public class FieldIndexCountingIteratorPerVisibility extends WrappingIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {

    protected static final Logger log = Logger.getLogger(FieldIndexCountingIteratorPerVisibility.class);
    // Config constants
    public static final String START_TIME = "FieldIndexCountingIterator.START_TIME";
    public static final String STOP_TIME = "FieldIndexCountingIterator.STOP_TIME";
    public static final String START_TIME_INCL = "FieldIndexCountingIterator.startInclusive";
    public static final String STOP_TIME_INCL = "FieldIndexCountingIterator.endInclusive";
    public static final String FIELD_NAMES = "FieldIndexCountingIterator.FIELD_NAMES";
    public static final String FIELD_VALUES = "FieldIndexCountingIterator.FIELD_VALUES";
    public static final String DATA_TYPES = "FieldIndexCountingIterator.DATA_TYPES";
    public static final String UNIQ_BY_DATA_TYPE = "FieldIndexCountingIterator.UNIQ_BY_DATA_TYPE";
    public static final String UNIQ_BY_VISIBILITY = "FieldIndexCountingIterator.UNIQ_BY_VISIBILITY";
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
    private boolean uniqByVisibilityOption = false; // returning counts separated by visibility
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
    private Map<Text,MutableInt> currentVisibilityCounts = null;

    private StringBuilder dataTypeStringBuilder = new StringBuilder();
    private StringBuilder fieldValueStringBuilder = new StringBuilder();

    public static final String ONE_BYTE_STRING = "\u0001";
    public static final String DATE_FORMAT_STRING = "yyyyMMddHHmmss";

    private Set<ColumnVisibility> columnVisibilities = Sets.newHashSet();
    private TreeMap<Key,Value> keyCache = null;

    // -------------------------------------------------------------------------
    // ------------- Constructors
    public FieldIndexCountingIteratorPerVisibility() {
        currentVisibilityCounts = new TreeMap<>();

        keyCache = new TreeMap<>();
    }

    public FieldIndexCountingIteratorPerVisibility(FieldIndexCountingIteratorPerVisibility other, IteratorEnvironment env) {
        this();
        this.source = other.getSource().deepCopy(env);
        this.seekColumnFamilies = new TreeSet<>(other.seekColumnFamilies);
        this.parentRange = new Range(other.parentRange);

        this.stamp_start = other.stamp_start;
        this.stamp_end = other.stamp_end;
        this.stampRange = new LongRange(stamp_start, stamp_end);

        this.uniqByDataTypeOption = other.uniqByDataTypeOption;
        this.uniqByVisibilityOption = other.uniqByVisibilityOption;
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
                log.trace("seekColumnFamilies for this iterator: " + (new String(bs.toArray())).replaceAll("\u0000", "%00"));
            }
        }

        this.keyCache.putAll(other.keyCache);
    }

    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        options.put(START_TIME, "The GMT start time for the scan using the format " + FieldIndexCountingIteratorPerVisibility.DATE_FORMAT_STRING);
        options.put(STOP_TIME, "The GMT stop time for the scan using the format " + FieldIndexCountingIteratorPerVisibility.DATE_FORMAT_STRING);
        options.put(START_TIME_INCL, "Boolean value denoting whether the start time is inclusive");
        options.put(STOP_TIME_INCL, "Boolean value denoting whether the stop time is inclusive");
        options.put(FIELD_NAMES, "The (optional) field names to count separated by \"" + SEP + "\"");
        options.put(FIELD_VALUES, "The (optional) field values to count separated by \"" + SEP + "\"");
        options.put(DATA_TYPES, "The (optional) data types to filter by");
        options.put(UNIQ_BY_DATA_TYPE, "Boolean value denoting whether the counts should separated by data type");
        options.put(UNIQ_BY_VISIBILITY, "Boolean value denoting whether the counts should separated by visibility");

        return new IteratorOptions(getClass().getSimpleName(), "An iterator used to count items in the field index", options, null);
    }

    /**
     * Get an IteratorSetting given a hadoop configuration
     *
     * @param conf
     *            a configuration
     * @return the iterator setting
     */
    public static IteratorSetting getIteratorSetting(Configuration conf) {
        Map<String,String> summaryIteratorSettings = new HashMap<>();
        putIfNotNull(summaryIteratorSettings, FieldIndexCountingIteratorPerVisibility.START_TIME,
                        conf.get(FieldIndexCountingIteratorPerVisibility.START_TIME, null));
        putIfNotNull(summaryIteratorSettings, FieldIndexCountingIteratorPerVisibility.STOP_TIME,
                        conf.get(FieldIndexCountingIteratorPerVisibility.STOP_TIME, null));
        putIfNotNull(summaryIteratorSettings, FieldIndexCountingIteratorPerVisibility.START_TIME_INCL,
                        conf.get(FieldIndexCountingIteratorPerVisibility.START_TIME_INCL, null));
        putIfNotNull(summaryIteratorSettings, FieldIndexCountingIteratorPerVisibility.STOP_TIME_INCL,
                        conf.get(FieldIndexCountingIteratorPerVisibility.STOP_TIME_INCL, null));
        putIfNotNull(summaryIteratorSettings, FieldIndexCountingIteratorPerVisibility.FIELD_NAMES,
                        conf.get(FieldIndexCountingIteratorPerVisibility.FIELD_NAMES, null));
        putIfNotNull(summaryIteratorSettings, FieldIndexCountingIteratorPerVisibility.FIELD_VALUES,
                        conf.get(FieldIndexCountingIteratorPerVisibility.FIELD_VALUES, null));
        putIfNotNull(summaryIteratorSettings, FieldIndexCountingIteratorPerVisibility.DATA_TYPES,
                        conf.get(FieldIndexCountingIteratorPerVisibility.DATA_TYPES, null));
        putIfNotNull(summaryIteratorSettings, FieldIndexCountingIteratorPerVisibility.UNIQ_BY_DATA_TYPE,
                        conf.get(FieldIndexCountingIteratorPerVisibility.UNIQ_BY_DATA_TYPE, null));
        putIfNotNull(summaryIteratorSettings, FieldIndexCountingIteratorPerVisibility.UNIQ_BY_VISIBILITY,
                        conf.get(FieldIndexCountingIteratorPerVisibility.UNIQ_BY_VISIBILITY, null));
        return new IteratorSetting(IteratorSettingHelper.BASE_ITERATOR_PRIORITY + 40, "FieldIndexCountingIterator",
                        FieldIndexCountingIteratorPerVisibility.class.getName(), summaryIteratorSettings);
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
        if (this.fieldNameFilter != null) {
            for (String fn : this.fieldNameFilter) {
                this.seekColumnFamilies.add(new ArrayByteSequence(Constants.FIELD_INDEX_PREFIX + fn));
            }
        }
        if (log.isTraceEnabled()) {
            for (ByteSequence bs : this.seekColumnFamilies) {
                log.trace("seekColumnFamilies for this iterator: " + (new String(bs.toArray())).replaceAll("\u0000", "%00"));
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
        return new FieldIndexCountingIteratorPerVisibility(this, env);
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
        if (!popCache()) {
            if (source.hasTop()) {
                updateCache();
            }
            popCache();
        }
    }

    private boolean popCache() {
        this.topKey = null;
        this.topValue = null;

        if (!keyCache.isEmpty()) {
            Map.Entry<Key,Value> entry = keyCache.pollFirstEntry();
            this.topKey = entry.getKey();
            this.topValue = entry.getValue();
            return true;
        }

        return false;
    }

    @Override
    public void seek(Range r, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.parentRange = new Range(r);

        this.topKey = null;
        this.topValue = null;

        if (log.isTraceEnabled()) {
            log.trace("begin seek, range: " + parentRange);
            for (ByteSequence bs : this.seekColumnFamilies) {
                log.trace("seekColumnFamilies for this iterator: " + (new String(bs.toArray())).replaceAll("\u0000", "%00"));
            }
        }

        Key pStartKey = parentRange.getStartKey();

        // Check if we are recovering from IterationInterruptedException
        if (null != pStartKey && null != pStartKey.getRow() && null != pStartKey.getColumnFamily() && !pStartKey.getColumnFamily().toString().isEmpty()
                        && null != pStartKey.getColumnQualifier() && !pStartKey.getColumnQualifier().toString().isEmpty()
                        && !parentRange.isStartKeyInclusive()) {

            Key startKey = new Key(pStartKey.getRow(), pStartKey.getColumnFamily(), new Text(pStartKey.getColumnQualifier() + ONE_BYTE_STRING));
            this.parentRange = new Range(startKey, true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
            source.seek(parentRange, this.seekColumnFamilies, (!this.seekColumnFamilies.isEmpty()));
        } else {
            source.seek(parentRange, this.seekColumnFamilies, (!this.seekColumnFamilies.isEmpty()));
        }

        // if the start key of our bounding range > parentKey.endKey we can stop
        if (!source.hasTop() || !parentRange.contains(source.getTopKey())) {
            if (log.isTraceEnabled()) {
                log.trace("startKey is outside parentRange, done.");
            }
            return;
        }

        // advance to the field index if needed
        advanceToFieldIndex();

        // now if we still have some cached keys, then reset the parentRange to include the next one
        if (!keyCache.isEmpty()) {
            parentRange = new Range(keyCache.firstKey(), true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
        }

        // now get the next top key
        next();

        if (log.isTraceEnabled()) {
            log.trace("seek, topKey : " + ((null == topKey) ? "null" : topKey));
        }
    }

    @Override
    public String toString() {
        return "FieldIndexCountingIterator{" + "stamp_start=" + stamp_start + ", stamp_end=" + stamp_end + ", uniqByDataTypeOption=" + uniqByDataTypeOption
                        + ", uniqByVisibilityOption=" + uniqByVisibilityOption + ", currentRow=" + currentRow + ", currentFieldValue=" + currentFieldValue
                        + ", currentDataType=" + currentDataType + '}';
    }

    // -------------------------------------------------------------------------
    // ------------- other stuff

    /**
     * Given a Key to consume, update any necessary counters etc.
     *
     * @param key
     *            a key
     */
    private void consume(Key key) {

        if (log.isTraceEnabled()) {
            log.trace("consume, key: " + key);
        }

        // update the visibility set
        MutableInt counter = this.currentVisibilityCounts.get(key.getColumnVisibility());
        if (counter == null) {
            this.currentVisibilityCounts.put(key.getColumnVisibility(), new MutableInt(1));
        } else {
            counter.increment();
        }

        // update current count
        this.count += 1;

        // set most recent timestamp
        this.maxTimeStamp = (this.maxTimeStamp > key.getTimestamp()) ? maxTimeStamp : CompositeTimestamp.getEventDate(key.getTimestamp());
    }

    private boolean acceptTimestamp(Key k) {
        return this.stampRange.containsLong(CompositeTimestamp.getEventDate(k.getTimestamp()));
    }

    private boolean isFieldIndexKey(Key key) {
        Text cf = key.getColumnFamily();
        return (cf.getLength() >= 3 && cf.charAt(0) == 'f' && cf.charAt(1) == 'i' && cf.charAt(2) == '\0');
    }

    private void advanceToFieldIndex() throws IOException {
        if (!source.hasTop()) {
            return;
        }

        if (!isFieldIndexKey(source.getTopKey())) {
            Text cFam = new Text(Constants.FIELD_INDEX_PREFIX + (this.fieldNameFilter == null ? "" : this.fieldNameFilter.first()));
            Text cQual = new Text();
            if (null != this.fieldValueFilter) {
                TextUtil.textAppendNoNull(cQual, this.fieldValueFilter.first());
                if (null != this.dataTypeFilter) {
                    TextUtil.textAppend(cQual, this.dataTypeFilter.first());
                }
            }
            Key startKey = new Key(source.getTopKey().getRow(), cFam, cQual);

            if (log.isTraceEnabled()) {
                log.trace("seeking to field index: " + startKey);
            }

            if (startKey.compareTo(parentRange.getEndKey()) > 0) {
                Range r = new Range(parentRange.getEndKey(), true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
                source.seek(r, this.seekColumnFamilies, (!this.seekColumnFamilies.isEmpty()));
            } else {
                Range r = new Range(startKey, true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
                source.seek(r, this.seekColumnFamilies, (!this.seekColumnFamilies.isEmpty()));
            }
        }
    }

    private void advanceToNextFieldIndex() throws IOException {
        if (!source.hasTop()) {
            return;
        }

        // seek to the next row
        Text row = new Text(source.getTopKey().getRow());
        TextUtil.textAppend(row, "\0");
        Key key = new Key(row);
        if (key.compareTo(parentRange.getEndKey()) > 0) {
            source.seek(new Range(parentRange.getEndKey(), true, parentRange.getEndKey(), parentRange.isEndKeyInclusive()), this.seekColumnFamilies,
                            (!this.seekColumnFamilies.isEmpty()));
        } else {
            source.seek(new Range(new Key(row), true, parentRange.getEndKey(), parentRange.isEndKeyInclusive()), this.seekColumnFamilies,
                            (!this.seekColumnFamilies.isEmpty()));
        }

        // if the start key of our bounding range > parentKey.endKey we can stop
        if (!source.hasTop() || !parentRange.contains(source.getTopKey())) {
            if (log.isTraceEnabled()) {
                log.trace("startKey is outside parentRange, done.");
            }
            return;
        }

        advanceToFieldIndex();
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
        if (sKey.compareTo(parentRange.getEndKey()) > 0) {
            parentRange = new Range(parentRange.getEndKey(), true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
            source.seek(parentRange, seekColumnFamilies, (!this.seekColumnFamilies.isEmpty()));
        } else {
            parentRange = new Range(sKey, true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
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

        if (sKey.compareTo(parentRange.getEndKey()) > 0) {
            parentRange = new Range(parentRange.getEndKey(), true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
            source.seek(parentRange, seekColumnFamilies, (!this.seekColumnFamilies.isEmpty()));
        } else {
            parentRange = new Range(sKey, true, parentRange.getEndKey(), parentRange.isEndKeyInclusive());
            source.seek(parentRange, seekColumnFamilies, (!this.seekColumnFamilies.isEmpty()));
        }
    }

    /**
     *
     * @return true if we have a new key to return, false if the count is empty. This also resets current counters etc.
     * @throws java.io.IOException
     *             for issues with read/write
     */
    private boolean wrapUpCurrent() throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("wrapUpCurrent(), count:" + this.count);
        }
        // if our key is empty i.e. all values outside of timestamp
        // we need to call find top again
        if (count == 0) {
            if (log.isTraceEnabled()) {
                log.trace("wrapUpCurrent count was empty, tossing key");
            }
            resetCurrent();
        } else {
            // fill the cache
            this.keyCache.putAll(buildReturnKeys());

            // reset for the next round
            resetCurrent();
        }

        return (!this.keyCache.isEmpty());
    }// -----------------------------

    /**
     * Row : shardId Fam : fieldName Qual: fieldValue \x00 datatype
     *
     * @return a map of the keys and values
     */
    private Map<Key,Value> buildReturnKeys() {
        if (log.isTraceEnabled()) {
            log.trace("buildReturnKeys, currentRow: " + this.currentRow);
            log.trace("buildReturnKeys, currentFieldName: " + this.currentFieldName);
            log.trace("buildReturnKeys, currentFieldValue: " + this.currentFieldValue);
        }

        Text cq = new Text(this.currentFieldValue);
        if (this.uniqByDataTypeOption) {
            TextUtil.textAppend(cq, this.currentDataType);
        }

        // if doing this by visibility, then build a set of Keys
        if (this.uniqByVisibilityOption) {
            Map<Key,Value> keys = new TreeMap<>();
            for (Map.Entry<Text,MutableInt> visibility : currentVisibilityCounts.entrySet()) {
                Key key = new Key(this.currentRow, new Text(this.currentFieldName), cq, visibility.getKey(), this.maxTimeStamp);
                Value value = new Value(Long.toString(visibility.getValue().intValue()).getBytes());
                keys.put(key, value);
            }
            return keys;
        } else {
            for (Text visibility : currentVisibilityCounts.keySet()) {
                try {
                    this.columnVisibilities.add(new ColumnVisibility(visibility));
                } catch (Exception e) {
                    log.error("Error parsing columnVisibility of key", e);
                }
            }

            ColumnVisibility cv = null;
            try {
                // Calculate the columnVisibility for this key from the combiner.
                cv = MarkingFunctions.Factory.createMarkingFunctions().combine(columnVisibilities);
            } catch (Exception e) {
                log.error("Could not create combined columnVisibility for the count", e);
                return null;
            }

            Key key = new Key(this.currentRow, new Text(this.currentFieldName), cq, new Text(cv.getExpression()), this.maxTimeStamp);
            Value value = new Value(Long.toString(count).getBytes());
            return Collections.singletonMap(key, value);
        }
    }

    private void resetCurrent() {
        this.currentRow = null;
        this.currentFieldName = null;
        this.currentFieldValue = null;
        this.currentDataType = null;
        this.count = 0;
        this.maxTimeStamp = 0;
        this.columnVisibilities.clear();
        this.currentVisibilityCounts.clear();
    }

    /**
     * Basic method to find our topKey which matches our given FieldName,FieldValue.
     *
     * @throws java.io.IOException
     *             for issues with read/write
     */
    protected void updateCache() throws IOException {
        resetCurrent();
        String cq;
        String fv;

        while (true) {
            if (!source.hasTop()) {
                log.trace("Source does not have top");
                wrapUpCurrent();
                return;
            }

            Key key = source.getTopKey();
            if (log.isTraceEnabled()) {
                log.trace("updateCache examining key: " + key);
            }

            // check that the key is within the parent range
            if (!parentRange.contains(key)) {
                if (log.isTraceEnabled()) {
                    log.trace("key is not in parentRange: " + key);
                }
                wrapUpCurrent();
                return;
            }

            // Check ROW
            if (null == currentRow) {
                currentRow = key.getRow();
            }

            int rowCompare = currentRow.compareTo(key.getRow());
            if (rowCompare < 0) { // current row is behind
                if (log.isTraceEnabled()) {
                    log.trace("row changed current: " + currentRow + " , new: " + key.getRow());
                }
                if (wrapUpCurrent()) {// sets top key/val and resets counters
                    return;
                }
                continue;
            } else if (rowCompare > 0) { // current row is ahead
                // issue seek to move key ahead.
                // is this condition really possible?
                throw new IllegalArgumentException("source iterator is behind us, how did this happen? OurRow: " + currentRow + " Source key- Row:"
                                + key.getRow() + " CF: " + key.getColumnFamily() + " CQ: " + key.getColumnQualifier() + " VIS: " + key.getColumnVisibility());
            } else { // same row

                // if not a field index key, then advance to the next field index
                if (!isFieldIndexKey(key)) {
                    if (wrapUpCurrent()) {// sets top key/val and resets counters
                        return;
                    }
                    advanceToNextFieldIndex();
                    continue;
                }

                // check FIELD NAME (COLFAM)
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
                    throw new IllegalArgumentException("source iterator is behind us, how did this happen?" + " Our Row: " + currentRow + " FieldName: "
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
                        // skip this key. potential optimization, seek to next value.
                        advanceToNextFieldValue(fv); // will issue a seek
                        // source.next();
                        continue;
                    }

                    if (null == this.currentFieldValue) {
                        this.currentFieldValue = fv;
                    }

                    int fvCompare = this.currentFieldValue.compareTo(fv);
                    if (fvCompare != 0) { // we have a new field value
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
                                    // source.next();
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
                                            // source.next();
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
            // Note LongRange is inclusive only, so if start/stop is marked as
            // exclusive, inc/decrement by 1.
            if (options.get(START_TIME_INCL) != null) {
                if (!Boolean.parseBoolean(options.get(START_TIME_INCL))) {
                    this.stamp_start += 1;
                }
            }
            if (options.get(STOP_TIME_INCL) != null) {
                if (!Boolean.parseBoolean(options.get(STOP_TIME_INCL))) {
                    this.stamp_end -= 1;
                }
            }
            this.stampRange = new LongRange(stamp_start, stamp_end);
        } catch (Exception e) {
            log.error("Invalid time range for " + FieldIndexCountingIteratorPerVisibility.class.getName());
            return false;
        }

        // -----------------------------
        // Set the field names
        if (null == options.get(FIELD_NAMES)) {
            log.warn("FIELD_NAME not specified for " + FieldIndexCountingIteratorPerVisibility.class.getName());
        } else {
            fieldNameFilter = new TreeSet<>(Arrays.asList(options.get(FIELD_NAMES).split(SEP)));
            if (fieldNameFilter.isEmpty()) {
                log.warn("FIELD_NAME empty for " + FieldIndexCountingIteratorPerVisibility.class.getName());
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

        // -----------------------------
        // See if we need to return counts per visibility
        if (null != options.get(UNIQ_BY_VISIBILITY)) {
            this.uniqByVisibilityOption = Boolean.parseBoolean(options.get(UNIQ_BY_VISIBILITY));
            if (log.isTraceEnabled()) {
                log.trace(UNIQ_BY_VISIBILITY + ":" + uniqByVisibilityOption);
            }
        }

        return true;
    }

    // --------------------------------------------------------------------------
    // All timestamp stuff, taken from org.apache.accumulo.core.iterators.user.TimestampFilter
    private static SimpleDateFormat initDateParser() {
        SimpleDateFormat dateParser = new SimpleDateFormat(FieldIndexCountingIteratorPerVisibility.DATE_FORMAT_STRING);
        dateParser.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateParser;
    }

    /**
     * A convenience method for setting the range of timestamps accepted by the timestamp filter.
     *
     * @param is
     *            the iterator setting object to configure
     * @param start
     *            the stamp_start timestamp, inclusive (yyyyMMddHHmmssz)
     * @param end
     *            the stamp_end timestamp, inclusive (yyyyMMddHHmmssz)
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
     *            the stamp_start timestamp (yyyyMMddHHmmssz)
     * @param startInclusive
     *            boolean indicating whether the stamp_start is inclusive
     * @param end
     *            the stamp_end timestamp (yyyyMMddHHmmssz)
     * @param endInclusive
     *            boolean indicating whether the stamp_end is inclusive
     */
    public static void setRange(IteratorSetting is, String start, boolean startInclusive, String end, boolean endInclusive) {
        setStart(is, start, startInclusive);
        setEnd(is, end, endInclusive);
    }

    /**
     * A convenience method for setting the stamp_start timestamp accepted by the timestamp filter.
     *
     * @param is
     *            the iterator setting object to configure
     * @param start
     *            the stamp_start timestamp (yyyyMMddHHmmssz)
     * @param startInclusive
     *            boolean indicating whether the stamp_start is inclusive
     */
    public static void setStart(IteratorSetting is, String start, boolean startInclusive) {
        is.addOption(START_TIME, start);
        is.addOption(START_TIME_INCL, Boolean.toString(startInclusive));
    }

    /**
     * A convenience method for setting the stamp_end timestamp accepted by the timestamp filter.
     *
     * @param is
     *            the iterator setting object to configure
     * @param end
     *            the stamp_end timestamp (yyyyMMddHHmmssz)
     * @param endInclusive
     *            boolean indicating whether the stamp_end is inclusive
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
     *            the stamp_start timestamp, inclusive
     * @param end
     *            the stamp_end timestamp, inclusive
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
     *            the stamp_start timestamp
     * @param startInclusive
     *            boolean indicating whether the stamp_start is inclusive
     * @param end
     *            the stamp_end timestamp
     * @param endInclusive
     *            boolean indicating whether the stamp_end is inclusive
     */
    public static void setRange(IteratorSetting is, long start, boolean startInclusive, long end, boolean endInclusive) {
        setStart(is, start, startInclusive);
        setEnd(is, end, endInclusive);
    }

    /**
     * A convenience method for setting the stamp_start timestamp accepted by the timestamp filter.
     *
     * @param is
     *            the iterator setting object to configure
     * @param start
     *            the stamp_start timestamp
     * @param startInclusive
     *            boolean indicating whether the stamp_start is inclusive
     */
    public static void setStart(IteratorSetting is, long start, boolean startInclusive) {
        SimpleDateFormat dateParser = initDateParser();
        is.addOption(START_TIME, dateParser.format(new Date(start)));
        is.addOption(START_TIME_INCL, Boolean.toString(startInclusive));
    }

    /**
     * A convenience method for setting the stamp_end timestamp accepted by the timestamp filter.
     *
     * @param is
     *            the iterator setting object to configure
     * @param end
     *            the stamp_end timestamp
     * @param endInclusive
     *            boolean indicating whether the stamp_end is inclusive
     */
    public static void setEnd(IteratorSetting is, long end, boolean endInclusive) {
        SimpleDateFormat dateParser = initDateParser();
        is.addOption(STOP_TIME, dateParser.format(new Date(end)));
        is.addOption(STOP_TIME_INCL, Boolean.toString(endInclusive));
    }
}
