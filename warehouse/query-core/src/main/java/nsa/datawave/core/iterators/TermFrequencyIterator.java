package nsa.datawave.core.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.util.StringUtils;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;

/**
 * 
 * An iterator for the Datawave shard table, it searches TermFrequency keys for a list of terms and values. It is assumed that the range specified includes all
 * of the documents of interest.
 * 
 * TermFrequency keys: {shardId}:tf:datatype\0uid\0{fieldValue}:{fieldName} {TermWeight protobuf}
 * 
 */
@SuppressWarnings("rawtypes")
public class TermFrequencyIterator extends WrappingIterator {
    public static final Logger log = Logger.getLogger(TermFrequencyIterator.class);
    
    protected Key topKey = null;
    protected Value topValue = null;
    
    protected List<FieldValue> fieldValues = new ArrayList<FieldValue>();
    
    public static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();
    
    public static final int MAX_SCAN_BEFORE_SEEK = 32;
    // The min distance before we seek. If there are less than 5 characters that match, then seek.
    public static final double MIN_DISTANCE_BEFORE_SEEK = 1d / 5d;
    
    protected Range initialSeekRange;
    // This iterator can only have a single columnFamily which is tf
    protected Collection<ByteSequence> seekColumnFamilies = Collections.<ByteSequence> singleton(new ArrayByteSequence(Constants.TERM_FREQUENCY_COLUMN_FAMILY
                    .getBytes(), 0, Constants.TERM_FREQUENCY_COLUMN_FAMILY.getLength()));
    protected boolean seekColumnFamiliesInclusive = true;
    
    // Wrapping iterator only accesses its private source in setSource and getSource
    // Since this class overrides these methods, it's safest to keep the source declaration here
    protected SortedKeyValueIterator<Key,Value> source;
    
    // -------------------------------------------------------------------------
    // ------------- Constructors
    public TermFrequencyIterator() {}
    
    public TermFrequencyIterator(Multimap<String,String> fieldValues) {
        for (Map.Entry<String,String> entry : fieldValues.entries()) {
            this.fieldValues.add(new FieldValue(entry.getKey(), entry.getValue()));
        }
        Collections.sort(this.fieldValues);
        if (log.isTraceEnabled()) {
            log.trace("FieldValues: " + this.fieldValues);
        }
    }
    
    public TermFrequencyIterator(TermFrequencyIterator other, IteratorEnvironment env) {
        this.source = other.getSource().deepCopy(env);
        this.fieldValues.addAll(other.fieldValues);
        if (log.isTraceEnabled()) {
            log.trace("FieldValues: " + this.fieldValues);
        }
    }
    
    // -------------------------------------------------------------------------
    // ------------- Overrides
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
    }
    
    @Override
    protected void setSource(SortedKeyValueIterator<Key,Value> source) {
        this.source = source;
    }
    
    @Override
    protected SortedKeyValueIterator<Key,Value> getSource() {
        return source;
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new TermFrequencyIterator(this, env);
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
        if (log.isTraceEnabled())
            log.trace("next() called");
        if (!source.hasTop()) {
            this.topKey = null;
            return;
        }
        source.next();
        findTop();
        
    }
    
    @Override
    public void seek(Range r, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        this.initialSeekRange = new Range(r);
        
        this.topKey = null;
        this.topValue = null;
        
        if (log.isTraceEnabled()) {
            log.trace("begin seek, range: " + initialSeekRange);
            for (ByteSequence bs : this.seekColumnFamilies) {
                log.trace("seekColumnFamilies for this iterator: " + (new String(bs.toArray())).replaceAll(Constants.NULL_BYTE_STRING, "%00"));
            }
        }
        
        // update the start key to the first possible match
        Range initialSeek = getNextRange(r.getStartKey(), r.isStartKeyInclusive());
        
        if (log.isTraceEnabled()) {
            log.trace("updated initial seek range: " + initialSeek);
        }
        
        source.seek(initialSeek, this.seekColumnFamilies, this.seekColumnFamiliesInclusive);
        findTop();
        
        if (log.isTraceEnabled()) {
            log.trace("seek, topKey : " + ((null == topKey) ? "null" : topKey));
        }
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("TermFrequencyIterator{range=").append(initialSeekRange).append("; fieldValues=").append(this.fieldValues).append("}");
        return builder.toString();
    }
    
    public static String getDataTypeUid(String cq) {
        int index = cq.indexOf('\0');
        if (index < 0) {
            return null;
        }
        index = cq.indexOf('\0', index + 1);
        if (index < 0) {
            return cq;
        }
        return cq.substring(0, index);
    }
    
    protected Range getNextRange(Key key, boolean inclusive) {
        // the key would only be null with a full table scan range; find the first row
        if (key == null) {
            return getRange(key, false);
        }
        
        // validate the row
        Text row = key.getRow();
        if (row == null) {
            return getRange(key, false);
        }
        
        // validate the cf
        Text cf = key.getColumnFamily();
        
        // if not in the term frequency section yet, then start there
        if ((cf == null) || (cf.compareTo(Constants.TERM_FREQUENCY_COLUMN_FAMILY) < 0)) {
            return getRange(new Key(row, Constants.TERM_FREQUENCY_COLUMN_FAMILY), false);
        }
        
        // if past the term frequency section, then we are done
        if (cf.compareTo(Constants.TERM_FREQUENCY_COLUMN_FAMILY) > 0) {
            return getRange(initialSeekRange.getEndKey(), false);
        }
        
        // pull apart the cq
        String cq = (key.getColumnQualifier() == null ? null : key.getColumnQualifier().toString());
        FieldValue fv = FieldValue.getFieldValue(cq);
        
        // if the field value is null, then the best we have is a datatype/uid in the cq if anything
        if (fv == null) {
            String dataTypeUid = getDataTypeUid(cq);
            // if not even a datatype/uid, then lets simply find the first one
            if (dataTypeUid == null) {
                return getRange(key, false);
            }
            // else start with the first value/field
            else {
                return getRange(new Key(row, cf, new Text(dataTypeUid + '\0' + this.fieldValues.get(0).getValueField())), true);
            }
        }
        
        // find the next field value equal to or greater that this one, depending on the inclusivity
        int index = Collections.binarySearch(this.fieldValues, fv);
        // if we found this entry in out list
        if (index >= 0) {
            return getRange(key, inclusive);
        } else {
            index = (index + 1) * -1;
            String dataTypeUid = getDataTypeUid(cq);
            // if past the end, then go to the next uid
            if (index == this.fieldValues.size()) {
                return getRange(new Key(row, cf, new Text(dataTypeUid + '\1')), false);
            } else {
                fv = this.fieldValues.get(index);
                return getRange(new Key(row, cf, new Text(dataTypeUid + '\0' + fv.getValueField())), true);
            }
        }
    }
    
    protected Range getRange(Key startKey, boolean inclusive) {
        Key endKey = initialSeekRange.getEndKey();
        // if we are past the end of the range
        if (inclusive ? (endKey.compareTo(startKey) < 0) : (endKey.compareTo(startKey) <= 0)) {
            return new Range(startKey, inclusive, startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME), false);
        } else {
            return new Range(startKey, inclusive, endKey, initialSeekRange.isEndKeyInclusive());
        }
    }
    
    // need to build a range starting at the end of current row and seek the
    // source to it. If we get an IOException, that means we hit the end of the tablet.
    protected Text moveToNext(Key k) throws IOException {
        if (log.isTraceEnabled())
            log.trace("moveToNext()");
        
        this.source.seek(getNextRange(k, false), this.seekColumnFamilies, this.seekColumnFamiliesInclusive);
        
        if (source.hasTop()) {
            return source.getTopKey().getRow();
        } else {
            return null;
        }
    }
    
    /**
     * Basic method to find our topKey which matches our given FieldName,FieldValue.
     * 
     * @throws IOException
     */
    protected void findTop() throws IOException {
        if (log.isTraceEnabled())
            log.trace("findTop()");
        topKey = null;
        topValue = null;
        int count = 0;
        while (true) {
            if (!source.hasTop()) {
                if (log.isTraceEnabled())
                    log.trace("Source does not have top");
                break;
            }
            Key k = source.getTopKey();
            if (!initialSeekRange.contains(k)) {
                if (log.isTraceEnabled())
                    log.trace("Source is out of the parentRange");
                break;
            }
            count++;
            if (accept(k)) {
                topKey = k;
                topValue = source.getTopValue();
                break;
            } else if (shouldSeek(k, count)) {
                source.seek(getNextRange(k, false), this.seekColumnFamilies, this.seekColumnFamiliesInclusive);
                // reset the seek counter
                count = 0;
            } else {
                source.next();
            }
        }
    }
    
    protected boolean shouldSeek(Key k, int count) {
        // if the count is over our threshold, then lets seek
        if (count >= MAX_SCAN_BEFORE_SEEK) {
            return true;
        }
        // If the current key is far enough away from any of the given field values, then seek
        // otherwise we should continue the scan
        FieldValue fv = FieldValue.getFieldValue(k);
        int index = Collections.binarySearch(this.fieldValues, fv);
        // if we found this entry in out list, then certainly do not seek
        if (index >= 0) {
            return false;
        }
        
        index = (index + 1) * -1;
        // if past the last field value, then seek
        if (index == this.fieldValues.size()) {
            return true;
        }
        // get the next field value and make a guess
        else {
            FieldValue nextFv = this.fieldValues.get(index);
            double distance = fv.distance(nextFv);
            if (distance > MIN_DISTANCE_BEFORE_SEEK) {
                return true;
            } else {
                return false;
            }
        }
    }
    
    protected boolean accept(Key key) {
        FieldValue fv = FieldValue.getFieldValue(key);
        
        // if the field value is null, then the best we have is a datatype/uid in the cq if anything
        if (fv == null) {
            return false;
        }
        
        // find the next field value equal to or greater that this one, depending on the inclusivity
        int index = Collections.binarySearch(this.fieldValues, fv);
        
        // if we found this entry in out list
        if (index >= 0) {
            return true;
        } else {
            return false;
        }
    }
    
    /**
     * A field name and value which is sorted on <value>\0<name>
     */
    public static class FieldValue implements Comparable<FieldValue> {
        private int nullOffset;
        private String valueField;
        
        public FieldValue(String field, String value) {
            this.nullOffset = value.length();
            this.valueField = value + '\0' + field;
        }
        
        /**
         * A distance between this field value and another. Here we want a distance that correlates with the number of keys between here and there for the same.
         * Essentially we want the inverse of the number of bytes that match. document.
         * 
         * @param fv
         * @return a distance between here and there (negative means there is before here)
         */
        public double distance(FieldValue fv) {
            byte[] s1 = getValueField().getBytes();
            byte[] s2 = fv.getValueField().getBytes();
            int len = Math.min(s1.length, s2.length);
            
            int matches = 0;
            int lastCharDiff = 0;
            
            for (int i = 0; i <= len; i++) {
                lastCharDiff = getValue(s2, i) - getValue(s1, i);
                if (lastCharDiff == 0) {
                    matches++;
                } else {
                    break;
                }
            }
            
            return Math.copySign(1.0d / (matches + 1), lastCharDiff);
        }
        
        private int getValue(byte[] bytes, int index) {
            if (index >= bytes.length) {
                return 0;
            } else {
                return bytes[index];
            }
        }
        
        public String getValueField() {
            return valueField;
        }
        
        public String getField() {
            return valueField.substring(nullOffset + 1);
        }
        
        public String getValue() {
            return valueField.substring(0, nullOffset);
        }
        
        @Override
        public int compareTo(FieldValue o) {
            return valueField.compareTo(o.valueField);
        }
        
        @Override
        public int hashCode() {
            return valueField.hashCode();
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof FieldValue) {
                return valueField.equals(((FieldValue) obj).valueField);
            }
            return false;
        }
        
        @Override
        public String toString() {
            return getField() + " -> " + getValue();
        }
        
        public static FieldValue getFieldValue(Key key) {
            return getFieldValue(key.getColumnQualifier());
        }
        
        public static FieldValue getFieldValue(Text cqText) {
            if (cqText == null) {
                return null;
            }
            return getFieldValue(cqText.toString());
        }
        
        public static FieldValue getFieldValue(String cq) {
            if (cq == null) {
                return null;
            }
            
            // pull apart the cq
            String[] cqParts = StringUtils.split(cq, '\0');
            
            // if we do not even have the first datatype\0uid, then lets find it
            if (cqParts.length <= 2) {
                return null;
            }
            
            // get the value and field
            String value = "";
            String field = "";
            if (cqParts.length >= 4) {
                field = cqParts[cqParts.length - 1];
                value = cqParts[2];
                // in case the value had null characters therein
                for (int i = 3; i < (cqParts.length - 1); i++) {
                    value = value + '\0' + cqParts[i];
                }
            } else if (cqParts.length == 3) {
                value = cqParts[2];
            }
            
            return new FieldValue(field, value);
        }
        
    }
    
}
