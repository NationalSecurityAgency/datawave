package datawave.core.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import datawave.query.Constants;
import datawave.query.data.parsers.DatawaveKey;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.commons.lang3.StringUtils;
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
public class TermFrequencyIterator extends WrappingIterator {
    public static final Logger log = Logger.getLogger(TermFrequencyIterator.class);
    
    protected Key topKey = null;
    protected Value topValue = null;
    
    public static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<>();
    
    public static final int MAX_SCAN_BEFORE_SEEK = 32;
    // The min distance before we seek. If there are less than 5 characters that match, then seek.
    public static final double MIN_DISTANCE_BEFORE_SEEK = 1d / 5d;
    
    protected Range initialSeekRange;
    // This iterator can only have a single columnFamily which is tf
    protected Collection<ByteSequence> seekColumnFamilies = Collections.singleton(new ArrayByteSequence(Constants.TERM_FREQUENCY_COLUMN_FAMILY.getBytes(), 0,
                    Constants.TERM_FREQUENCY_COLUMN_FAMILY.getLength()));
    protected boolean seekColumnFamiliesInclusive = true;
    
    // Support for smart seeking
    protected Multimap<String,String> fieldValues;
    protected TreeSet<Key> keys; // shard:datatype\x00uid
    protected TreeSet<String> uids;
    protected TreeSet<String> values;
    protected TreeSet<String> fields;
    protected TreeSet<String> uidsAndValues;
    
    // Wrapping iterator only accesses its private source in setSource and getSource
    // Since this class overrides these methods, it's safest to keep the source declaration here
    protected SortedKeyValueIterator<Key,Value> source;
    
    // -------------------------------------------------------------------------
    // ------------- Constructors
    public TermFrequencyIterator() {}
    
    public TermFrequencyIterator(Multimap<String,String> fieldValues, Set<Key> keys) {
        this.fieldValues = fieldValues;
        this.keys = new TreeSet<>(keys);
        this.uids = buildUidsFromKeys(keys);
        this.fields = new TreeSet<>(fieldValues.keySet());
        this.values = new TreeSet<>(fieldValues.values());
        this.uidsAndValues = buildUidAndValuesInner(uids, values);
        
        if (this.keys == null) {
            throw new IllegalStateException("Keys must be set on TermFrequencyIterator, they were null.");
        }
    }
    
    public TermFrequencyIterator(TermFrequencyIterator other, IteratorEnvironment env) {
        this.source = other.getSource().deepCopy(env);
        this.fieldValues = other.fieldValues;
        this.keys = other.keys;
        this.uids = other.uids;
        this.fields = other.fields;
        this.values = other.values;
        this.uidsAndValues = other.uidsAndValues;
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
        
        // Seek the initial range, unmodified. Initial range points to a TLD 'tf:datatype\0uid'
        source.seek(r, this.seekColumnFamilies, this.seekColumnFamiliesInclusive);
        findTop();
        
        if (log.isTraceEnabled()) {
            log.trace("seek, topKey : " + ((null == topKey) ? "null" : topKey));
        }
    }
    
    @Override
    public String toString() {
        return "TermFrequencyIterator{range=" + initialSeekRange + "; fieldValues=" + this.fieldValues + "}";
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
            
            if (count > MAX_SCAN_BEFORE_SEEK) {
                seekToNext(k);
                count = 0;
            } else {
                String[] cqParts = StringUtils.split(k.getColumnQualifier().toString(), '\u0000');
                String value = getValueFromParts(cqParts); // parse once and pass..
                
                if (!uidMatches(cqParts) || shouldSeekByValue(value)) {
                    seekToNext(k);
                    count = 0; // reset count
                } else if (fieldValueAccepted(cqParts, value)) {
                    topKey = k;
                    topValue = source.getTopValue();
                    break;
                } else {
                    source.next();
                }
            }
            count++;
        }
    }
    
    private boolean fieldValueAccepted(String[] cqParts, String value) {
        String uid = cqParts[1];
        String uidAndValue = uid + '\u0000' + value;
        return uidsAndValues.contains(uidAndValue) && fields.contains(cqParts[cqParts.length - 1]);
    }
    
    private boolean uidMatches(String[] parts) {
        return uids.contains(parts[1]);
    }
    
    // This method is required because there might exist a value that contains a null byte.
    protected String getValueFromParts(String[] cqParts) {
        if (cqParts.length <= 2) {
            return null;
        } else if (cqParts.length == 3) {
            return cqParts[2];
        } else {
            // Reconstruct value with nulls
            StringBuilder sb = new StringBuilder();
            sb.append(cqParts[2]);
            for (int ii = 3; ii < cqParts.length - 1; ii++) {
                sb.append('\u0000').append(cqParts[ii]);
            }
            return sb.toString();
        }
    }
    
    /**
     * IFF this value is greater than the maximum search value or less than the first search value by a distance measure.
     */
    private boolean shouldSeekByValue(String value) {
        
        // First check if the current value exceeds the maximum value in the search space
        boolean valueGreaterThanMax = value.compareTo(values.last()) > 0;
        if (valueGreaterThanMax) {
            return true;
        }
        
        // Next check if we are before the minimum value in the search space
        boolean valueLessThanMin = value.compareTo(values.first()) < 0;
        if (valueLessThanMin) {
            // Next check distance
            String first = values.first();
            double distance = getDistance(value, first);
            // Iterator should seek if the distance between values exceeds the threshold
            return distance > MIN_DISTANCE_BEFORE_SEEK; // > 0.2d
        }
        return false;
    }
    
    private void seekToNext(Key k) throws IOException {
        Range seekRange = getNextSeekRange(k);
        source.seek(seekRange, this.seekColumnFamilies, this.seekColumnFamiliesInclusive);
    }
    
    protected double getDistance(String a, String b) {
        return getDistance(a.getBytes(), b.getBytes());
    }
    
    private double getDistance(byte[] a, byte[] b) {
        return getDistance(a, b, Math.min(a.length, b.length));
    }
    
    private double getDistance(byte[] a, byte[] b, int len) {
        int matches = 0;
        int lastCharDiff = 0;
        for (int ii = 0; ii < len; ii++) {
            lastCharDiff = b[ii] - a[ii];
            if (lastCharDiff == 0)
                matches++;
            else
                break;
        }
        return Math.copySign(1.0d / (matches + 1), lastCharDiff);
    }
    
    /**
     * Get the next seek range based on the provided set of keys and the current key
     *
     * @param k
     *            the current key
     * @return
     */
    public Range getNextSeekRange(Key k) {
        DatawaveKey tfKey = new DatawaveKey(k);
        String uidAndValue = tfKey.getUid() + '\u0000' + tfKey.getFieldValue();
        String next = uidsAndValues.higher(uidAndValue);
        if (next == null) {
            // Done, return an empty range.
            Key startKey = initialSeekRange.getEndKey();
            Key endKey = startKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME);
            return new Range(startKey, false, endKey, false);
        } else {
            // Seek to next document containing a hit
            Text cq = new Text(tfKey.getDataType() + '\u0000' + next);
            Key nextStart = new Key(k.getRow(), k.getColumnFamily(), cq);
            return new Range(nextStart, false, initialSeekRange.getEndKey(), initialSeekRange.isEndKeyInclusive());
        }
    }
    
    public TreeSet<Key> getKeys() {
        return this.keys;
    }
    
    public void setKeys(Set<Key> keys) {
        this.keys = new TreeSet<>(keys);
    }
    
    public TreeSet<String> buildUidsFromKeys(Collection<Key> keys) {
        TreeSet<String> uids = new TreeSet<>();
        for (Key key : keys) {
            DatawaveKey datawaveKey = new DatawaveKey(key);
            uids.add(datawaveKey.getUid());
        }
        return uids;
    }
    
    public TreeSet<String> buildUidAndValuesInner(TreeSet<String> uids, TreeSet<String> values) {
        TreeSet<String> uidsAndValues = new TreeSet<>();
        for (String uid : uids) {
            for (String value : values) {
                uidsAndValues.add(uid + '\u0000' + value);
            }
        }
        return uidsAndValues;
    }
}
