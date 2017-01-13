package nsa.datawave.core.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.query.rewrite.predicate.TimeFilter;
import nsa.datawave.util.StringUtils;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.google.common.base.Predicate;

/**
 * 
 * An iterator for the Datawave shard table, it searches FieldIndex keys and returns Event keys (its topKey must be an Event key).
 *
 * This version takes a range (lowerbound/inclusive flag and upperbound/inclusive flag) and returns sorted UIDs in that range.
 * 
 * FieldIndex keys: fi\0{fieldName}:{fieldValue}\0datatype\0uid
 * 
 * Event key: CF, {datatype}\0{UID}
 * 
 */
public class DatawaveFieldIndexRangeIteratorJexl extends DatawaveFieldIndexCachingIteratorJexl {
    protected Text upperBound = null;
    protected boolean upperInclusive = true;
    protected boolean lowerInclusive = true;
    
    // -------------------------------------------------------------------------
    // ------------- Constructors
    public DatawaveFieldIndexRangeIteratorJexl() {
        super();
    }
    
    @SuppressWarnings("hiding")
    public DatawaveFieldIndexRangeIteratorJexl(Text fieldName, Text lowerBound, boolean lowerInclusive, Text upperBound, boolean upperInclusive,
                    TimeFilter timeFilter, Predicate<Key> datatypeFilter, long scanThreshold, long scanTimeout, int bufferSize, int maxRangeSplit,
                    FileSystem fs, Path uniqueDir, boolean allowDirReuse) {
        this(fieldName, lowerBound, lowerInclusive, upperBound, upperInclusive, timeFilter, datatypeFilter, false, scanThreshold, scanTimeout, bufferSize,
                        maxRangeSplit, fs, uniqueDir, allowDirReuse);
    }
    
    @SuppressWarnings("hiding")
    public DatawaveFieldIndexRangeIteratorJexl(Text fieldName, Text lowerBound, boolean lowerInclusive, Text upperBound, boolean upperInclusive,
                    TimeFilter timeFilter, Predicate<Key> datatypeFilter, boolean neg, long scanThreshold, long scanTimeout, int bufferSize, int maxRangeSplit,
                    FileSystem fs, Path uniqueDir, boolean allowDirReuse) {
        this(fieldName, lowerBound, lowerInclusive, upperBound, upperInclusive, timeFilter, datatypeFilter, neg, scanThreshold, scanTimeout, bufferSize,
                        maxRangeSplit, fs, uniqueDir, allowDirReuse, DEFAULT_RETURN_KEY_TYPE, true);
    }
    
    @SuppressWarnings("hiding")
    public DatawaveFieldIndexRangeIteratorJexl(Text fieldName, Text lowerBound, boolean lowerInclusive, Text upperBound, boolean upperInclusive,
                    TimeFilter timeFilter, Predicate<Key> datatypeFilter, boolean neg, long scanThreshold, long scanTimeout, int bufferSize, int maxRangeSplit,
                    FileSystem fs, Path uniqueDir, boolean allowDirReuse, PartialKey returnKeyType, boolean sortedUIDs) {
        super(fieldName, lowerBound, timeFilter, datatypeFilter, neg, scanThreshold, scanTimeout, bufferSize, maxRangeSplit, fs, uniqueDir, allowDirReuse,
                        returnKeyType, sortedUIDs);
        this.lowerInclusive = lowerInclusive;
        this.upperBound = upperBound;
        this.upperInclusive = upperInclusive;
    }
    
    public DatawaveFieldIndexRangeIteratorJexl(DatawaveFieldIndexRangeIteratorJexl other, IteratorEnvironment env) {
        super(other, env);
        this.lowerInclusive = other.lowerInclusive;
        this.upperBound = other.upperBound;
        this.upperInclusive = other.upperInclusive;
    }
    
    // -------------------------------------------------------------------------
    // ------------- Overrides
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new DatawaveFieldIndexRangeIteratorJexl(this, env);
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DatawaveFieldIndexRangeIteratorJexl{fName=").append(getFieldName()).append(", lowerBound=").append(getFieldValue())
                        .append(", lowerInclusive=").append(lowerInclusive).append(", upperBound=").append(upperBound).append(", upperInclusive=")
                        .append(upperInclusive).append(", negated=").append(isNegated()).append("}");
        return builder.toString();
    }
    
    @Override
    protected List<Range> buildBoundingFiRanges(Text rowId, Text fiName, Text fieldValue) {
        Key startKey = null;
        Key endKey = null;
        // construct new range
        
        // we cannot simply use startKeyInclusive in the Range as the datatype and UID follow the value in the keys
        // hence we need to compute the min possibly value that would be inclusive
        this.boundingFiRangeStringBuilder.setLength(0);
        if (lowerInclusive) {
            this.boundingFiRangeStringBuilder.append(fieldValue);
        } else {
            this.boundingFiRangeStringBuilder.append(fieldValue).append(ONE_BYTE);
        }
        this.boundingFiRangeStringBuilder.append(NULL_BYTE);
        startKey = new Key(rowId, fiName, new Text(boundingFiRangeStringBuilder.toString()));
        
        // we cannot simply use endKeyInclusive in the Range as the datatype and UID follow the value in the keys
        // hence we need to compute the max possibly value that would be inclusive
        this.boundingFiRangeStringBuilder.setLength(0);
        if (upperInclusive) {
            this.boundingFiRangeStringBuilder.append(upperBound).append(ONE_BYTE);
        } else {
            String upperString = upperBound.toString();
            this.boundingFiRangeStringBuilder.append(upperString.substring(0, upperString.length() - 1));
            this.boundingFiRangeStringBuilder.append(upperString.charAt(upperString.length() - 1) - 1);
            this.boundingFiRangeStringBuilder.append(Constants.MAX_UNICODE_STRING);
        }
        endKey = new Key(rowId, fiName, new Text(boundingFiRangeStringBuilder.toString()));
        if (isNegated()) {
            Key startFi = new Key(rowId, fiName);
            Key endFi = new Key(rowId, new Text(fiName.toString() + '\0'));
            List<Range> rangeList = new ArrayList<>(new RangeSplitter(new Range(startFi, true, startKey, true), getMaxRangeSplit() / 2));
            rangeList.addAll(new RangeSplitter(new Range(endKey, true, endFi, true), getMaxRangeSplit() / 2));
            return rangeList;
        } else {
            return new RangeSplitter(new Range(startKey, true, endKey, true), getMaxRangeSplit());
        }
    }
    
    // -------------------------------------------------------------------------
    // ------------- Other stuff
    
    /**
     * Does this key match our range. NOTE: This must be thread safe NOTE: The caller takes care of the negation
     * 
     * @param k
     * @return true (if a field index row)
     */
    @Override
    protected boolean matches(Key k) throws IOException {
        boolean matches = false;
        // test that we are in the range
        String colq = k.getColumnQualifier().toString();
        
        // search backwards for the null bytes to expose the value in value\0datatype\0UID
        int index = colq.lastIndexOf('\0');
        index = colq.lastIndexOf('\0', index - 1);
        Text value = new Text(colq.substring(0, index));
        
        if ((lowerInclusive ? (value.compareTo(getFieldValue()) >= 0) : (value.compareTo(getFieldValue()) > 0))
                        && (upperInclusive ? (value.compareTo(upperBound) <= 0) : (value.compareTo(upperBound) < 0))) {
            matches = true;
        }
        return matches;
    }
    
}
