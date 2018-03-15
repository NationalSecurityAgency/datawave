package datawave.core.iterators;

import com.google.common.base.Predicate;
import datawave.core.iterators.querylock.QueryLock;
import datawave.query.Constants;
import datawave.query.iterator.filter.composite.CompositePredicateFilter;
import datawave.query.predicate.Filter;
import datawave.query.predicate.TimeFilter;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 
 * An iterator for the Datawave shard table, it searches FieldIndex keys and returns Event keys (its topKey must be an Event key).
 *
 * This version will take in an arbitrary range, and matching function and will return a sorted set of UIDs matching the supplied function.
 * 
 * FieldIndex keys: fi\0{fieldName}:{fieldValue}\0datatype\0uid
 * 
 * Event key: CF, {datatype}\0{UID}
 * 
 */
public class DatawaveFieldIndexFilterIteratorJexl extends DatawaveFieldIndexRangeIteratorJexl {
    private Filter filter;
    
    // -------------------------------------------------------------------------
    // ------------- Constructors
    public DatawaveFieldIndexFilterIteratorJexl() {
        super();
    }
    
    @SuppressWarnings("hiding")
    public DatawaveFieldIndexFilterIteratorJexl(Text fieldName, Filter filter, Text lowerBound, boolean lowerInclusive, Text upperBound,
                    boolean upperInclusive, TimeFilter timeFilter, Predicate<Key> datatypeFilter, long scanThreshold, long scanTimeout, int bufferSize,
                    int maxRangeSplit, int maxOpenFiles, FileSystem fs, Path uniqueDir, QueryLock queryLock, boolean allowDirReuse) {
        this(fieldName, filter, lowerBound, lowerInclusive, upperBound, upperInclusive, timeFilter, datatypeFilter, false, scanThreshold, scanTimeout,
                        bufferSize, maxRangeSplit, maxOpenFiles, fs, uniqueDir, queryLock, allowDirReuse);
    }
    
    @SuppressWarnings("hiding")
    public DatawaveFieldIndexFilterIteratorJexl(Text fieldName, Filter filter, Text lowerBound, boolean lowerInclusive, Text upperBound,
                    boolean upperInclusive, TimeFilter timeFilter, Predicate<Key> datatypeFilter, boolean neg, long scanThreshold, long scanTimeout,
                    int bufferSize, int maxRangeSplit, int maxOpenFiles, FileSystem fs, Path uniqueDir, QueryLock queryLock, boolean allowDirReuse) {
        this(fieldName, filter, lowerBound, lowerInclusive, upperBound, upperInclusive, timeFilter, datatypeFilter, neg, scanThreshold, scanTimeout,
                        bufferSize, maxRangeSplit, maxOpenFiles, fs, uniqueDir, queryLock, allowDirReuse, DEFAULT_RETURN_KEY_TYPE, true, null);
    }
    
    @SuppressWarnings("hiding")
    public DatawaveFieldIndexFilterIteratorJexl(Text fieldName, Filter filter, Text lowerBound, boolean lowerInclusive, Text upperBound,
                    boolean upperInclusive, TimeFilter timeFilter, Predicate<Key> datatypeFilter, boolean neg, long scanThreshold, long scanTimeout,
                    int bufferSize, int maxRangeSplit, int maxOpenFiles, FileSystem fs, Path uniqueDir, QueryLock queryLock, boolean allowDirReuse,
                    PartialKey returnKeyType, boolean sortedUIDs, Map<String,Map<String,CompositePredicateFilter>> compositePredicateFilters) {
        super(fieldName, lowerBound, lowerInclusive, upperBound, upperInclusive, timeFilter, datatypeFilter, neg, scanThreshold, scanTimeout, bufferSize,
                        maxRangeSplit, maxOpenFiles, fs, uniqueDir, queryLock, allowDirReuse, returnKeyType, sortedUIDs, compositePredicateFilters);
        this.filter = filter;
    }
    
    public DatawaveFieldIndexFilterIteratorJexl(DatawaveFieldIndexFilterIteratorJexl other, IteratorEnvironment env) {
        super(other, env);
        this.filter = other.filter;
    }
    
    // -------------------------------------------------------------------------
    // ------------- Overrides
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new DatawaveFieldIndexFilterIteratorJexl(this, env);
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DatawaveFieldIndexFilterIteratorJexl{fName=").append(getFieldName()).append(", filter=").append(filter).append(", lowerBound=")
                        .append(getFieldValue()).append(", lowerInclusive=").append(lowerInclusive).append(", upperBound=").append(upperBound)
                        .append(", upperInclusive=").append(upperInclusive).append(", negated=").append(isNegated()).append("}");
        
        return builder.toString();
    }
    
    /**
     * Unlike the super class's buildBoundingFiRanges, we want the same bounding range even if we are negated. negation in this case only refers to the supplied
     * filter.
     * 
     * @param rowId
     * @param fiName
     * @param fieldValue
     * @return the bounding ranges
     */
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
        return new RangeSplitter(new Range(startKey, true, endKey, true), getMaxRangeSplit());
    }
    
    // -------------------------------------------------------------------------
    // ------------- Other stuff
    
    /**
     * Does this key match our range and filter. NOTE: This must be thread safe NOTE: The caller takes care of the negation
     *
     * @param k
     * @return true (if a field index row)
     */
    @Override
    protected boolean matches(Key k) throws IOException {
        boolean matches = super.matches(k) && filter.keep(k);
        return matches;
    }
    
}
