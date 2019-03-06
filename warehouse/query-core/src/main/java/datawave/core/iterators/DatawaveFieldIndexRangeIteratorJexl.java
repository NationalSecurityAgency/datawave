package datawave.core.iterators;

import datawave.data.type.DiscreteIndexType;
import datawave.query.Constants;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
    
    public static class Builder<B extends Builder<B>> extends DatawaveFieldIndexCachingIteratorJexl.Builder<B> {
        protected Text upperBound = null;
        protected boolean upperInclusive = true;
        protected boolean lowerInclusive = true;
        
        public B withLowerBound(Text lowerBound) {
            return super.withFieldValue(lowerBound);
        }
        
        public B withLowerBound(String lowerBound) {
            return this.withLowerBound(new Text(lowerBound));
        }
        
        public B withUpperBound(Text upperBound) {
            this.upperBound = upperBound;
            return self();
        }
        
        public B withUpperBound(String upperBound) {
            return this.withUpperBound(new Text(upperBound));
        }
        
        public B upperInclusive(boolean upperInclusive) {
            this.upperInclusive = upperInclusive;
            return self();
        }
        
        public B lowerInclusive(boolean lowerInclusive) {
            this.lowerInclusive = lowerInclusive;
            return self();
        }
        
        public DatawaveFieldIndexRangeIteratorJexl build() {
            return new DatawaveFieldIndexRangeIteratorJexl(this);
        }
    }
    
    public static Builder<?> builder() {
        return new Builder();
    }
    
    protected DatawaveFieldIndexRangeIteratorJexl(Builder builder) {
        super(builder);
        this.upperBound = builder.upperBound;
        this.upperInclusive = builder.upperInclusive;
        this.lowerInclusive = builder.lowerInclusive;
    }
    
    protected Text upperBound = null;
    protected boolean upperInclusive = true;
    protected boolean lowerInclusive = true;
    
    // -------------------------------------------------------------------------
    // ------------- Constructors
    public DatawaveFieldIndexRangeIteratorJexl() {
        super();
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
        Key startKey;
        Key endKey;
        // construct new range
        
        // we cannot simply use startKeyInclusive in the Range as the datatype and UID follow the value in the keys
        // hence we need to compute the min possibly value that would be inclusive
        this.boundingFiRangeStringBuilder.setLength(0);
        if (lowerInclusive) {
            this.boundingFiRangeStringBuilder.append(fieldValue);
        } else {
            // in case of composite ranges, use the discrete index type to get the inclusive bound
            if (compositeSeeker != null && compositeSeeker.getFieldToDiscreteIndexType().get(fiName.toString()) != null) {
                DiscreteIndexType discreteIndexType = compositeSeeker.getFieldToDiscreteIndexType().get(fiName.toString());
                this.boundingFiRangeStringBuilder.append(discreteIndexType.incrementIndex(fieldValue.toString()));
            } else {
                this.boundingFiRangeStringBuilder.append(fieldValue).append(ONE_BYTE);
            }
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
            // in case of composite ranges, use the discrete index type to get the inclusive bound
            if (compositeSeeker != null && compositeSeeker.getFieldToDiscreteIndexType().get(fiName.toString()) != null) {
                DiscreteIndexType discreteIndexType = compositeSeeker.getFieldToDiscreteIndexType().get(fiName.toString());
                this.boundingFiRangeStringBuilder.append(discreteIndexType.decrementIndex(upperString));
            } else {
                this.boundingFiRangeStringBuilder.append(upperString.substring(0, upperString.length() - 1));
                this.boundingFiRangeStringBuilder.append((char) (upperString.charAt(upperString.length() - 1) - 1));
            }
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
    
    protected Range buildCompositeSafeFiRange(Text rowId, Text fiName, Text fieldValue) {
        Key startKey = new Key(rowId, fiName, new Text(fieldValue));
        Key endKey = new Key(rowId, fiName, new Text(upperBound));
        return new Range(startKey, lowerInclusive, endKey, upperInclusive);
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
