package datawave.core.iterators;

import java.io.IOException;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import datawave.query.Constants;
import datawave.query.predicate.Filter;

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

    public static class Builder<B extends Builder<B>> extends DatawaveFieldIndexRangeIteratorJexl.Builder<B> {
        private Filter filter;

        public B withFilter(Filter filter) {
            this.filter = filter;
            return self();
        }

        public DatawaveFieldIndexFilterIteratorJexl build() {
            return new DatawaveFieldIndexFilterIteratorJexl(this);
        }

    }

    public static Builder<?> builder() {
        return new Builder();
    }

    protected DatawaveFieldIndexFilterIteratorJexl(Builder builder) {
        super(builder);
        this.filter = builder.filter;
    }

    // -------------------------------------------------------------------------
    // ------------- Constructors
    public DatawaveFieldIndexFilterIteratorJexl() {
        super();
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
        builder.append("DatawaveFieldIndexFilterIteratorJexl (").append(queryId).append(") fName=").append(getFieldName()).append(", filter=").append(filter)
                        .append(", lowerBound=").append(getFieldValue()).append(", lowerInclusive=").append(lowerInclusive).append(", upperBound=")
                        .append(upperBound).append(", upperInclusive=").append(upperInclusive).append(", negated=").append(isNegated()).append("}");

        return builder.toString();
    }

    /**
     * Unlike the super class's buildBoundingFiRanges, we want the same bounding range even if we are negated. negation in this case only refers to the supplied
     * filter.
     *
     * @param rowId
     *            the row id
     * @param fiName
     *            field index name
     * @param fieldValue
     *            the field value
     * @return the bounding ranges
     */
    @Override
    protected List<Range> buildBoundingFiRanges(Text rowId, Text fiName, Text fieldValue) {
        Key startKey = null;
        Key endKey = null;
        // construct new range
        if (ANY_FINAME.equals(fiName)) {
            startKey = new Key(rowId, FI_START);
            endKey = new Key(rowId, FI_END);
            return new RangeSplitter(new Range(startKey, true, endKey, false), getMaxRangeSplit());
        }
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
            this.boundingFiRangeStringBuilder.append((char) (upperString.charAt(upperString.length() - 1) - 1));
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
     *            a key
     * @return true (if a field index row)
     * @throws IOException
     *             for issues with read/write
     */
    @Override
    protected boolean matches(Key k) throws IOException {
        return super.matches(k) && filter.keep(k);
    }

}
