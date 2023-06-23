package datawave.core.iterators;

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
import java.util.SortedSet;

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
        protected SortedSet<Range> subRanges = null;

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

        public B withSubRanges(SortedSet<Range> subRanges) {
            this.subRanges = subRanges;
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
        this.subRanges = builder.subRanges;
    }

    protected Text upperBound = null;
    protected boolean upperInclusive = true;
    protected boolean lowerInclusive = true;
    protected SortedSet<Range> subRanges = null;

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
        builder.append("DatawaveFieldIndexRangeIteratorJexl (").append(queryId).append(") fName=").append(getFieldName()).append(", lowerBound=")
                        .append(getFieldValue()).append(", lowerInclusive=").append(lowerInclusive).append(", upperBound=").append(upperBound)
                        .append(", upperInclusive=").append(upperInclusive).append(", negated=").append(isNegated()).append("}");
        return builder.toString();
    }

    @Override
    protected List<Range> buildBoundingFiRanges(Text rowId, Text fiName, Text fieldValue) {
        if (ANY_FINAME.equals(fiName)) {
            Key startKey = new Key(rowId, FI_START);
            Key endKey = new Key(rowId, FI_END);
            return new RangeSplitter(new Range(startKey, true, endKey, false), getMaxRangeSplit());
        } else if (subRanges != null && !subRanges.isEmpty()) {
            List<Range> ranges = new ArrayList<>();

            // Note: The IndexRangeIteratorBuilder hard codes 'negated' to false, so unless that changes, this logic will never be executed.
            if (isNegated()) {
                Key startFi = new Key(rowId, fiName);
                Key endFi = new Key(rowId, new Text(fiName.toString() + '\0'));

                Key startKey = startFi;
                for (Range subRange : subRanges) {
                    Range range = new Range(startKey, true,
                                    createUpperBoundKey(rowId, fiName, subRange.getStartKey().getRow(), !subRange.isStartKeyInclusive()), true);
                    startKey = createLowerBoundKey(rowId, fiName, subRange.getEndKey().getRow(), !subRange.isEndKeyInclusive());
                    ranges.add(range);
                }

                // add the final range
                ranges.add(new Range(startKey, true, endFi, true));
            } else {
                for (Range subRange : subRanges) {
                    Key startKey = createLowerBoundKey(rowId, fiName, subRange.getStartKey().getRow(), subRange.isStartKeyInclusive());
                    Key endKey = createUpperBoundKey(rowId, fiName, subRange.getEndKey().getRow(), subRange.isEndKeyInclusive());
                    ranges.add(new Range(startKey, true, endKey, true));
                }
            }

            return ranges;
        } else {
            Key startKey = createLowerBoundKey(rowId, fiName, fieldValue, lowerInclusive);
            Key endKey = createUpperBoundKey(rowId, fiName, upperBound, upperInclusive);

            // Note: The IndexRangeIteratorBuilder hard codes 'negated' to false, so unless that changes, this logic will never be executed.
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
    }

    private Key createLowerBoundKey(Text rowId, Text fiName, Text lowerBound, boolean isLowerInclusive) {
        // we cannot simply use startKeyInclusive in the Range as the datatype and UID follow the value in the keys
        // hence we need to compute the min possibly value that would be inclusive
        this.boundingFiRangeStringBuilder.setLength(0);
        if (isLowerInclusive) {
            this.boundingFiRangeStringBuilder.append(lowerBound);
        } else {
            this.boundingFiRangeStringBuilder.append(lowerBound).append(ONE_BYTE);
        }
        this.boundingFiRangeStringBuilder.append(NULL_BYTE);
        return new Key(rowId, fiName, new Text(boundingFiRangeStringBuilder.toString()));
    }

    private Key createUpperBoundKey(Text rowId, Text fiName, Text upperBound, boolean isUpperInclusive) {
        // we cannot simply use endKeyInclusive in the Range as the datatype and UID follow the value in the keys
        // hence we need to compute the max possibly value that would be inclusive
        this.boundingFiRangeStringBuilder.setLength(0);
        if (isUpperInclusive) {
            this.boundingFiRangeStringBuilder.append(upperBound).append(ONE_BYTE);
        } else {
            String upperString = upperBound.toString();
            this.boundingFiRangeStringBuilder.append(upperString.substring(0, upperString.length() - 1));
            this.boundingFiRangeStringBuilder.append((char) (upperString.charAt(upperString.length() - 1) - 1));
            this.boundingFiRangeStringBuilder.append(Constants.MAX_UNICODE_STRING);
        }
        return new Key(rowId, fiName, new Text(boundingFiRangeStringBuilder.toString()));
    }

    protected Range buildCompositeSafeFiRange(Text rowId, Text fiName, Text fieldValue) {
        if (subRanges != null && !subRanges.isEmpty()) {
            return currentFiRange;
        } else {
            Key startKey = new Key(rowId, fiName, new Text(fieldValue));
            Key endKey = new Key(rowId, fiName, new Text(upperBound));
            return new Range(startKey, lowerInclusive, endKey, upperInclusive);
        }
    }

    // -------------------------------------------------------------------------
    // ------------- Other stuff

    /**
     * Does this key match our range. NOTE: This must be thread safe NOTE: The caller takes care of the negation
     *
     * @param k
     *            a key
     * @return true (if a field index row)
     * @throws IOException
     *             for issues with read/write
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

        if (subRanges == null) {
            if ((lowerInclusive ? (value.compareTo(getFieldValue()) >= 0) : (value.compareTo(getFieldValue()) > 0))
                            && (upperInclusive ? (value.compareTo(upperBound) <= 0) : (value.compareTo(upperBound) < 0))) {
                matches = true;
            }
        } else {
            // find the first range that contains the key
            matches = (subRanges.stream().filter(subRange -> subRange.contains(new Key(value))).findFirst().orElse(null) != null);
        }
        return matches;
    }

}
