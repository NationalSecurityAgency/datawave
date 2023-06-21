package datawave.query.composite;

import com.google.common.collect.Multimap;
import datawave.data.type.DiscreteIndexType;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The CompositeSeeker can be used within an Accumulo iterator in order to determing whether or not the current key is within the bounds of the composite range.
 * If one of the component values is out of range, this class can be used to determine the next valid composite range that we should seek to.
 */
public abstract class CompositeSeeker {
    protected Map<String,DiscreteIndexType<?>> fieldToDiscreteIndexType;

    CompositeSeeker(Map<String,DiscreteIndexType<?>> fieldToDiscreteIndexType) {
        this.fieldToDiscreteIndexType = fieldToDiscreteIndexType;
    }

    abstract public boolean isKeyInRange(Key currentKey, Range currentRange, String separator);

    abstract public Key nextSeekKey(List<String> fields, Key currentKey, Range currentRange, String separator);

    boolean isInRange(List<String> values, List<String> startValues, boolean isStartInclusive, List<String> endValues, boolean isEndInclusive) {
        for (int i = values.size(); i >= 0; i--) {
            String value = (i < values.size()) ? values.get(i) : null;
            String start = (i < startValues.size()) ? startValues.get(i) : null;
            String end = (i < endValues.size()) ? endValues.get(i) : null;

            boolean isStartValueInclusive = (i != startValues.size() - 1) || isStartInclusive;
            boolean isEndValueInclusive = (i != endValues.size() - 1) || isEndInclusive;

            // if start and end are equal, and one side is exclusive while the other is inclusive, just mark both as inclusive for our purposes
            if (start != null && end != null && isStartValueInclusive != isEndValueInclusive && start.equals(end)) {
                isStartValueInclusive = true;
                isEndValueInclusive = true;
            }

            if (value != null) {
                // only use exclusive comparison for the last value, all others are inclusive
                if (start != null && !isStartValid(value, start, isStartValueInclusive))
                    return false;

                // only use exclusive comparison for the last value, all others are inclusive
                if (end != null && !isEndValid(value, end, isEndValueInclusive))
                    return false;
            }
        }
        return true;
    }

    private boolean isStartValid(String startValue, String startBound, boolean isInclusive) {
        if (isInclusive)
            return startValue.compareTo(startBound) >= 0;
        else
            return startValue.compareTo(startBound) > 0;
    }

    private boolean isEndValid(String endValue, String endBound, boolean isInclusive) {
        if (isInclusive)
            return endValue.compareTo(endBound) <= 0;
        else
            return endValue.compareTo(endBound) < 0;
    }

    String nextLowerBound(List<String> fields, List<String> values, String separator, List<String> startValues, boolean isStartInclusive,
                    List<String> endValues, boolean isEndInclusive) {
        String[] newValues = new String[fields.size()];

        boolean carryOver = false;
        for (int i = fields.size() - 1; i >= 0; i--) {
            DiscreteIndexType discreteIndexType = fieldToDiscreteIndexType.get(fields.get(i));
            String value = (i < values.size()) ? values.get(i) : null;
            String start = (i < startValues.size()) ? startValues.get(i) : null;
            String end = (i < endValues.size()) ? endValues.get(i) : null;

            boolean isStartValueInclusive = (i != startValues.size() - 1) || isStartInclusive;
            boolean isEndValueInclusive = (i != endValues.size() - 1) || isEndInclusive;

            // if start and end are equal, and one side is exclusive while the other is inclusive, just mark both as inclusive for our purposes
            if (start != null && end != null && isStartValueInclusive != isEndValueInclusive && start.equals(end)) {
                isStartValueInclusive = true;
                isEndValueInclusive = true;
            }

            if (value != null) {
                // if it's not fixed length, check to see if we are in range
                if (discreteIndexType == null) {
                    // value precedes start value. need to seek forward.
                    if (start != null && !isStartValid(value, start, isStartValueInclusive)) {
                        newValues[i] = start;

                        // subsequent values set to start
                        for (int j = i + 1; j < startValues.size(); j++)
                            newValues[j] = startValues.get(j);
                    }
                    // value exceeds end value. need to seek forward, and carry over.
                    else if (end != null && !isEndValid(value, end, isEndValueInclusive)) {
                        newValues[i] = start;
                        carryOver = true;

                        // subsequent values set to start
                        for (int j = i + 1; j < startValues.size(); j++)
                            newValues[j] = startValues.get(j);
                    }
                    // value is in range.
                    else {
                        newValues[i] = values.get(i);
                    }
                }
                // if it's fixed length, determine whether or not we need to increment
                else {
                    // carry over means we need to increase our value
                    if (carryOver) {
                        // value precedes start value. just seek forward and ignore previous carry over.
                        if (start != null && !isStartValid(value, start, isStartValueInclusive)) {
                            newValues[i] = start;
                            carryOver = false;

                            // subsequent values set to start
                            for (int j = i + 1; j < startValues.size(); j++)
                                newValues[j] = startValues.get(j);
                        }
                        // value is at or exceeds end value. need to seek forward, and maintain carry over.
                        else if (end != null && !isEndValid(value, end, false)) {
                            newValues[i] = start;
                            carryOver = true;

                            // subsequent values set to start
                            for (int j = i + 1; j < startValues.size(); j++)
                                newValues[j] = startValues.get(j);
                        }
                        // value is in range. just increment, and finish carry over
                        else {
                            newValues[i] = discreteIndexType.incrementIndex(values.get(i));
                            carryOver = false;
                        }
                    } else {
                        // value precedes start value. need to seek forward.
                        if (start != null && !isStartValid(value, start, isStartValueInclusive)) {
                            newValues[i] = start;

                            // subsequent values set to start
                            for (int j = i + 1; j < startValues.size(); j++)
                                newValues[j] = startValues.get(j);
                        }
                        // value exceeds end value. need to seek forward, and carry over.
                        else if (end != null && !isEndValid(value, end, isEndValueInclusive)) {
                            newValues[i] = start;
                            carryOver = true;

                            // subsequent values set to start
                            for (int j = i + 1; j < startValues.size(); j++)
                                newValues[j] = startValues.get(j);
                        }
                        // value is in range.
                        else {
                            newValues[i] = values.get(i);
                        }
                    }
                }
            }
        }

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < newValues.length; i++) {
            if (newValues[i] != null)
                if (i > 0)
                    builder.append(separator).append(newValues[i]);
                else
                    builder.append(newValues[i]);
            else
                break;
        }

        return builder.toString();
    }

    public Map<String,DiscreteIndexType<?>> getFieldToDiscreteIndexType() {
        return fieldToDiscreteIndexType;
    }

    /**
     * This version of the CompositeSeeker is intended to be used when scanning keys in the shard index.
     */
    public static class ShardIndexCompositeSeeker extends CompositeSeeker {
        private List<String> fields;
        private String separator;

        public ShardIndexCompositeSeeker(List<String> fields, String separator, Map<String,DiscreteIndexType<?>> fieldToDiscreteIndexType) {
            super(fieldToDiscreteIndexType);
            this.fields = fields;
            this.separator = separator;
        }

        public boolean isKeyInRange(Key currentKey, Range currentRange) {
            return isKeyInRange(currentKey, currentRange, separator);
        }

        @Override
        public boolean isKeyInRange(Key currentKey, Range currentRange, String separator) {
            List<String> values = Arrays.asList(currentKey.getRow().toString().split(separator));
            List<String> startValues = Arrays.asList(currentRange.getStartKey().getRow().toString().split(separator));
            List<String> endValues = Arrays.asList(currentRange.getEndKey().getRow().toString().split(separator));
            return isInRange(values, startValues, currentRange.isStartKeyInclusive(), endValues, currentRange.isEndKeyInclusive());
        }

        public Key nextSeekKey(Key currentKey, Range currentRange) {
            return nextSeekKey(fields, currentKey, currentRange, separator);
        }

        @Override
        public Key nextSeekKey(List<String> fields, Key currentKey, Range currentRange, String separator) {
            Key startKey = currentRange.getStartKey();
            Key endKey = currentRange.getEndKey();

            List<String> values = Arrays.asList(currentKey.getRow().toString().split(separator));
            List<String> startValues = Arrays.asList(startKey.getRow().toString().split(separator));
            List<String> endValues = Arrays.asList(endKey.getRow().toString().split(separator));

            String nextLowerBound = nextLowerBound(fields, values, separator, startValues, currentRange.isStartKeyInclusive(), endValues,
                            currentRange.isEndKeyInclusive());

            Key newStartKey = new Key(new Text(nextLowerBound), startKey.getColumnFamily(), startKey.getColumnQualifier(), startKey.getColumnVisibility(), 0L);

            // return a new seek key only if it falls within the current range
            if (currentRange.contains(newStartKey))
                return newStartKey;

            return startKey;
        }
    }

    /**
     * This version of the CompositeSeeker is intended to be used when scanning keys in the field index.
     */
    public static class FieldIndexCompositeSeeker extends CompositeSeeker {
        public FieldIndexCompositeSeeker(Multimap<String,?> fieldDatatypes) {
            super(CompositeUtils.getFieldToDiscreteIndexTypeMap(fieldDatatypes));
        }

        @Override
        public boolean isKeyInRange(Key currentKey, Range currentRange, String separator) {
            List<String> values = Arrays.asList(currentKey.getColumnQualifier().toString().split("\0")[0].split(separator));
            List<String> startValues = Arrays.asList(currentRange.getStartKey().getColumnQualifier().toString().split("\0")[0].split(separator));
            List<String> endValues = Arrays.asList(currentRange.getEndKey().getColumnQualifier().toString().split("\0")[0].split(separator));
            return isInRange(values, startValues, currentRange.isStartKeyInclusive(), endValues, currentRange.isEndKeyInclusive());
        }

        @Override
        public Key nextSeekKey(List<String> fields, Key currentKey, Range currentRange, String separator) {
            Key startKey = currentRange.getStartKey();
            Key endKey = currentRange.getEndKey();

            String currentValue = currentKey.getColumnQualifier().toString().split("\0")[0];

            String startColQual = startKey.getColumnQualifier().toString();
            String lowerBound = startColQual.split("\0")[0];

            String endColQual = endKey.getColumnQualifier().toString();
            String upperBound = endColQual.split("\0")[0];

            List<String> values = Arrays.asList(currentValue.split(separator));
            List<String> startValues = Arrays.asList(lowerBound.split(separator));
            List<String> endValues = Arrays.asList(upperBound.split(separator));

            String nextLowerBound = nextLowerBound(fields, values, separator, startValues, currentRange.isStartKeyInclusive(), endValues,
                            currentRange.isEndKeyInclusive());

            // build a new range only if the new lower bound exceeds the current value without exceeding the upper bound of the range
            if (nextLowerBound.compareTo(currentValue) > 0 && nextLowerBound.compareTo(upperBound) <= 0) {
                String newColQual = nextLowerBound + "\0";
                return new Key(currentKey.getRow(), currentKey.getColumnFamily(), new Text(newColQual), startKey.getColumnVisibility(), 0L);
            }

            return startKey;
        }
    }
}
