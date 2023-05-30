package datawave.query.common.grouping;

import datawave.query.attributes.Attribute;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.Collections;
import java.util.Set;

/**
 * Determines the max of aggregated field values. This supports fields that have {@link datawave.query.attributes.Numeric} values,
 * {@link datawave.query.attributes.DateContent} values, and values that have {@link String} data types.
 */
public class MaxAggregator extends AbstractAggregator<Attribute<?>> {
    
    private Attribute<?> max;
    
    public static MaxAggregator of(String field, Attribute<?> max) {
        return new MaxAggregator(field, max);
    }
    
    public MaxAggregator(String field) {
        super(field);
    }
    
    private MaxAggregator(String field, Attribute<?> max) {
        this(field);
        this.max = max;
    }
    
    /**
     * Returns {@link AggregateOperation#MAX}.
     * 
     * @return {@link AggregateOperation#MAX}
     */
    @Override
    public AggregateOperation getOperation() {
        return AggregateOperation.MAX;
    }
    
    /**
     * Returns a singleton set containing the column visibility of the max attribute found. Possible empty, but never null.
     * 
     * @return a set containing the column visibility
     */
    @Override
    public Set<ColumnVisibility> getColumnVisibilities() {
        if (max != null) {
            return Collections.singleton(max.getColumnVisibility());
        }
        return Collections.emptySet();
    }
    
    /**
     * Return the attribute with the max value seen of all attributes aggregated into this aggregator.
     * 
     * @return the attribute, or null if no attributes have been aggregated yet
     */
    @Override
    public Attribute<?> getAggregation() {
        return this.max;
    }
    
    @Override
    public boolean hasAggregation() {
        return max != null;
    }
    
    /**
     * Compares the given value to the current max in this aggregator. If no max has been established yet, or if the given value is greater than the current
     * max, the given value will be retained as the new max. Otherwise, the current max will remain the same.
     * 
     * @param value
     *            the value to aggregate
     * @throws IllegalArgumentException
     *             if a value of a different {@link Attribute} than that of the current max is provided
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void aggregate(Attribute<?> value) {
        if (this.max == null) {
            this.max = value;
        } else {
            try {
                Comparable maxCopy = this.max.copy();
                int compare = maxCopy.compareTo(value.copy());
                if (compare < 0) {
                    this.max = value;
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to compare current max '" + this.max.getData() + "' to new value '" + value.getData() + "'", e);
            }
        }
    }
    
    @Override
    public void merge(Aggregator<?> other) {
        if (other instanceof MaxAggregator) {
            aggregate(((MaxAggregator) other).max);
        } else {
            throw new IllegalArgumentException("Cannot merge instance of " + other.getClass().getName());
        }
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("field", field).append("max", max).toString();
    }
}
