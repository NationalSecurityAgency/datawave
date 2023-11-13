package datawave.query.common.grouping;

import java.util.Collections;
import java.util.Set;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.builder.ToStringBuilder;

import datawave.query.attributes.Attribute;

/**
 * Determines the min of aggregated field values. This supports fields that have {@link datawave.query.attributes.Numeric} values,
 * {@link datawave.query.attributes.DateContent} values, and values that have {@link String} data types.
 */
public class MinAggregator extends AbstractAggregator<Attribute<?>> {

    /**
     * The current min attribute.
     */
    private Attribute<?> min;

    public static MinAggregator of(String field, Attribute<?> min) {
        return new MinAggregator(field, min);
    }

    public MinAggregator(String field) {
        super(field);
    }

    private MinAggregator(String field, Attribute<?> min) {
        super(field);
        this.min = min;
    }

    /**
     * Returns {@link AggregateOperation#MIN}.
     *
     * @return {@link AggregateOperation#MIN}
     */
    @Override
    public AggregateOperation getOperation() {
        return AggregateOperation.MIN;
    }

    /**
     * Returns a singleton set containing the column visibility of the min attribute found. Possible empty, but never null.
     *
     * @return a set containing the column visibility
     */
    @Override
    public Set<ColumnVisibility> getColumnVisibilities() {
        if (min != null) {
            return Collections.singleton(min.getColumnVisibility());
        }
        return Collections.emptySet();
    }

    /**
     * Return the attribute with the min value seen of all attributes aggregated into this aggregator.
     *
     * @return the attribute, or null if no attributes have been aggregated yet
     */
    @Override
    public Attribute<?> getAggregation() {
        return min;
    }

    @Override
    public boolean hasAggregation() {
        return min != null;
    }

    /**
     * Compares the given value to the current min in this aggregator. If no min has been established yet, or if the given value is less than the current min,
     * the given value will be retained as the new min. Otherwise, the current min will remain the same.
     *
     * @param value
     *            the value to aggregate
     * @throws IllegalArgumentException
     *             if a value of a different {@link Attribute} than that of the current min is provided
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void aggregate(Attribute<?> value) {
        if (this.min == null) {
            this.min = value;
        } else {
            try {
                Comparable minCopy = this.min.copy();
                int compare = minCopy.compareTo(value.copy());
                if (compare > 0) {
                    this.min = value;
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to compare current min '" + this.min.getData() + "' to new value '" + value.getData() + "'", e);
            }
        }
    }

    @Override
    public void merge(Aggregator<?> other) {
        if (other instanceof MinAggregator) {
            aggregate(((MinAggregator) other).min);
        } else {
            throw new IllegalArgumentException("Cannot merge instance of " + other.getClass().getName());
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("field", field).append("min", min).toString();
    }
}
