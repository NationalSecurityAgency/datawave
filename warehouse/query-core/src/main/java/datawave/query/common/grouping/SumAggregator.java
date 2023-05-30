package datawave.query.common.grouping;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Numeric;
import datawave.query.attributes.TypeAttribute;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Calculates the sum of aggregated field values. This is limited to fields for which their values can be parsed as {@link BigDecimal} instances.
 */
public class SumAggregator extends AbstractAggregator<BigDecimal> {
    
    /**
     * The current sum.
     */
    private BigDecimal sum;
    
    /**
     * The column visibilities of all attributes aggregated.
     */
    private final Set<ColumnVisibility> columnVisibilities;
    
    public static SumAggregator of(String field, TypeAttribute<BigDecimal> attribute) {
        BigDecimal sum = attribute.getType().getDelegate();
        return new SumAggregator(field, sum, attribute.getColumnVisibility());
    }
    
    public SumAggregator(String field) {
        super(field);
        this.columnVisibilities = new HashSet<>();
    }
    
    private SumAggregator(String field, BigDecimal sum, ColumnVisibility visibility) {
        this(field);
        this.sum = sum;
        if (visibility != null) {
            this.columnVisibilities.add(visibility);
        }
    }
    
    /**
     * Returns {@link AggregateOperation#SUM}.
     *
     * @return {@link AggregateOperation#SUM}
     */
    @Override
    public AggregateOperation getOperation() {
        return AggregateOperation.SUM;
    }
    
    @Override
    public Set<ColumnVisibility> getColumnVisibilities() {
        return Collections.unmodifiableSet(columnVisibilities);
    }
    
    /**
     * Return the sum of all values seen for the field.
     *
     * @return the sum, or null if no values were aggregated
     */
    @Override
    public BigDecimal getAggregation() {
        return sum;
    }
    
    @Override
    public boolean hasAggregation() {
        return sum != null;
    }
    
    /**
     * Adds the value into the current sum.
     *
     * @param value
     *            the value to aggregate
     * @throws IllegalArgumentException
     *             if the given value is not a {@link Numeric} type
     */
    @Override
    public void aggregate(Attribute<?> value) {
        BigDecimal number;
        try {
            number = new BigDecimal(value.getData().toString());
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to calculate a sum with non-numerical value '" + value.getData() + "'", e);
        }
        if (sum == null) {
            sum = number;
        } else {
            sum = sum.add(number);
        }
        columnVisibilities.add(value.getColumnVisibility());
    }
    
    @Override
    public void merge(Aggregator<?> other) {
        if (other instanceof SumAggregator) {
            SumAggregator aggregator = (SumAggregator) other;
            this.sum = this.sum.add(aggregator.sum);
            this.columnVisibilities.addAll(aggregator.columnVisibilities);
        } else {
            throw new IllegalArgumentException("Cannot merge instance of " + other.getAggregation());
        }
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("field", field).append("sum", sum).append("columnVisibilities", columnVisibilities).toString();
    }
}
