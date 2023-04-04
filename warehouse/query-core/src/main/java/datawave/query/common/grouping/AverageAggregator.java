package datawave.query.common.grouping;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Numeric;
import datawave.query.attributes.TypeAttribute;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Calculates the average value of aggregated field values. This is limited to fields that have {@link Numeric} values.
 */
public class AverageAggregator extends AbstractAggregator<BigDecimal> {
    
    private static final MathContext MATH_CONTEXT = new MathContext(10, RoundingMode.HALF_UP);
    
    private BigDecimal numerator;
    private BigDecimal divisor;
    private BigDecimal average;
    private final Set<ColumnVisibility> columnVisibilities;
    
    public static AverageAggregator of(String field, TypeAttribute<BigDecimal> numerator, TypeAttribute<BigDecimal> divisor) {
        return new AverageAggregator(field, numerator.getType().getDelegate(), divisor.getType().getDelegate(), numerator.getColumnVisibility());
    }
    
    public AverageAggregator(String field) {
        super(field);
        this.columnVisibilities = new HashSet<>();
    }
    
    private AverageAggregator(String field, BigDecimal numerator, BigDecimal divisor, ColumnVisibility columnVisibility) {
        this(field);
        this.numerator = numerator;
        this.divisor = divisor;
        this.average = numerator.divide(divisor, MATH_CONTEXT);
        if (columnVisibility != null) {
            this.columnVisibilities.add(columnVisibility);
        }
    }
    
    /**
     * Returns {@link AggregateOperation#AVERAGE}.
     * 
     * @return {@link AggregateOperation#AVERAGE}
     */
    @Override
    public AggregateOperation getOperation() {
        return AggregateOperation.AVERAGE;
    }
    
    /**
     * Returns an unmodifiable set of all distinct column visibilities for each attribute aggregated into this aggregator. Possibly empty, but never null.
     * 
     * @return a set of the column visibilities
     */
    @Override
    public Set<ColumnVisibility> getColumnVisibilities() {
        return Collections.unmodifiableSet(columnVisibilities);
    }
    
    /**
     * Return the average value seen for the field.
     * 
     * @return the average value, or null if no values have been aggregated yet
     */
    @Override
    public BigDecimal getAggregation() {
        return average;
    }
    
    /**
     * Return the current sum for the field values.
     * 
     * @return the sum
     */
    public BigDecimal getNumerator() {
        return numerator;
    }
    
    /**
     * Return the current count for the field.
     * 
     * @return the count
     */
    public BigDecimal getDivisor() {
        return divisor;
    }
    
    /**
     * Adds the value into the current sum and increments the total count by one. The average will be recalculated the next time {@link #getAggregation()} is
     * called.
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
            throw new IllegalArgumentException("Unable to calculate an average with non-numerical value '" + value.getData() + "'", e);
        }
        if (numerator == null) {
            numerator = number;
            divisor = BigDecimal.ONE;
        } else {
            numerator = numerator.add(number);
            divisor = divisor.add(BigDecimal.ONE);
        }
        average = numerator.divide(divisor, MATH_CONTEXT);
        columnVisibilities.add(value.getColumnVisibility());
    }
    
    @Override
    public void merge(Aggregator<?> other) {
        if (other instanceof AverageAggregator) {
            AverageAggregator aggregator = (AverageAggregator) other;
            this.numerator = numerator.add(aggregator.numerator);
            this.divisor = divisor.add(aggregator.divisor);
            this.average = this.numerator.divide(this.divisor, MATH_CONTEXT);
            this.columnVisibilities.addAll(aggregator.columnVisibilities);
        } else {
            throw new IllegalArgumentException("Cannot merge instance of " + other.getClass().getName());
        }
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("field", field).append("average", average).append("numerator", numerator).append("divisor", divisor)
                        .append("columnVisibilities", columnVisibilities).toString();
    }
}
