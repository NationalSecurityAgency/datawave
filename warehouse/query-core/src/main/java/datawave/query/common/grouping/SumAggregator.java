package datawave.query.common.grouping;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.access.AccessExpression;
import org.apache.commons.lang.builder.ToStringBuilder;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Numeric;
import datawave.query.attributes.TypeAttribute;

/**
 * Calculates the sum of aggregated field values. This is limited to fields for which their values can be parsed as {@link BigDecimal} instances.
 */
public class SumAggregator extends AbstractAggregator<BigDecimal> {

    /**
     * The current sum.
     */
    private BigDecimal sum;

    /**
     * The access expressions of all attributes aggregated.
     */
    private final Set<AccessExpression> accessExpressions;

    public static SumAggregator of(String field, TypeAttribute<BigDecimal> attribute) {
        BigDecimal sum = attribute.getType().getDelegate();
        return new SumAggregator(field, sum, attribute.getAccessExpression());
    }

    public SumAggregator(String field) {
        super(field);
        this.accessExpressions = new HashSet<>();
    }

    private SumAggregator(String field, BigDecimal sum, AccessExpression expression) {
        this(field);
        this.sum = sum;
        if (expression != null) {
            this.accessExpressions.add(expression);
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
    public Set<AccessExpression> getAccessExpressions() {
        return Collections.unmodifiableSet(accessExpressions);
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
        accessExpressions.add(value.getAccessExpression());
    }

    @Override
    public void merge(Aggregator<?> other) {
        if (other instanceof SumAggregator) {
            SumAggregator aggregator = (SumAggregator) other;
            this.sum = this.sum.add(aggregator.sum);
            this.accessExpressions.addAll(aggregator.accessExpressions);
        } else {
            throw new IllegalArgumentException("Cannot merge instance of " + other.getAggregation());
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("field", field).append("sum", sum).append("accessExpressions", accessExpressions).toString();
    }
}
