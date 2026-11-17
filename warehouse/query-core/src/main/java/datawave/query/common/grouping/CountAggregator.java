package datawave.query.common.grouping;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.access.AccessExpression;
import org.apache.commons.lang.builder.ToStringBuilder;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.TypeAttribute;

/**
 * Determines the total count of aggregated field values. This supports values of all {@link Attribute} types.
 */
public class CountAggregator extends AbstractAggregator<Long> {

    /**
     * The total number of times the field was seen.
     */
    private long count;

    /**
     * The access expressions of all attributes aggregated.
     */
    private final Set<AccessExpression> accessExpressions;

    public static CountAggregator of(String field, TypeAttribute<BigDecimal> attribute) {
        return new CountAggregator(field, attribute.getType().getDelegate().longValue(), attribute.getAccessExpression());
    }

    public CountAggregator(String field) {
        super(field);
        this.accessExpressions = new HashSet<>();
    }

    private CountAggregator(String field, long count, AccessExpression expression) {
        this(field);
        this.count = count;
        if (expression != null) {
            accessExpressions.add(expression);
        }
    }

    /**
     * Returns {@link AggregateOperation#COUNT}.
     *
     * @return {@link AggregateOperation#COUNT}
     */
    @Override
    public AggregateOperation getOperation() {
        return AggregateOperation.COUNT;
    }

    @Override
    public Set<AccessExpression> getAccessExpressions() {
        return Collections.unmodifiableSet(accessExpressions);
    }

    /**
     * Return the total number of times a field was seen.
     *
     * @return the total count
     */
    @Override
    public Long getAggregation() {
        return count;
    }

    @Override
    public boolean hasAggregation() {
        return count > 0L;
    }

    /**
     * Increments the current count by 1.
     *
     * @param value
     *            the value to aggregate
     */
    @Override
    public void aggregate(Attribute<?> value) {
        count++;
        this.accessExpressions.add(value.getAccessExpression());
    }

    @Override
    public void merge(Aggregator<?> other) {
        if (other instanceof CountAggregator) {
            CountAggregator aggregator = (CountAggregator) other;
            this.count += aggregator.count;
            this.accessExpressions.addAll(aggregator.accessExpressions);
        } else {
            throw new IllegalArgumentException("Cannot merge instance of " + other.getClass().getName());
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("field", field).append("count", count).append("accessExpressions", accessExpressions).toString();
    }
}
