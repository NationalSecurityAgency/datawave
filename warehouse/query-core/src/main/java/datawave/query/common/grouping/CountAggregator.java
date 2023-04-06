package datawave.query.common.grouping;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.TypeAttribute;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Determines the total count of aggregated field values. This supports values of all {@link Attribute} types.
 */
public class CountAggregator extends AbstractAggregator<Integer> {
    
    /**
     * The total number of times the field was seen.
     */
    private int count;
    
    /**
     * The column visibilities of all attributes aggregated.
     */
    private final Set<ColumnVisibility> columnVisibilities;
    
    public static CountAggregator of(String field, TypeAttribute<BigDecimal> attribute) {
        return new CountAggregator(field, attribute.getType().getDelegate().intValue(), attribute.getColumnVisibility());
    }
    
    public CountAggregator(String field) {
        super(field);
        this.columnVisibilities = new HashSet<>();
    }
    
    private CountAggregator(String field, int count, ColumnVisibility visibility) {
        this(field);
        this.count = count;
        if (visibility != null) {
            columnVisibilities.add(visibility);
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
    public Set<ColumnVisibility> getColumnVisibilities() {
        return Collections.unmodifiableSet(columnVisibilities);
    }
    
    /**
     * Return the total number of times a field was seen.
     * 
     * @return the total count
     */
    @Override
    public Integer getAggregation() {
        return count;
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
        this.columnVisibilities.add(value.getColumnVisibility());
    }
    
    @Override
    public void merge(Aggregator<?> other) {
        if (other instanceof CountAggregator) {
            CountAggregator aggregator = (CountAggregator) other;
            this.count += aggregator.count;
            this.columnVisibilities.addAll(aggregator.columnVisibilities);
        } else {
            throw new IllegalArgumentException("Cannot merge instance of " + other.getClass().getName());
        }
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("field", field).append("count", count).append("columnVisibilities", columnVisibilities).toString();
    }
}
