package datawave.query.common.grouping;

import datawave.query.attributes.Attribute;
import org.apache.accumulo.core.security.ColumnVisibility;

import java.util.Collection;
import java.util.Set;

/**
 * Provides the methods by which aggregates can be calculated for fields when grouped by other fields.
 * 
 * @param <AGGREGATE>
 *            the aggregate result type
 */
public interface Aggregator<AGGREGATE> {
    
    /**
     * Return the aggregate operation being performed.
     * 
     * @return the aggregate operation
     */
    AggregateOperation getOperation();
    
    /**
     * Return the field being aggregated.
     * 
     * @return the field
     */
    String getField();
    
    /**
     * Return the distinct column visibilities that collectively represent the column visibility of the aggregated result. This is required to establish a final
     * column visibility when adding the aggregated information to a document.
     * 
     * @return the column visibilities
     */
    Set<ColumnVisibility> getColumnVisibilities();
    
    /**
     * Return the aggregation result.
     *
     * @return the aggregation
     */
    AGGREGATE getAggregation();
    
    /**
     * Aggregate the given value into this aggregator.
     *
     * @param value
     *            the value to aggregate
     */
    void aggregate(Attribute<?> value);
    
    /**
     * Aggregate each value in the given collection into this aggregator.
     *
     * @param values
     *            the values to aggregate
     */
    void aggregateAll(Collection<Attribute<?>> values);
    
    void merge(Aggregator<?> otherAggregator);
}
