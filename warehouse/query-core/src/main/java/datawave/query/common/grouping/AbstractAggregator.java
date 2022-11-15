package datawave.query.common.grouping;

import datawave.query.attributes.Attribute;
import org.apache.accumulo.core.security.ColumnVisibility;

import java.util.Collection;
import java.util.Set;

/**
 * Abstract implementation of {@link Aggregator}
 * 
 * @param <AGGREGATE>
 *            the aggregation result type
 */
public abstract class AbstractAggregator<AGGREGATE> implements Aggregator<AGGREGATE> {
    
    protected final String field;
    
    protected AbstractAggregator(String field) {
        this.field = field;
    }
    
    @Override
    public abstract AggregateOperation getOperation();
    
    @Override
    public String getField() {
        return this.field;
    }
    
    @Override
    public abstract Set<ColumnVisibility> getColumnVisibilities();
    
    @Override
    public abstract AGGREGATE getAggregation();
    
    @Override
    public abstract void aggregate(Attribute<?> value);
    
    @Override
    public void aggregateAll(Collection<Attribute<?>> values) {
        values.forEach(this::aggregate);
    }
    
    @Override
    public abstract void merge(Aggregator<?> other);
    
}
