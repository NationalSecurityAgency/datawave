package datawave.query.common.grouping;

import java.util.Set;

import org.apache.accumulo.core.security.ColumnVisibility;

import datawave.query.attributes.Attribute;

/**
 * Abstract implementation of {@link Aggregator}
 *
 * @param <AGGREGATE>
 *            the aggregation result type
 */
public abstract class AbstractAggregator<AGGREGATE> implements Aggregator<AGGREGATE> {

    /**
     * The name of the field being aggregated.
     */
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
    public abstract void merge(Aggregator<?> other);

}
