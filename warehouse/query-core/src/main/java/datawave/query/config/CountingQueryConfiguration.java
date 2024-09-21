package datawave.query.config;

import java.util.Collection;
import java.util.Objects;

import org.apache.accumulo.core.security.ColumnVisibility;

import datawave.core.query.configuration.QueryData;
import datawave.microservice.query.Query;
import datawave.query.tables.CountingShardQueryLogic;

public class CountingQueryConfiguration extends ShardQueryConfiguration {

    private long resultCount;
    private ColumnVisibility resultVis;

    public CountingQueryConfiguration() {}

    public CountingQueryConfiguration(CountingQueryConfiguration other) {
        super(other);

        setResultCount(other.getResultCount());
        setResultVis(other.getResultVis());
    }

    public CountingQueryConfiguration(CountingShardQueryLogic logic, Query query) {
        this(logic.getConfig());
        setQuery(query);
    }

    public CountingQueryConfiguration(CountingQueryConfiguration other, Collection<QueryData> queries) {
        super(other, queries);
        setResultCount(other.getResultCount());
        setResultVis(other.getResultVis());
    }

    /**
     * Factory method that instantiates a fresh CountingQueryConfiguration
     *
     * @return - a clean CountingQueryConfiguration
     */
    public static CountingQueryConfiguration create() {
        return new CountingQueryConfiguration();
    }

    /**
     * Factory method that returns a deep copy of the provided CountingQueryConfiguration
     *
     * @param other
     *            - another instance of a CountingQueryConfiguration
     * @return - copy of provided CountingQueryConfiguration
     */
    public static CountingQueryConfiguration create(CountingQueryConfiguration other) {
        return new CountingQueryConfiguration(other);
    }

    /**
     * Factory method that creates a CountingQueryConfiguration deep copy from a CountingShardQueryLogic
     *
     * @param shardQueryLogic
     *            - a configured CountingShardQueryLogic
     * @return - a CountingQueryConfiguration
     */
    public static CountingQueryConfiguration create(CountingShardQueryLogic shardQueryLogic) {
        CountingQueryConfiguration config = create(shardQueryLogic.getConfig());
        return config;
    }

    /**
     * Factory method that creates a CountingQueryConfiguration from a CountingShardQueryLogic and a Query
     *
     * @param shardQueryLogic
     *            - a configured CountingShardQueryLogic
     * @param query
     *            - a configured Query object
     * @return - a CountingQueryConfiguration
     */
    public static CountingQueryConfiguration create(CountingShardQueryLogic shardQueryLogic, Query query) {
        CountingQueryConfiguration config = create(shardQueryLogic);
        config.setQuery(query);
        return config;
    }

    public void setResultCount(long value) {
        this.resultCount = value;
    }

    public long getResultCount() {
        return this.resultCount;
    }

    public ColumnVisibility getResultVis() {
        return resultVis;
    }

    public void setResultVis(ColumnVisibility resultVis) {
        this.resultVis = resultVis;
    }

    @Override
    public CountingQueryConfiguration checkpoint() {
        // Create a new config that only contains what is needed to execute the ranges
        return new CountingQueryConfiguration(this, getQueries());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        CountingQueryConfiguration that = (CountingQueryConfiguration) o;
        return Objects.equals(resultCount, that.resultCount) && Objects.equals(resultVis, that.resultVis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resultCount, resultVis);
    }

}
