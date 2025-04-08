package datawave.core.query.logic.composite;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;

public class CompositeQueryConfiguration extends GenericQueryConfiguration implements Serializable {

    private Map<String,GenericQueryConfiguration> configs = new HashMap<>();

    // Specifies whether all queries must succeed initialization
    private boolean allMustInitialize = false;

    // Specifies whether queries are run sequentially. We stop after the first query that returns any results.
    private boolean shortCircuitExecution = false;

    private long resultsPollTimeout = 1000;

    private TimeUnit resultsPollTimeoutTimeUnit = TimeUnit.MILLISECONDS;

    public CompositeQueryConfiguration() {
        super();
        setQuery(new QueryImpl());
    }

    /**
     * Performs a deep copy of the provided CompositeQueryConfiguration into a new instance
     *
     * @param other
     *            - another CompositeQueryConfiguration instance
     */
    public CompositeQueryConfiguration(CompositeQueryConfiguration other) {

        // GenericQueryConfiguration copy first
        super(other);
    }

    /**
     * Factory method that instantiates an fresh CompositeQueryConfiguration
     *
     * @return - a clean CompositeQueryConfiguration
     */
    public static CompositeQueryConfiguration create() {
        return new CompositeQueryConfiguration();
    }

    /**
     * Factory method that returns a deep copy of the provided CompositeQueryConfiguration
     *
     * @param other
     *            - another instance of a CompositeQueryConfiguration
     * @return - copy of provided CompositeQueryConfiguration
     */
    public static CompositeQueryConfiguration create(CompositeQueryConfiguration other) {
        return new CompositeQueryConfiguration(other);
    }

    /**
     * Factory method that creates a CompositeQueryConfiguration deep copy from a CompositeQueryLogic
     *
     * @param compositeQueryLogic
     *            - a configured CompositeQueryLogic
     * @return - a CompositeQueryConfiguration
     */
    public static CompositeQueryConfiguration create(CompositeQueryLogic compositeQueryLogic) {

        CompositeQueryConfiguration config = create(compositeQueryLogic.getConfig());

        return config;
    }

    /**
     * Factory method that creates a CompositeQueryConfiguration from a CompositeQueryLogic and a Query
     *
     * @param compositeQueryLogic
     *            - a configured CompositeQueryLogic
     * @param query
     *            - a configured Query object
     * @return - a CompositeQueryConfiguration
     */
    public static CompositeQueryConfiguration create(CompositeQueryLogic compositeQueryLogic, Query query) {
        CompositeQueryConfiguration config = create(compositeQueryLogic);
        config.setQuery(query);
        return config;
    }

    public GenericQueryConfiguration getConfig(String logicName) {
        return configs != null ? configs.get(logicName) : null;
    }

    public Map<String,GenericQueryConfiguration> getConfigs() {
        return configs;
    }

    public void setConfigs(Map<String,GenericQueryConfiguration> configs) {
        this.configs = configs;
    }

    public boolean isAllMustInitialize() {
        return allMustInitialize;
    }

    public void setAllMustInitialize(boolean allMustInitialize) {
        this.allMustInitialize = allMustInitialize;
    }

    public boolean isShortCircuitExecution() {
        return shortCircuitExecution;
    }

    public void setShortCircuitExecution(boolean shortCircuitExecution) {
        this.shortCircuitExecution = shortCircuitExecution;
    }

    public long getResultsPollTimeout() {
        return resultsPollTimeout;
    }

    public void setResultsPollTimeout(long resultsPollTimeout) {
        this.resultsPollTimeout = resultsPollTimeout;
    }

    public TimeUnit getResultsPollTimeoutTimeUnit() {
        return resultsPollTimeoutTimeUnit;
    }

    public void setResultsPollTimeoutTimeUnit(TimeUnit resultsPollTimeoutTimeUnit) {
        this.resultsPollTimeoutTimeUnit = resultsPollTimeoutTimeUnit;
    }
}
