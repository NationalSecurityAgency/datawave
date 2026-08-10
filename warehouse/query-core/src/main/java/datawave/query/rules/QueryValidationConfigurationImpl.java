package datawave.query.rules;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.microservice.query.Query;

/**
 * Provides a baseline implementation of {@link QueryValidationConfiguration}.
 */
public class QueryValidationConfigurationImpl implements QueryValidationConfiguration {

    protected Query querySettings;

    protected GenericQueryConfiguration queryConfiguration;

    /**
     * {@inheritDoc}
     */
    @Override
    public Query getQuerySettings() {
        return querySettings;
    }

    /**
     * Sets the query settings to be validated.
     *
     * @param query
     *            the query settings
     */
    public void setQuerySettings(Query query) {
        this.querySettings = query;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericQueryConfiguration getQueryConfiguration() {
        return queryConfiguration;
    }

    /**
     * Sets the query configuration for the query
     *
     * @param queryConfiguration
     *            the query configuration
     */
    public void setQueryConfiguration(GenericQueryConfiguration queryConfiguration) {
        this.queryConfiguration = queryConfiguration;
    }
}
