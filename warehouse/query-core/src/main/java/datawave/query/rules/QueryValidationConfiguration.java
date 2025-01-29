package datawave.query.rules;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.microservice.query.Query;

public interface QueryValidationConfiguration {

    /**
     * Returns the {@link Query} settings to be validated.
     *
     * @return the query settings
     */
    Query getQuerySettings();

    /**
     * Returns the {@link GenericQueryConfiguration} for the query.
     *
     * @return the query configuration
     */
    GenericQueryConfiguration getQueryConfiguration();

}
