package datawave.query.validate;

import org.apache.commons.jexl3.parser.ParseException;

import java.util.List;
import java.util.Set;

public interface QueryValidator {
    
    /**
     * Returns the default configuration for the query validator.
     * @return the default configuration.
     */
    QueryValidatorConfiguration getConfig();
    
    /**
     * Sets the default configuration for the query validator.
     * @param config the configuration
     */
    void setConfig(QueryValidatorConfiguration config);
    
    /**
     * Returns a copy of the default configuration in its base state, without a configured query, query syntax,
     * {@link datawave.core.query.configuration.GenericQueryConfiguration}, or {@link datawave.query.util.MetadataHelper}.
     * @return a copy of the default configuration in a uninitialized state
     */
    default QueryValidatorConfiguration getConfigBaseCopy() {
        return getConfig().getBaseCopy();
    }
    
    /**
     * Returns the set of query syntaxes supported by this query validator.
     * @return
     */
    Set<String> getSupportedQuerySyntaxes();
    
    /**
     * Validates the query within the given configuration and returns a list of messages detailing any identified issues.
     * @param config the configuration
     * @return the details of any issues found within the query
     * @throws Exception if any exception occurrs
     */
    List<String> validate(QueryValidatorConfiguration config) throws Exception;
}
