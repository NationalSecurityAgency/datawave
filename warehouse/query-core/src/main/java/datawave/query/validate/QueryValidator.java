package datawave.query.validate;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.query.util.MetadataHelper;

import java.util.List;
import java.util.Set;

public interface QueryValidator {
    
    /**
     * Returns the set of query syntaxes supported by this query validator.
     * @return
     */
    Set<String> getSupportedQuerySyntaxes();
    
    /**
     * Validates the given query and returns a list of messages detailing any identified issues.
     * @param query the query
     * @param querySyntax the query syntax
     * @param config the query config
     * @param metadataHelper the metadata helper
     * @return the details of any issues found within the query
     * @throws Exception if any exception occurs
     */
    List<String> evaluate(String query, String querySyntax, GenericQueryConfiguration config, MetadataHelper metadataHelper) throws Exception;
}
