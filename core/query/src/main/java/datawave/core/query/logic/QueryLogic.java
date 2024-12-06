package datawave.core.query.logic;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.iterators.TransformIterator;

import datawave.audit.SelectorExtractor;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.authorization.UserOperations;
import datawave.validation.ParameterValidator;
import datawave.webservice.common.audit.Auditor.AuditType;
import datawave.webservice.common.connection.AccumuloClientConfiguration;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseResponse;
import datawave.webservice.result.QueryValidationResponse;

public interface QueryLogic<T> extends Iterable<T>, Cloneable, ParameterValidator {

    /**
     * A mechanism to get the normalized query without actually setting up the query. This can be called with having to call initialize.
     * <p>
     * The default implementation is to return the query string as the normalized query
     *
     * @param client
     *            - Accumulo connector to use for this query
     * @param settings
     *            - query settings (query, begin date, end date, etc.)
     * @param runtimeQueryAuthorizations
     *            - authorizations that have been calculated for this query based on the caller and server.
     * @param expandFields
     *            - should unfielded terms be expanded
     * @param expandValues
     *            - should regex/ranges be expanded into discrete values
     * @return the normalized query
     * @throws Exception
     *             if there are issues
     */
    String getPlan(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields, boolean expandValues)
                    throws Exception;

    /**
     * Implementations create a configuration using the connection, settings, and runtimeQueryAuthorizations.
     *
     * @param client
     *            - Accumulo client to use for this query
     * @param settings
     *            - query settings (query, begin date, end date, etc.)
     * @param runtimeQueryAuthorizations
     *            - authorizations that have been calculated for this query based on the caller and server.
     * @return a configuration
     * @throws Exception
     *             if there are issues
     */
    GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception;

    /**
     * @param settings
     *            - query settings (query, begin date, end date, etc.)
     * @return list of selectors used in the Query
     */
    List<String> getSelectors(Query settings);

    SelectorExtractor getSelectorExtractor();

    /**
     * Implementations use the configuration to run their query. It is expected that initialize has already been called.
     *
     * @param configuration
     *            Encapsulates all information needed to run a query (whether the query is a BatchScanner, a MapReduce job, etc)
     * @throws Exception
     *             if there are issues
     */
    void setupQuery(GenericQueryConfiguration configuration) throws Exception;

    /**
     * @return a copy of this instance
     * @throws CloneNotSupportedException
     *             if the clone is not supported
     */
    Object clone() throws CloneNotSupportedException;

    /**
     * @return priority from AccumuloConnectionFactory
     */
    AccumuloConnectionFactory.Priority getConnectionPriority();

    /**
     * @param settings
     *            query settings
     * @return Transformer that will convert Key,Value to a Result object
     */
    QueryLogicTransformer getTransformer(Query settings);

    QueryLogicTransformer getEnrichedTransformer(Query settings);

    default ResultPostprocessor getResultPostprocessor(GenericQueryConfiguration config) {
        return new ResultPostprocessor.IdentityResultPostprocessor();
    }

    default String getResponseClass(Query query) throws QueryException {
        try {
            QueryLogicTransformer t = this.getEnrichedTransformer(query);
            BaseResponse refResponse = t.createResponse(new ResultsPage());
            return refResponse.getClass().getCanonicalName();
        } catch (RuntimeException e) {
            throw new QueryException(DatawaveErrorCode.QUERY_TRANSFORM_ERROR);
        }
    }

    /**
     * Allows for the customization of handling query results, e.g. allows for aggregation of query results before returning to the client.
     *
     * @param settings
     *            The query settings object
     * @return Return a TransformIterator for the QueryLogic implementation
     */
    TransformIterator getTransformIterator(Query settings);

    /**
     * Whether the query is a type that should be allowed to be run long (exceed the short circuit timeout)
     *
     * @return Return whether the query is a type that should be allowed to be run long (exceed the short circuit timeout)
     */
    boolean isLongRunningQuery();

    /**
     * release resources
     */
    void close();

    /**
     * @return the tableName
     */
    String getTableName();

    /**
     * @return max number of results to pass back to the caller
     */
    long getMaxResults();

    /**
     * @return max number of concurrent tasks to run for this query
     */
    int getMaxConcurrentTasks();

    /**
     * @return the results of getMaxWork
     */
    @Deprecated
    long getMaxRowsToScan();

    /**
     * @return max number of nexts + seeks performed by the underlying iterators in total
     */
    long getMaxWork();

    /**
     * @return max number of records to return in a page (max pagesize allowed)
     */
    int getMaxPageSize();

    /**
     * @return the number of bytes at which a page will be returned, even if pagesize has not been reached
     */
    long getPageByteTrigger();

    /**
     * Returns the base iterator priority.
     *
     * @return base iterator priority
     */
    int getBaseIteratorPriority();

    /**
     * @param tableName
     *            the name of the table
     */
    void setTableName(String tableName);

    /**
     * @param maxResults
     *            max number of results to pass back to the caller
     */
    void setMaxResults(long maxResults);

    /**
     * @param maxConcurrentTasks
     *            max number of concurrent tasks to run for this query
     */
    void setMaxConcurrentTasks(int maxConcurrentTasks);

    /**
     * @param maxRowsToScan
     *            This is now deprecated and setMaxWork should be used instead. This is equivalent to setMaxWork.
     */
    @Deprecated
    void setMaxRowsToScan(long maxRowsToScan);

    /**
     * @param maxWork
     *            max work which is normally calculated as the number of next + seek calls made by the underlying iterators
     */
    void setMaxWork(long maxWork);

    /**
     * @param maxPageSize
     *            max number of records in a page (max pagesize allowed)
     */
    void setMaxPageSize(int maxPageSize);

    /**
     * @param pageByteTrigger
     *            the number of bytes at which a page will be returned, even if pagesize has not been reached
     */
    void setPageByteTrigger(long pageByteTrigger);

    /**
     * Sets the base iterator priority
     *
     * @param priority
     *            base iterator priority
     */
    void setBaseIteratorPriority(final int priority);

    /**
     * @param logicName
     *            name of the query logic
     */
    void setLogicName(String logicName);

    /**
     * @return name of the query logic
     */
    String getLogicName();

    /**
     * @param logicDescription
     *            a brief description of this logic type
     */
    void setLogicDescription(String logicDescription);

    /**
     * @param query
     *            the query
     * @return the audit level for this logic
     */
    AuditType getAuditType(Query query);

    /**
     * @return the audit level for this logic for a specific query
     */
    AuditType getAuditType();

    /**
     * @param auditType
     *            the audit level for this logic
     */
    void setAuditType(AuditType auditType);

    /**
     * @return a brief description of this logic type
     */
    String getLogicDescription();

    /**
     * @return should query metrics be collected for this query logic
     */
    boolean getCollectQueryMetrics();

    /**
     * @param collectQueryMetrics
     *            whether query metrics be collected for this query logic
     */
    void setCollectQueryMetrics(boolean collectQueryMetrics);

    /**
     * List of parameters that can be used in the 'params' parameter to Query/create
     *
     * @return the supported parameters
     */
    Set<String> getOptionalQueryParameters();

    /**
     * @param connPoolName
     *            The name of the connection pool to set.
     */
    void setConnPoolName(String connPoolName);

    /**
     * @return the connPoolName
     */
    String getConnPoolName();

    /**
     * Check that the user has one of the required roles. userRoles may be null when there is no intent to control access to QueryLogic
     *
     * @param userRoles
     *            The user's roles
     * @return true/false
     */
    boolean canRunQuery(Collection<String> userRoles);

    void setRequiredRoles(Set<String> requiredRoles);

    Set<String> getRequiredRoles();

    MarkingFunctions getMarkingFunctions();

    void setMarkingFunctions(MarkingFunctions markingFunctions);

    ResponseObjectFactory getResponseObjectFactory();

    void setResponseObjectFactory(ResponseObjectFactory responseObjectFactory);

    /**
     * List of parameters that must be passed from the client for this query logic to work
     *
     * @return the required parameters
     */
    Set<String> getRequiredQueryParameters();

    /**
     *
     * @return set of example queries
     */
    Set<String> getExampleQueries();

    /**
     * Return the DNs authorized access to this query logic.
     *
     * @return the set of DNs authorized access to this query logic, possibly null or empty
     */
    Set<String> getAuthorizedDNs();

    /**
     * Set the DNs authorized access to this query logic.
     *
     * @param allowedDNs
     *            the DNs authorized access
     */
    void setAuthorizedDNs(Set<String> allowedDNs);

    /**
     * Return whether or not the provided collection of DNs contains at least oneDN that is authorized for access to this query logic. This will return true in
     * the following cases:
     * <ul>
     * <li>The set of authorized DNs for this query logic is null or empty.</li>
     * <li>The set of authorized DNs is not empty, and the provided collection contains a DN that is also found within the set of authorized DNs.</li>
     * </ul>
     *
     * @param dns
     *            the DNs to determine access rights for
     * @return true if the collection contains at least one DN that has access to this query logic, or false otherwise
     */
    default boolean containsDNWithAccess(Collection<String> dns) {
        Set<String> authorizedDNs = getAuthorizedDNs();
        return authorizedDNs == null || authorizedDNs.isEmpty() || (dns != null && dns.stream().anyMatch(authorizedDNs::contains));
    }

    /**
     * Set the map of DNs to query result limits. This should override the default limit returned by {@link #getMaxResults()} for any included DNs.
     *
     * @param dnResultLimits
     *            the map of DNs to query result limits
     */
    void setDnResultLimits(Map<String,Long> dnResultLimits);

    /**
     * Return the map of DNs to result limits.
     *
     * @return the map of DNs to query result limits
     */
    Map<String,Long> getDnResultLimits();

    /**
     * Set the map of DNs to query result limits. This should override the default limit returned by {@link #getMaxResults()} for any included DNs.
     *
     * @param systemFromResultLimits
     *            the map of system from values to query result limits
     */
    void setSystemFromResultLimits(Map<String,Long> systemFromResultLimits);

    /**
     * Return a map of System From values to results limits.
     *
     * @return the map of system from values to query result limits.
     */
    Map<String,Long> getSystemFromResultLimits();

    /**
     * Return the maximum number of results to include for the query based on criteria including the DN or systemFrom stored in the query setting object that is
     * provided. If limits are found for multiple criteria in the collection, the smallest value will be returned. If no limits are found for any criteria, the
     * value of {@link #getMaxResults()} will be returned. If both the DN and systemFrom rules match the request, the DN result will take precedence over the
     * systemFrom rules.
     *
     * @param settings
     *            the query settings used to determine the maximum number of results to include for the query. It's expected that this includes the list of all
     *            DNs for a user and any systemFrom parameter values.
     * @return the maximum number of results to include
     */
    default long getResultLimit(Query settings) {
        long maxResults = getMaxResults();

        Map<String,Long> systemFromLimits = getSystemFromResultLimits();
        String systemFromParam = settings.getSystemFrom();
        if (systemFromLimits != null && systemFromParam != null) {
            // this findParameter implementation never returns null, it will return a parameter with an empty string
            // as the value if the parameter is not present.
            maxResults = systemFromLimits.getOrDefault(systemFromParam, maxResults);
        }

        Map<String,Long> dnResultLimits = getDnResultLimits();
        Collection<String> dns = settings.getDnList();
        if (dnResultLimits != null && dns != null) {
            maxResults = dns.stream().filter(dnResultLimits::containsKey).map(dnResultLimits::get).min(Long::compareTo).orElse(maxResults);
        }

        return maxResults;
    }

    /**
     * inform the logic of when a new page of results processing starts. Logics may ignore this information, or pass it to interested transformers, but the
     * logics shouldn't store this property.
     *
     * @param pageProcessingStartTime
     *            the processing start time
     */
    void setPageProcessingStartTime(long pageProcessingStartTime);

    /**
     * Normally this simply returns null as the query logic simply uses the user within the local system. However sometimes we may have a query that goes off
     * system and hence needs to use a different set of auths when downgrading.
     *
     * @return A user operations interface implementation. Null if NA (i.e. the local principal is sufficient)
     */
    UserOperations getUserOperations();

    /**
     * This is to be used prior to requesting user operations for a logic that is not yet initialized. The main use case is for the FilteredQueryLogic to allow
     * it to filter this call as well. Most query logics will not implement this.
     *
     * @param settings
     *            query settings
     * @param userAuthorizations
     *            a set of user authorizations
     */
    default void preInitialize(Query settings, Set<Authorizations> userAuthorizations) {
        // noop
    }

    /**
     * Set a client configuration for scanner hints and consistency.
     *
     * @param config
     */
    void setClientConfig(AccumuloClientConfiguration config);

    /**
     * Get the client configuration
     *
     * @return client configuration
     */
    AccumuloClientConfiguration getClientConfig();

    ProxiedUserDetails getCurrentUser();

    void setCurrentUser(ProxiedUserDetails currentUser);

    ProxiedUserDetails getServerUser();

    void setServerUser(ProxiedUserDetails serverUser);

    /**
     * Validates the given query according to the validation criteria established for the query logic.
     *
     * @param client
     *            the Accumulo connector to use for this query
     * @param query
     *            the query settings (query, begin date, end date, etc.)
     * @param auths
     *            the authorizations that have been calculated for this query based on the caller and server.
     * @return a list of messages detailing any issues found for the query
     */
    default Object validateQuery(AccumuloClient client, Query query, Set<Authorizations> auths) throws Exception {
        throw new UnsupportedOperationException("Query validation not implemented");
    }

    /**
     * Return a transformer that will convert the result of {@link QueryLogic#validateQuery(AccumuloClient, Query, Set)} to a {@link QueryValidationResponse}.
     *
     * @return the transformer
     */
    default Transformer<Object,QueryValidationResponse> getQueryValidationResponseTransformer() {
        throw new UnsupportedOperationException("Query validation response transformer not implemented");
    }

}
