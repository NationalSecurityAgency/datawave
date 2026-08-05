package datawave.webservice.query.limit;

import java.util.Collection;
import java.util.Set;

/**
 * A service responsible for determining if any concurrent query limits will be exceeded for a user, system, or query logic when a new query is submitted.
 */
public interface QueryLimitService {
    
    /**
     * Check if the user is allowed to create another query based on the given query logic on the current system.
     *
     * @param userDn
     *            the user DN
     * @param system
     *            the query system
     * @param queryLogic
     *            the query logic
     * @return the response
     * @throws Exception
     *             if an exception occurs
     */
    QueryLimiterResponse checkForLimits(String userDn, String system, String queryLogic) throws Exception;
    
    /**
     * Mark the given query as active.
     *
     * @param queryId
     *            the query ID
     * @param userDn
     *            the DN of the user who submitted the query
     * @param system
     *            the system the query was submitted from
     * @param queryLogic
     *            the query logic the query is based on
     * @throws Exception
     *             if an error occurs
     */
    void markActive(String queryId, String userDn, String system, String queryLogic) throws Exception;
    
    /**
     * Fetch the set of IDs for queries considered to be actively running.
     *
     * @return the set of IDs for active queries
     */
    Set<String> getActiveQueries();
    
    /**
     * Mark the given queries as inactive.
     *
     * @param queryIds
     *            the query IDs
     */
    void markInactive(Collection<String> queryIds) ;
    
    /**
     * Mark the given query as inactive.
     *
     * @param queryId
     *            the query ID
     */
    void markInactive(String queryId) ;
    
    /**
     * Whether the service is currently enforcing limits.
     * @return true if the service is enforcing limits, or false otherwise
     */
    boolean isEnforcingLimits();
}
