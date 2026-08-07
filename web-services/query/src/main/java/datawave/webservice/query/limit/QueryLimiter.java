package datawave.webservice.query.limit;

import java.util.Collection;
import java.util.Set;

/**
 * A service responsible for determining if any concurrent query limits are going to be exceeded for a user, system, or query logic when a new query is
 * submitted.
 */
public interface QueryLimiter {

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
     *             if an error occurs while checking limits
     */
    QueryLimiterResponse checkLimits(String userDn, String system, String queryLogic) throws Exception;

    /**
     * Mark the given query as active, counting it towards query limits.
     *
     * @param queryId
     *            the query ID
     * @param userDn
     *            the userDN of the user who submitted the query
     * @param system
     *            the system from
     * @param queryLogic
     *            the queryLogic the query is based on
     * @throws Exception
     *             if an error occurs
     */
    void markActive(String queryId, String userDn, String system, String queryLogic) throws Exception;

    /**
     * Mark the given query as inactive, and stop counting it towards query limits.
     * @param queryId the query ID
     */
    void markInactive(String queryId);

    /**
     * Mark each of the given queries as inactive, and stop counting them towards query limits.
     * @param queryIds the query IDs
     */
    void markInactive(Collection<String> queryIds);

    /**
     * Get the set of IDs for queries considered active and counted towards query limits.
     * @return the query IDs
     */
    Set<String> getActiveQueries();
}
