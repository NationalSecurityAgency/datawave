package datawave.core.query.remote;

import java.net.URI;
import java.util.List;
import java.util.Map;

import datawave.security.authorization.ProxiedUserDetails;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;

/**
 * A remote query service is one that can pass calls off to another query service
 */
public interface RemoteQueryService {

    /**
     * Call the create on a remote query service
     *
     * @param queryLogicName
     *            a query logic name
     * @param queryParameters
     *            the query parameters
     * @param callerObject
     *            the caller
     * @return the generic response
     */
    GenericResponse<String> createQuery(String queryLogicName, Map<String,List<String>> queryParameters, ProxiedUserDetails callerObject) throws QueryException;

    /**
     * Set the class for the next response. The default is to use the event query response but to make this useful for other query services we need to be able
     * to override.
     *
     * @param nextQueryResponseClass
     */
    void setNextQueryResponseClass(Class<? extends BaseQueryResponse> nextQueryResponseClass);

    /**
     * Call next on a remote query service
     *
     * @param id
     *            the id
     * @param callerObject
     *            the caller
     * @return the base query response
     */
    BaseQueryResponse next(String id, ProxiedUserDetails callerObject) throws QueryException;

    /**
     * Call close on a remote query service
     *
     * @param id
     *            the id
     * @param callerObject
     *            the caller
     * @return the void response
     */
    VoidResponse close(String id, ProxiedUserDetails callerObject) throws QueryException;

    /**
     * Plan a query using a remote query service
     *
     * @param queryLogicName
     *            a query logic name
     * @param queryParameters
     *            the query parameters
     * @param callerObject
     *            the caller
     * @return the generic response
     */
    GenericResponse<String> planQuery(String queryLogicName, Map<String,List<String>> queryParameters, ProxiedUserDetails callerObject) throws QueryException;

    /**
     * Get the plan from a remote query service
     *
     * @param id
     *            the id
     * @param callerObject
     *            the caller
     * @return a generic response
     */
    GenericResponse<String> planQuery(String id, ProxiedUserDetails callerObject) throws QueryException;

    /**
     * Get the URI for the query metrics
     *
     * @param id
     *            the id
     * @return the query metrics uri
     */
    URI getQueryMetricsURI(String id);
}
