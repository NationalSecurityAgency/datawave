package datawave.core.query.logic;

import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.webservice.query.exception.QueryException;

import java.security.Principal;
import java.util.List;

public interface QueryLogicFactory {
    
    /**
     *
     * @param name
     *            name of query logic
     * @param principal
     *            the user principal
     * @return new instance of QueryLogic class
     * @throws IllegalArgumentException
     *             if query logic name does not exist
     * @throws QueryException
     *             if query not available for user's roles
     * @throws CloneNotSupportedException
     *             if the query logic object failed to clone
     */
    QueryLogic<?> getQueryLogic(String name, Principal principal) throws QueryException, IllegalArgumentException, CloneNotSupportedException;
    
    /**
     *
     * @param name
     *            name of query logic
     * @param currentUser
     *            the current user
     * @return new instance of QueryLogic class
     * @throws IllegalArgumentException
     *             if query logic name does not exist
     * @throws QueryException
     *             if query not available for user's roles
     * @throws CloneNotSupportedException
     *             if the query logic object failed to clone
     */
    QueryLogic<?> getQueryLogic(String name, ProxiedUserDetails currentUser) throws QueryException, IllegalArgumentException, CloneNotSupportedException;
    
    QueryLogic<?> getQueryLogic(String name) throws QueryException, IllegalArgumentException, CloneNotSupportedException;
    
    List<QueryLogic<?>> getQueryLogicList();
}
