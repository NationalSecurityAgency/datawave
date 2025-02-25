package datawave.microservice.query.cachedresults;

import datawave.security.authorization.ProxiedUserDetails;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;

public interface QueryService {
    // duplicate
    GenericResponse<String> duplicate(String queryId, ProxiedUserDetails currentUser) throws QueryException;
    
    // next
    BaseQueryResponse next(String queryId, ProxiedUserDetails currentUser) throws QueryException;
    
    // close
    VoidResponse close(String queryId, ProxiedUserDetails currentUser) throws QueryException;
    
    // cancel
    VoidResponse cancel(String queryId, ProxiedUserDetails currentUser) throws QueryException;
    
    // remove
    VoidResponse remove(String queryId, ProxiedUserDetails currentUser) throws QueryException;
}
