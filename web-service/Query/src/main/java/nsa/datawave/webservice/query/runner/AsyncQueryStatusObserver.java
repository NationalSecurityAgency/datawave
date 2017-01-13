package nsa.datawave.webservice.query.runner;

import nsa.datawave.webservice.query.exception.QueryException;
import nsa.datawave.webservice.result.BaseQueryResponse;
import nsa.datawave.webservice.result.GenericResponse;

/**
 *
 */
public interface AsyncQueryStatusObserver {
    void queryCreated(GenericResponse<String> createQueryResponse);
    
    void queryResultsAvailable(BaseQueryResponse results);
    
    void queryCreateException(QueryException ex);
    
    void queryException(QueryException ex);
    
    void queryFinished(String queryId);
}
