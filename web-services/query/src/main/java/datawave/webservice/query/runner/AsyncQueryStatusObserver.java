package datawave.webservice.query.runner;

import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;

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
