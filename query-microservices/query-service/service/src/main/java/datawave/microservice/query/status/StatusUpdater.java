package datawave.microservice.query.status;

import datawave.microservice.query.storage.QueryStatus;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

public interface StatusUpdater {
    void apply(QueryStatus queryStatus) throws QueryException;
    
    default void onLockFailed() throws QueryException {
        throw new QueryException(DatawaveErrorCode.QUERY_LOCKED_ERROR, "Unable to acquire lock on query");
    }
}
