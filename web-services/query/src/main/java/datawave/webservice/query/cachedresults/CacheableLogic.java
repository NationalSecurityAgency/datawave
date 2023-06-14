package datawave.webservice.query.cachedresults;

import java.util.List;

import datawave.webservice.query.exception.QueryException;

public interface CacheableLogic {

    List<CacheableQueryRow> writeToCache(Object o) throws QueryException;

    // CachedRowSet is passed pointing to the current row
    // This method must create the objects that will later be passed to createResponse
    List<Object> readFromCache(List<CacheableQueryRow> row);

}
