package datawave.core.query.cachedresults;

import datawave.webservice.query.cachedresults.CacheableQueryRow;
import datawave.webservice.query.exception.QueryException;

public interface CacheableLogic {

    CacheableQueryRow writeToCache(Object o) throws QueryException;

    // CachedRowSet is passed pointing to the current row
    // This method must create the objects that will later be passed to createResponse
    Object readFromCache(CacheableQueryRow row);
}
