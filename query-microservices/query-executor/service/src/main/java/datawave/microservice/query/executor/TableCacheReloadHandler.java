package datawave.microservice.query.executor;

import datawave.microservice.query.remote.TableCacheReloadRequestHandler;
import datawave.services.common.cache.AccumuloTableCache;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

@Service
public class TableCacheReloadHandler implements TableCacheReloadRequestHandler {
    private final Logger log = Logger.getLogger(TableCacheReloadHandler.class);
    private final AccumuloTableCache cache;
    
    public TableCacheReloadHandler(AccumuloTableCache cache) {
        this.cache = cache;
    }
    
    @Override
    public void handleRemoteRequest(String tableName, String originService, String destinationService) {
        log.debug("Handling remote reload request for " + tableName);
        cache.reloadTableCache(tableName);
    }
}
