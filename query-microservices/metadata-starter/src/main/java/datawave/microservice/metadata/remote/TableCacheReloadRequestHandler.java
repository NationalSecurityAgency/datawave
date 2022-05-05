package datawave.microservice.metadata.remote;

// implement this if you only want to handle external requests
public interface TableCacheReloadRequestHandler {
    void handleRemoteRequest(String tableName, String originService, String destinationService);
    
    // implement this if you want to handle external and self requests
    interface QuerySelfRequestHandler extends TableCacheReloadRequestHandler {}
}
