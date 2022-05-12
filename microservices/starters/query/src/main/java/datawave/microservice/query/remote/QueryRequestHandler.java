package datawave.microservice.query.remote;

// implement this if you only want to handle external requests
public interface QueryRequestHandler {
    void handleRemoteRequest(QueryRequest queryRequest, String originService, String destinationService);
    
    // implement this if you want to handle external and self requests
    interface QuerySelfRequestHandler extends QueryRequestHandler {}
}
