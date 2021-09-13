package datawave.microservice.query.remote;

public interface QueryRequestHandler {
    void handleRemoteRequest(QueryRequest queryRequest, String originService, String destinationService);
}
