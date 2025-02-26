package datawave.microservice.query.mapreduce.remote;

// implement this if you only want to handle external requests

public interface MapReduceQueryRequestHandler {
    void handleRemoteRequest(MapReduceQueryRequest queryRequest, String originService, String destinationService);
    
    // implement this if you want to handle external and self requests
    interface MapReduceQuerySelfRequestHandler extends MapReduceQueryRequestHandler {}
}
