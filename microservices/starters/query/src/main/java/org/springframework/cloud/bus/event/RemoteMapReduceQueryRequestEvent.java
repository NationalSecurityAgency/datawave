package org.springframework.cloud.bus.event;

import datawave.microservice.query.mapreduce.remote.MapReduceQueryRequest;

public class RemoteMapReduceQueryRequestEvent extends RemoteApplicationEvent {
    
    private final MapReduceQueryRequest request;
    
    @SuppressWarnings("unused")
    public RemoteMapReduceQueryRequestEvent() {
        // this constructor is only for serialization/deserialization
        request = null;
    }
    
    public RemoteMapReduceQueryRequestEvent(Object source, String originService, MapReduceQueryRequest request) {
        this(source, originService, null, request);
    }
    
    public RemoteMapReduceQueryRequestEvent(Object source, String originService, String destinationService, MapReduceQueryRequest request) {
        super(source, originService, DEFAULT_DESTINATION_FACTORY.getDestination(destinationService));
        this.request = request;
    }
    
    public MapReduceQueryRequest getRequest() {
        return request;
    }
}
