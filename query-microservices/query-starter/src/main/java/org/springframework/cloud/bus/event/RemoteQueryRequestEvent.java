package org.springframework.cloud.bus.event;

import datawave.microservice.query.remote.QueryRequest;

public class RemoteQueryRequestEvent extends RemoteApplicationEvent {
    
    private final QueryRequest request;
    
    @SuppressWarnings("unused")
    public RemoteQueryRequestEvent() {
        // this constructor is only for serialization/deserialization
        request = null;
    }
    
    public RemoteQueryRequestEvent(Object source, String originService, QueryRequest request) {
        this(source, originService, null, request);
    }
    
    public RemoteQueryRequestEvent(Object source, String originService, String destinationService, QueryRequest request) {
        super(source, originService, DEFAULT_DESTINATION_FACTORY.getDestination(destinationService));
        this.request = request;
    }
    
    public QueryRequest getRequest() {
        return request;
    }
}
