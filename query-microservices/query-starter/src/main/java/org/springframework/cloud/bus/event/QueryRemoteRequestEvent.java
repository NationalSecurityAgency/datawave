package org.springframework.cloud.bus.event;

import datawave.microservice.query.remote.QueryRequest;

public class QueryRemoteRequestEvent extends RemoteApplicationEvent {
    
    private final QueryRequest queryRequest;
    
    @SuppressWarnings("unused")
    public QueryRemoteRequestEvent() {
        // this constructor is only for serialization/deserialization
        queryRequest = null;
    }
    
    public QueryRemoteRequestEvent(Object source, String originService, QueryRequest queryRequest) {
        this(source, originService, null, queryRequest);
    }
    
    public QueryRemoteRequestEvent(Object source, String originService, String destinationService, QueryRequest queryRequest) {
        super(source, originService, DEFAULT_DESTINATION_FACTORY.getDestination(destinationService));
        this.queryRequest = queryRequest;
    }
    
    public QueryRequest getRequest() {
        return queryRequest;
    }
}
