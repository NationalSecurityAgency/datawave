package org.springframework.cloud.bus.event;

import datawave.microservice.query.storage.QueryTaskNotification;

public class RemoteQueryTaskNotificationEvent extends RemoteApplicationEvent {
    
    private final QueryTaskNotification queryTaskNotification;
    
    public RemoteQueryTaskNotificationEvent() {
        // this constructor is only for serialization/deserialization
        queryTaskNotification = null;
    }
    
    public RemoteQueryTaskNotificationEvent(Object source, String originService, QueryTaskNotification queryTaskNotification) {
        this(source, originService, null, queryTaskNotification);
    }
    
    public RemoteQueryTaskNotificationEvent(Object source, String originService, String destinationService, QueryTaskNotification queryTaskNotification) {
        super(source, originService, DEFAULT_DESTINATION_FACTORY.getDestination(destinationService));
        this.queryTaskNotification = queryTaskNotification;
    }
    
    public QueryTaskNotification getNotification() {
        return queryTaskNotification;
    }
}
