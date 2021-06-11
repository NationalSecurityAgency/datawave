package datawave.microservice.query.storage.remote;

import datawave.microservice.query.storage.QueryTaskNotification;

public interface QueryTaskNotificationHandler {
    void handleQueryTaskNotification(QueryTaskNotification queryTaskNotification);
}
