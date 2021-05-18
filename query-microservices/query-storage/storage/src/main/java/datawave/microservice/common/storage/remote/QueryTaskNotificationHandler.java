package datawave.microservice.common.storage.remote;

import datawave.microservice.common.storage.QueryTaskNotification;

public interface QueryTaskNotificationHandler {
    void handleQueryTaskNotification(QueryTaskNotification queryTaskNotification);
}
