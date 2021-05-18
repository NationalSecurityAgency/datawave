package datawave.microservice.common.storage.remote.event.listener;

import datawave.microservice.common.storage.remote.QueryTaskNotificationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.cloud.bus.ConditionalOnBusEnabled;
import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.cloud.bus.event.RemoteQueryTaskNotificationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnBusEnabled
@ConditionalOnBean(QueryTaskNotificationHandler.class)
public class QueryTaskNotificationEventListener implements ApplicationListener<RemoteQueryTaskNotificationEvent> {
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final QueryTaskNotificationHandler queryTaskNotificationHandler;
    private final ServiceMatcher serviceMatcher;
    
    @Autowired
    public QueryTaskNotificationEventListener(QueryTaskNotificationHandler queryTaskNotificationHandler, ServiceMatcher serviceMatcher) {
        this.queryTaskNotificationHandler = queryTaskNotificationHandler;
        this.serviceMatcher = serviceMatcher;
    }
    
    @Override
    public void onApplicationEvent(RemoteQueryTaskNotificationEvent event) {
        // Ignore events that this service instance published, since we publish from a place
        // that takes the same action we do here, and we don't want to repeat the work.
        if (serviceMatcher.isFromSelf(event)) {
            log.debug("Dropping {} since it is from us.", event);
            return;
        }
        
        queryTaskNotificationHandler.handleQueryTaskNotification(event.getNotification());
    }
}
