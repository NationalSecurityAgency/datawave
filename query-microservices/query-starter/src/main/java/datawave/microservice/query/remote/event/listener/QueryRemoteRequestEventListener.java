package datawave.microservice.query.remote.event.listener;

import datawave.microservice.query.remote.QueryRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.cloud.bus.ConditionalOnBusEnabled;
import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.cloud.bus.event.RemoteQueryRequestEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnBusEnabled
@ConditionalOnBean(QueryRequestHandler.class)
public class QueryRemoteRequestEventListener implements ApplicationListener<RemoteQueryRequestEvent> {
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final QueryRequestHandler queryRequestHandler;
    private final ServiceMatcher serviceMatcher;
    
    @Autowired
    public QueryRemoteRequestEventListener(QueryRequestHandler queryRequestHandler, ServiceMatcher serviceMatcher) {
        this.queryRequestHandler = queryRequestHandler;
        this.serviceMatcher = serviceMatcher;
    }
    
    @Override
    public void onApplicationEvent(RemoteQueryRequestEvent event) {
        // Ignore events that this service instance published, since we publish from a place
        // that takes the same action we do here, and we don't want to repeat the work.
        if (serviceMatcher.isFromSelf(event)) {
            log.debug("Dropping {} since it is from us.", event);
            return;
        }
        
        queryRequestHandler.handleRemoteRequest(event.getRequest());
    }
}
