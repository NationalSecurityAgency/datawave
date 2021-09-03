package datawave.microservice.query.remote.event.listener;

import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.remote.QueryRequestHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.bus.ConditionalOnBusEnabled;
import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.cloud.bus.event.RemoteQueryRequestEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConditionalOnBusEnabled
// TODO: Figure out how to do this correctly
// @ConditionalOnBean(type = "QueryRequestHandler")
public class QueryRemoteRequestEventListener implements ApplicationListener<RemoteQueryRequestEvent> {
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final List<QueryRequestHandler> queryRequestHandlers;
    private final ServiceMatcher serviceMatcher;
    
    public QueryRemoteRequestEventListener(List<QueryRequestHandler> queryRequestHandlers, ServiceMatcher serviceMatcher) {
        this.queryRequestHandlers = queryRequestHandlers;
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
        
        // process the event using each query request handler.
        // By default, for parallelStreams java uses threads equal to the number of cores.
        // if we need more than that, we can specify our own ForkJoinPool.
        queryRequestHandlers.parallelStream().forEach(h -> handleRequest(h, event.getRequest()));
    }
    
    private void handleRequest(QueryRequestHandler queryRequestHandler, QueryRequest queryRequest) {
        try {
            queryRequestHandler.handleRemoteRequest(queryRequest);
        } catch (Exception e) {
            log.error("Failed to handle query request with handler: " + queryRequestHandler.getClass().getName(), e);
        }
    }
}
