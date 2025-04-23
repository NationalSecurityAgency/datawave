package datawave.microservice.query.remote.event.listener;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.bus.ConditionalOnBusEnabled;
import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.cloud.bus.event.RemoteQueryRequestEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import datawave.microservice.query.remote.QueryRequestHandler;

@Component
@ConditionalOnBusEnabled
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
        boolean isSelfRequest = serviceMatcher.isFromSelf(event);
        if (isSelfRequest) {
            log.debug("Received a self-request {}.", event);
        }
        
        // process the event using each query request handler.
        // By default, for parallelStreams java uses threads equal to the number of cores.
        // if we need more than that, we can specify our own ForkJoinPool.
        // @formatter:off
        queryRequestHandlers
                .stream()
                .filter(requestHandler -> shouldHandleRequest(requestHandler, isSelfRequest))
                .parallel()
                .forEach(h -> handleRequest(h, event));
        // @formatter:on
    }
    
    private boolean shouldHandleRequest(QueryRequestHandler handler, boolean isSelfRequest) {
        return !isSelfRequest || handler instanceof QueryRequestHandler.QuerySelfRequestHandler;
    }
    
    private void handleRequest(QueryRequestHandler queryRequestHandler, RemoteQueryRequestEvent queryRequestEvent) {
        try {
            queryRequestHandler.handleRemoteRequest(queryRequestEvent.getRequest(), queryRequestEvent.getOriginService(),
                            queryRequestEvent.getDestinationService());
        } catch (Exception e) {
            log.error("Failed to handle query request with handler: " + queryRequestHandler.getClass().getName(), e);
        }
    }
}
