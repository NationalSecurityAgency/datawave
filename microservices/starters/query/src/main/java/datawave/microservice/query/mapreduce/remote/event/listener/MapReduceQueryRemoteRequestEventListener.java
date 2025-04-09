package datawave.microservice.query.mapreduce.remote.event.listener;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.bus.ConditionalOnBusEnabled;
import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.cloud.bus.event.RemoteMapReduceQueryRequestEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import datawave.microservice.query.mapreduce.remote.MapReduceQueryRequestHandler;
import datawave.microservice.query.remote.QueryRequestHandler;

@Component
@ConditionalOnBusEnabled
public class MapReduceQueryRemoteRequestEventListener implements ApplicationListener<RemoteMapReduceQueryRequestEvent> {
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final List<MapReduceQueryRequestHandler> mapReduceQueryRequestHandlers;
    private final ServiceMatcher serviceMatcher;
    
    public MapReduceQueryRemoteRequestEventListener(List<MapReduceQueryRequestHandler> mapReduceQueryRequestHandlers, ServiceMatcher serviceMatcher) {
        this.mapReduceQueryRequestHandlers = mapReduceQueryRequestHandlers;
        this.serviceMatcher = serviceMatcher;
    }
    
    @Override
    public void onApplicationEvent(RemoteMapReduceQueryRequestEvent event) {
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
        mapReduceQueryRequestHandlers
                .stream()
                .filter(requestHandler -> shouldHandleRequest(requestHandler, isSelfRequest))
                .parallel()
                .forEach(h -> handleRequest(h, event));
        // @formatter:on
    }
    
    private boolean shouldHandleRequest(MapReduceQueryRequestHandler handler, boolean isSelfRequest) {
        return !isSelfRequest || handler instanceof QueryRequestHandler.QuerySelfRequestHandler;
    }
    
    private void handleRequest(MapReduceQueryRequestHandler mapReducequeryRequestHandler, RemoteMapReduceQueryRequestEvent mapReducequeryRequestEvent) {
        try {
            mapReducequeryRequestHandler.handleRemoteRequest(mapReducequeryRequestEvent.getRequest(), mapReducequeryRequestEvent.getOriginService(),
                            mapReducequeryRequestEvent.getDestinationService());
        } catch (Exception e) {
            log.error("Failed to handle map reduce query request with handler: " + mapReducequeryRequestHandler.getClass().getName(), e);
        }
    }
}
