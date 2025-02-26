package datawave.microservice.metadata.remote.event.listener;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.bus.ConditionalOnBusEnabled;
import org.springframework.cloud.bus.ServiceMatcher;
import org.springframework.cloud.bus.event.TableCacheReloadRequestEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import datawave.microservice.metadata.remote.TableCacheReloadRequestHandler;

@Component
@ConditionalOnBusEnabled
@ConditionalOnProperty(name = "datawave.table.cache.enabled", havingValue = "true", matchIfMissing = true)
public class TableCacheReloadRequestEventListener implements ApplicationListener<TableCacheReloadRequestEvent> {
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final List<TableCacheReloadRequestHandler> tableCacheReloadRequestHandlers;
    private final ServiceMatcher serviceMatcher;
    
    public TableCacheReloadRequestEventListener(List<TableCacheReloadRequestHandler> tableCacheReloadRequestHandlers, ServiceMatcher serviceMatcher) {
        this.tableCacheReloadRequestHandlers = tableCacheReloadRequestHandlers;
        this.serviceMatcher = serviceMatcher;
    }
    
    @Override
    public void onApplicationEvent(TableCacheReloadRequestEvent event) {
        // Ignore events that this service instance published, since we publish from a place
        // that takes the same action we do here, and we don't want to repeat the work.
        boolean isSelfRequest = serviceMatcher.isFromSelf(event);
        if (isSelfRequest) {
            log.debug("Received a self-request {}.", event);
        }
        
        // process the event using each tableCacheReload request handler.
        // By default, for parallelStreams java uses threads equal to the number of cores.
        // if we need more than that, we can specify our own ForkJoinPool.
        // @formatter:off
        tableCacheReloadRequestHandlers
                .stream()
                .filter(requestHandler -> shouldHandleRequest(requestHandler, isSelfRequest))
                .parallel()
                .forEach(h -> handleRequest(h, event));
        // @formatter:on
    }
    
    private boolean shouldHandleRequest(TableCacheReloadRequestHandler handler, boolean isSelfRequest) {
        return !isSelfRequest || handler instanceof TableCacheReloadRequestHandler.QuerySelfRequestHandler;
    }
    
    private void handleRequest(TableCacheReloadRequestHandler tableCacheReloadRequestHandler, TableCacheReloadRequestEvent tableCacheReloadRequestEvent) {
        try {
            tableCacheReloadRequestHandler.handleRemoteRequest(tableCacheReloadRequestEvent.getTableName(), tableCacheReloadRequestEvent.getOriginService(),
                            tableCacheReloadRequestEvent.getDestinationService());
        } catch (Exception e) {
            log.error("Failed to handle tableCacheReload request with handler: " + tableCacheReloadRequestHandler.getClass().getName(), e);
        }
    }
}
