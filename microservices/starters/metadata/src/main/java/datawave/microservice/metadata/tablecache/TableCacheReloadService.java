package datawave.microservice.metadata.tablecache;

import org.apache.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.cloud.bus.event.TableCacheReloadRequestEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import datawave.core.common.cache.AccumuloTableCache;
import datawave.microservice.metadata.remote.TableCacheReloadRequestHandler;

@Service
@ConditionalOnProperty(name = "datawave.table.cache.enabled", havingValue = "true", matchIfMissing = true)
public class TableCacheReloadService implements TableCacheReloadRequestHandler {
    private final Logger log = Logger.getLogger(TableCacheReloadService.class);
    private final AccumuloTableCache cache;
    private final BusProperties busProperties;
    private final ApplicationEventPublisher publisher;
    
    public TableCacheReloadService(AccumuloTableCache cache, BusProperties busProperties, ApplicationEventPublisher publisher) {
        this.cache = cache;
        this.busProperties = busProperties;
        this.publisher = publisher;
    }
    
    /**
     * Handle a remote request to reload a table.
     * 
     * @param tableName
     *            The table name
     * @param originService
     *            The origin service
     * @param destinationService
     *            The destination service
     */
    @Override
    public void handleRemoteRequest(String tableName, String originService, String destinationService) {
        log.debug("Handling remote reload request for " + tableName);
        reloadTable(tableName, false);
    }
    
    /**
     * Reload a table and publish a reload message if requested
     * 
     * @param tableName
     *            The table name
     * @param publishEvent
     *            True if we want to notify all other services
     */
    public void reloadTable(String tableName, boolean publishEvent) {
        cache.reloadTableCache(tableName);
        if (publishEvent) {
            sendCacheReloadMessage(tableName);
        }
    }
    
    /**
     * Send a reload message to all other executors
     * 
     * @param tableName
     *            The table name
     */
    private void sendCacheReloadMessage(String tableName) {
        log.warn("Sending cache reload message about table " + tableName);
        // @formatter:off
        publisher.publishEvent(
                new TableCacheReloadRequestEvent(
                        cache,
                        busProperties.getId(),
                        null,  // null destination service will result is sending it to all services
                        tableName));
        // @formatter:on
    }
    
    public AccumuloTableCache getTableCache() {
        return cache;
    }
}
