package datawave.microservice.query.executor;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.Multimap;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.executor.action.ExecutorTask;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.executor.task.FindWorkTask;
import datawave.microservice.query.remote.QueryRequestHandler;
import datawave.microservice.query.remote.TableCacheReloadRequestHandler;
import datawave.microservice.query.result.ExecutorMetricsResponse;
import datawave.microservice.query.result.QueryTaskDescription;
import datawave.services.common.cache.AccumuloTableCache;
import datawave.services.common.cache.TableCache;
import datawave.services.common.cache.TableCacheDescription;
import datawave.services.common.result.AccumuloTableCacheStatus;
import datawave.webservice.result.VoidResponse;
import org.apache.log4j.Logger;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.cloud.bus.event.RemoteQueryRequestEvent;
import org.springframework.cloud.bus.event.TableCacheReloadRequestEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import static datawave.microservice.http.converter.protostuff.ProtostuffHttpMessageConverter.PROTOSTUFF_VALUE;

@RestController
@RequestMapping(path = "/v1/AccumuloTableCache", produces = MediaType.APPLICATION_JSON_VALUE)
public class TableCacheController implements TableCacheReloadRequestHandler {
    private final Logger log = Logger.getLogger(TableCacheController.class);
    private final AccumuloTableCache cache;
    private final BusProperties busProperties;
    private final ApplicationEventPublisher publisher;
    private final QueryProperties queryProperties;
    
    public TableCacheController(AccumuloTableCache cache, BusProperties busProperties, ApplicationEventPublisher publisher, QueryProperties queryProperties) {
        this.cache = cache;
        this.busProperties = busProperties;
        this.publisher = publisher;
        this.queryProperties = queryProperties;
    }
    
    /**
     * <strong>JBossAdministrator or Administrator credentials required.</strong> Force a reload the table cache for the specified table.
     *
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Timed(name = "dw.query.executor.tableCache.reloadCache", absolute = true)
    @Secured({"AuthorizedUser", "AuthorizedQueryServer", "AuthorizedServer", "InternalUser", "Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/reload/{tableName}", method = {RequestMethod.GET},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
    public VoidResponse reloadCache(@PathVariable String tableName) {
        cache.reloadTableCache(tableName);
        sendCacheReloadMessage(tableName);
        return new VoidResponse();
    }
    
    private void sendCacheReloadMessage(String tableName) {
        log.warn("Sending cache reload message about table " + tableName);
        // @formatter:off
        publisher.publishEvent(
                new TableCacheReloadRequestEvent(
                        cache,
                        busProperties.getId(),
                        queryProperties.getExecutorServiceName(), // TODO + "-*" ???
                        tableName));
        // @formatter:on
    }
    
    /**
     * <strong>JBossAdministrator or Administrator credentials required.</strong> Get the status of the table caches
     *
     * @return datawave.webservice.common.result.AccumuloTableCacheStatus
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @RequestHeader query-session-id session id value used for load balancing purposes. query-session-id can be placed in the request in a Cookie header or as
     *                a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     */
    @Timed(name = "dw.query.executor.tableCache.getStatus", absolute = true)
    @Secured({"AuthorizedUser", "AuthorizedQueryServer", "AuthorizedServer", "InternalUser", "Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/", method = {RequestMethod.GET},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
    public AccumuloTableCacheStatus getStatus() {
        AccumuloTableCacheStatus response = new AccumuloTableCacheStatus();
        response.getCaches().addAll(cache.getTableCaches());
        return response;
    }
    
    @Override
    public void handleRemoteRequest(String tableName, String originService, String destinationService) {
        cache.reloadTableCache(tableName);
    }
}
