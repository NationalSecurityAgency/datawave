package datawave.microservice.metadata.tablecache;

import org.apache.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.codahale.metrics.annotation.Timed;

import datawave.core.common.result.AccumuloTableCacheStatus;
import datawave.webservice.result.VoidResponse;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Table Cache Controller /v1", description = "DataWave Table Cache Operations",
                externalDocs = @ExternalDocumentation(description = "Spring Boot Starter Datawave Metadata Documentation",
                                url = "https://github.com/NationalSecurityAgency/datawave-spring-boot-starter-metadata"))
@RestController
@RequestMapping(path = "/v1/AccumuloTableCache", produces = MediaType.APPLICATION_JSON_VALUE)
@ConditionalOnProperty(name = "datawave.table.cache.enabled", havingValue = "true", matchIfMissing = true)
public class TableCacheController {
    private final Logger log = Logger.getLogger(TableCacheController.class);
    private final TableCacheReloadService service;
    
    public TableCacheController(TableCacheReloadService service) {
        this.service = service;
    }
    
    /**
     * Force a reload the table cache for the specified table.
     *
     * @return a VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Force a reload of the table cache for the specified table.")
    @Timed(name = "dw.table.cache.reloadCache", absolute = true)
    @Secured({"AuthorizedUser", "AuthorizedQueryServer", "AuthorizedServer", "InternalUser", "Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/reload/{tableName}", method = {RequestMethod.GET},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
    public VoidResponse reloadCache(@PathVariable String tableName) {
        log.debug("Handling reload request for " + tableName + " and notifying other executor services");
        service.reloadTable(tableName, true);
        VoidResponse response = new VoidResponse();
        response.addMessage(tableName + " reloaded and message sent to all other executor services");
        return response;
    }
    
    /**
     * Get the status of the table caches
     *
     * @return the AccumuloTableCacheStatus
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @RequestHeader query-session-id session id value used for load balancing purposes. query-session-id can be placed in the request in a Cookie header or as
     *                a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *                
     * @HTTP 200 success
     */
    @Operation(summary = "Get the status of the table caches.")
    @Timed(name = "dw.table.cache.getStatus", absolute = true)
    @Secured({"AuthorizedUser", "AuthorizedQueryServer", "AuthorizedServer", "InternalUser", "Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/", method = {RequestMethod.GET},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
    public AccumuloTableCacheStatus getStatus() {
        AccumuloTableCacheStatus response = new AccumuloTableCacheStatus();
        response.getCaches().addAll(service.getTableCache().getTableCaches());
        return response;
    }
}
