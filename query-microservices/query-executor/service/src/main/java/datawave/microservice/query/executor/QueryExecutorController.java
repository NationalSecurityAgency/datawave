package datawave.microservice.query.executor;

import com.codahale.metrics.annotation.Timed;
import datawave.services.common.result.ConnectionFactoryResponse;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.security.RolesAllowed;

@RestController
@RequestMapping(path = "/v1", produces = MediaType.APPLICATION_JSON_VALUE)
public class QueryExecutorController {
    private final QueryExecutor queryExecutor;
    
    public QueryExecutorController(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }
    
    /**
     * <strong>JBossAdministrator or Administrator credentials required.</strong> Returns metrics for the AccumuloConnectionFactory
     *
     * @return datawave.webservice.common.ConnectionFactoryResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Timed(name = "dw.query.executor.getConnectionFactoryMetrics", absolute = true)
    @RolesAllowed({"Administrator", "JBossAdministrator", "InternalUser"})
    @RequestMapping(path = "Common/AccumuloConnectionFactory/stats", method = {RequestMethod.GET},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
    public ConnectionFactoryResponse getConnectionFactoryMetrics() {
        ConnectionFactoryResponse response = new ConnectionFactoryResponse();
        response.setConnectionPools(queryExecutor.connectionFactory.getConnectionPools());
        return response;
    }
}
