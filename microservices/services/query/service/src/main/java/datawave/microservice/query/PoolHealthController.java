package datawave.microservice.query;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.executor.status.cache.ExecutorPoolStatus;
import datawave.microservice.query.executor.status.cache.ExecutorStatusCache;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Pool Health Controller /v1", description = "DataWave Query Pool Health",
                externalDocs = @ExternalDocumentation(description = "Query Service Documentation",
                                url = "https://github.com/NationalSecurityAgency/datawave-query-service"))
@RestController
@RequestMapping(path = "/v1/pool", produces = MediaType.APPLICATION_JSON_VALUE)
public class PoolHealthController {
    
    private final QueryProperties queryProperties;
    private final ExecutorStatusCache executorStatusCache;
    
    @Value("${datawave.connection.factory.default-pool:default}")
    private String defaultConnectionPool;
    
    @Value("${spring.security.datawave.manager-roles:'{}'}")
    private Set<String> managerRoles;
    
    public PoolHealthController(QueryProperties queryProperties, ExecutorStatusCache executorStatusCache) {
        this.queryProperties = queryProperties;
        this.executorStatusCache = executorStatusCache;
    }
    
    // NOTE: This endpoint is unauthenticated, as configured in QueryServiceConfiguration.java
    @RequestMapping(path = "{poolName}/health", method = {RequestMethod.GET}, produces = {"application/json"})
    public ResponseEntity<PoolHealth> health(@Parameter(description = "The query pool name", example = "default") @PathVariable String poolName,
                    @Parameter(description = "The conneciton pool name") @RequestParam(required = false) String connectionPool) {
        return getPoolHealth(poolName, connectionPool, null);
    }
    
    @RequestMapping(path = "{poolName}/status", method = {RequestMethod.GET}, produces = {"application/json"})
    public ResponseEntity<PoolHealth> status(@Parameter(description = "The query pool name", example = "default") @PathVariable String poolName,
                    @Parameter(description = "The conneciton pool name") @RequestParam(required = false) String connectionPool,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) {
        return getPoolHealth(poolName, connectionPool, currentUser);
    }
    
    protected ResponseEntity<PoolHealth> getPoolHealth(String poolName, String connectionPool, DatawaveUserDetails currentUser) {
        boolean isAdminUser = currentUser != null && !Collections.disjoint(currentUser.getPrimaryUser().getRoles(), managerRoles);
        
        connectionPool = (connectionPool != null) ? connectionPool : defaultConnectionPool;
        
        PoolHealth poolHealth = (isAdminUser) ? new ExtendedPoolHealth() : new PoolHealth();
        poolHealth.setQueryPool(poolName);
        poolHealth.setConnectionPool(connectionPool);
        
        executorStatusCache.lock(poolName);
        try {
            ExecutorPoolStatus poolStatus = executorStatusCache.get(poolName);
            
            if (poolStatus != null) {
                QueryProperties.PoolProperties poolLimits = queryProperties.getPoolLimits().get(poolName);
                
                Set<String> activeExecutors = new LinkedHashSet<>();
                Set<String> inactiveExecutors = new LinkedHashSet<>();
                long currentTime = System.currentTimeMillis();
                for (Map.Entry<String,Long> entry : poolStatus.getExecutorHeartbeat().entrySet()) {
                    if ((currentTime - entry.getValue()) > poolLimits.getLivenessTimeoutMillis()) {
                        inactiveExecutors.add(entry.getKey());
                    } else {
                        activeExecutors.add(entry.getKey());
                    }
                }
                
                int maxQueriesPerExecutor = poolLimits.getMaxQueriesPerExecutor().get(connectionPool);
                int maxQueries = maxQueriesPerExecutor * activeExecutors.size();
                Integer runningQueries = poolStatus.getQueryCountByConnectionPool().get(connectionPool);
                if (runningQueries == null) {
                    runningQueries = 0;
                }
                
                poolHealth.setHealthy(runningQueries < maxQueries);
                
                // set the extended info
                if (poolHealth instanceof ExtendedPoolHealth) {
                    ExtendedPoolHealth extendedPoolHealth = (ExtendedPoolHealth) poolHealth;
                    extendedPoolHealth.setMaxQueriesPerExecutor(maxQueriesPerExecutor);
                    extendedPoolHealth.setMaxQueries(maxQueries);
                    extendedPoolHealth.setRunningQueries(runningQueries);
                }
                
                // remove the inactive executors and update the cache
                poolStatus.getExecutorHeartbeat().keySet().removeAll(inactiveExecutors);
                executorStatusCache.update(poolName, poolStatus);
            } else {
                // if the pool isn't found, trigger a 400 response
                poolHealth = null;
            }
        } finally {
            executorStatusCache.unlock(poolName);
        }
        
        ResponseEntity<PoolHealth> responseEntity = null;
        if (poolHealth == null) {
            responseEntity = ResponseEntity.badRequest().build();
        } else if (poolHealth.isHealthy()) {
            responseEntity = ResponseEntity.ok(poolHealth);
        } else {
            responseEntity = ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(poolHealth);
        }
        
        return responseEntity;
        
    }
    
    public static class PoolHealth {
        protected boolean isHealthy;
        protected String queryPool;
        protected String connectionPool;
        
        public boolean isHealthy() {
            return isHealthy;
        }
        
        public void setHealthy(boolean healthy) {
            isHealthy = healthy;
        }
        
        public String getQueryPool() {
            return queryPool;
        }
        
        public void setQueryPool(String queryPool) {
            this.queryPool = queryPool;
        }
        
        public String getConnectionPool() {
            return connectionPool;
        }
        
        public void setConnectionPool(String connectionPool) {
            this.connectionPool = connectionPool;
        }
    }
    
    public static class ExtendedPoolHealth extends PoolHealth {
        protected int maxQueriesPerExecutor;
        protected int maxQueries;
        protected int runningQueries;
        
        public int getMaxQueriesPerExecutor() {
            return maxQueriesPerExecutor;
        }
        
        public void setMaxQueriesPerExecutor(int maxQueriesPerExecutor) {
            this.maxQueriesPerExecutor = maxQueriesPerExecutor;
        }
        
        public int getMaxQueries() {
            return maxQueries;
        }
        
        public void setMaxQueries(int maxQueries) {
            this.maxQueries = maxQueries;
        }
        
        public int getRunningQueries() {
            return runningQueries;
        }
        
        public void setRunningQueries(int runningQueries) {
            this.runningQueries = runningQueries;
        }
    }
}
