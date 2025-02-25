package datawave.microservice.query.executor;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import org.springframework.http.MediaType;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.Multimap;

import datawave.microservice.query.executor.action.ExecutorTask;
import datawave.microservice.query.result.ExecutorMetricsResponse;
import datawave.microservice.query.result.QueryTaskDescription;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Query Executor Controller /v1", description = "DataWave Query Executor Operations",
                externalDocs = @ExternalDocumentation(description = "Query Executor Service Documentation",
                                url = "https://github.com/NationalSecurityAgency/datawave-query-executor-service"))
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
     * @return an ExecutorMetricsResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Returns metrics for the AccumuloConnectionFactory.")
    @Timed(name = "dw.query.executor.getConnectionFactoryMetrics", absolute = true)
    @Secured({"Administrator", "JBossAdministrator", "InternalUser"})
    @RequestMapping(path = "AccumuloConnectionFactory/stats", method = {RequestMethod.GET},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
    public ExecutorMetricsResponse getConnectionFactoryMetrics() {
        ExecutorMetricsResponse response = new ExecutorMetricsResponse();
        response.setTitle("Accumulo Connection Factory Metrics");
        response.setPool(null); // don't specify pool so that we see all of them in the connection factory
        response.setQueryMetricsUrlPrefix(queryExecutor.getExecutorProperties().getQueryMetricsUrlPrefix());
        response.setConnectionPools(queryExecutor.connectionFactory.getConnectionPools());
        response.setThreadPoolStatus(null);
        response.setQueryToTask(null);
        return response;
    }
    
    /**
     * <strong>JBossAdministrator or Administrator credentials required.</strong> Returns metrics for the Executor thread pool
     *
     * @return an ExecutorMetricsResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Returns metrics for the Executor thread pool.")
    @Timed(name = "dw.query.executor.getExecutorThreadPoolMetrics", absolute = true)
    @Secured({"Administrator", "JBossAdministrator", "InternalUser"})
    @RequestMapping(path = "ThreadPool/stats", method = {RequestMethod.GET},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
    public ExecutorMetricsResponse getExecutorThreadPoolMetrics() {
        ExecutorMetricsResponse response = new ExecutorMetricsResponse();
        response.setTitle("Executor Thread Pool Metrics for " + queryExecutor.getExecutorProperties().getPool());
        response.setPool(queryExecutor.getExecutorProperties().getPool());
        response.setQueryMetricsUrlPrefix(queryExecutor.getExecutorProperties().getQueryMetricsUrlPrefix());
        response.setConnectionPools(null);
        response.setThreadPoolStatus(getThreadPoolStatus(queryExecutor.getThreadPoolExecutor()));
        response.setQueryToTask(null);
        return response;
    }
    
    /**
     * <strong>JBossAdministrator or Administrator credentials required.</strong> Returns queries being serviced
     *
     * @return an ExecutorMetricsResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Returns queries being serviced.")
    @Timed(name = "dw.query.executor.getExecutorQueries", absolute = true)
    @Secured({"Administrator", "JBossAdministrator", "InternalUser"})
    @RequestMapping(path = "queries", method = {RequestMethod.GET},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
    public ExecutorMetricsResponse getExecutorQueries() {
        ExecutorMetricsResponse response = new ExecutorMetricsResponse();
        response.setTitle("Executor Queries for " + queryExecutor.getExecutorProperties().getPool());
        response.setPool(queryExecutor.getExecutorProperties().getPool());
        response.setQueryMetricsUrlPrefix(queryExecutor.getExecutorProperties().getQueryMetricsUrlPrefix());
        response.setConnectionPools(null);
        response.setThreadPoolStatus(null);
        response.setQueryToTask(getQueryToTask(queryExecutor.getQueryToTasks()));
        return response;
    }
    
    /**
     * <strong>JBossAdministrator or Administrator credentials required.</strong> Returns all metrics and queries for the executor
     *
     * @return an ExecutorMetricsResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Operation(summary = "Returns all metrics and queries for the executor.")
    @Timed(name = "dw.query.executor.getExecutorMetrics", absolute = true)
    @Secured({"Administrator", "JBossAdministrator", "InternalUser"})
    @RequestMapping(path = "stats", method = {RequestMethod.GET},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
    public ExecutorMetricsResponse getExecutorMetrics() {
        ExecutorMetricsResponse response = new ExecutorMetricsResponse();
        response.setTitle("Executor Metrics for " + queryExecutor.getExecutorProperties().getPool());
        response.setPool(queryExecutor.getExecutorProperties().getPool());
        response.setQueryMetricsUrlPrefix(queryExecutor.getExecutorProperties().getQueryMetricsUrlPrefix());
        response.setConnectionPools(null);
        response.setThreadPoolStatus(getThreadPoolStatus(queryExecutor.getThreadPoolExecutor()));
        response.setQueryToTask(getQueryToTask(queryExecutor.getQueryToTasks()));
        return response;
    }
    
    private Map<String,String> getThreadPoolStatus(ThreadPoolExecutor threadPoolExecutor) {
        Map<String,String> status = new HashMap<>();
        status.put("status", getStatus(threadPoolExecutor));
        status.put("pool size", String.valueOf(threadPoolExecutor.getPoolSize()));
        status.put("active threads", String.valueOf(threadPoolExecutor.getActiveCount()));
        status.put("queued tasks", String.valueOf(threadPoolExecutor.getQueue().size()));
        status.put("completed tasks", String.valueOf(threadPoolExecutor.getCompletedTaskCount()));
        return status;
    }
    
    private String getStatus(ThreadPoolExecutor threadPoolExecutor) {
        if (threadPoolExecutor.isShutdown())
            return "Shutdown";
        else if (threadPoolExecutor.isTerminated())
            return "Terminated";
        else if (threadPoolExecutor.isTerminating())
            return "Terminating";
        else
            return "Running";
    }
    
    private Map<String,Collection<QueryTaskDescription>> getQueryToTask(Multimap<String,ExecutorTask> queryTasks) {
        Map<String,Collection<QueryTaskDescription>> queryToTask = new HashMap<>();
        for (Map.Entry<String,Collection<ExecutorTask>> entry : queryTasks.asMap().entrySet()) {
            queryToTask.put(entry.getKey(), entry.getValue().stream().map(r -> new QueryTaskDescription(String.valueOf(r.getTaskKey().getTaskId()),
                            r.getTask().getAction().name(), r.getTaskKey().getQueryLogic(), getStatus(r))).collect(Collectors.toList()));
        }
        return queryToTask;
    }
    
    private String getStatus(ExecutorTask task) {
        if (task.isTaskComplete()) {
            return "Complete";
        } else if (task.isTaskFailed()) {
            return "Failed";
        } else if (task.isInterrupted()) {
            return "Interrupted";
        } else if (task.isRunning()) {
            return "Running";
        } else {
            return "Queued";
        }
    }
    
}
