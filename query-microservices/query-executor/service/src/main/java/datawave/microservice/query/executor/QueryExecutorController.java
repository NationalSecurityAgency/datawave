package datawave.microservice.query.executor;

import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.Multimap;
import datawave.microservice.query.executor.action.ExecutorTask;
import datawave.microservice.query.result.QueryTaskDescription;
import datawave.services.common.result.ConnectionFactoryResponse;
import datawave.microservice.query.result.ExecutorThreadPoolResponse;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.security.RolesAllowed;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

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
    
    /**
     * <strong>JBossAdministrator or Administrator credentials required.</strong> Returns metrics for the Executor thread pool
     *
     * @return datawave.webservice.common.ExecutorThreadPoolResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @HTTP 200 success
     * @HTTP 500 internal server error
     */
    @Timed(name = "dw.query.executor.getExecutorThreadPoolMetrics", absolute = true)
    @RolesAllowed({"Administrator", "JBossAdministrator", "InternalUser"})
    @RequestMapping(path = "stats", method = {RequestMethod.GET},
                    produces = {"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
    public ExecutorThreadPoolResponse getExecutorThreadPoolMetrics() {
        ExecutorThreadPoolResponse response = new ExecutorThreadPoolResponse();
        response.setPool(queryExecutor.getExecutorProperties().getPool());
        response.setConnectionPools(queryExecutor.connectionFactory.getConnectionPools());
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
