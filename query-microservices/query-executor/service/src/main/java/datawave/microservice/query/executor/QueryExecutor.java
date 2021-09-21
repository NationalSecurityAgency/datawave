package datawave.microservice.query.executor;

import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.executor.action.Create;
import datawave.microservice.query.executor.action.ExecutorAction;
import datawave.microservice.query.executor.action.Next;
import datawave.microservice.query.executor.action.Plan;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.remote.QueryRequestHandler;
import datawave.microservice.query.storage.QueryQueueManager;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.query.storage.TaskStates;
import datawave.microservice.querymetric.QueryMetricClient;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.services.common.connection.AccumuloConnectionFactory;
import datawave.services.query.logic.QueryLogicFactory;
import datawave.services.query.runner.AccumuloConnectionRequestMap;
import org.apache.log4j.Logger;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This class holds the business logic for handling a task notification
 *
 **/
@Service
public class QueryExecutor implements QueryRequestHandler {
    private static final Logger log = Logger.getLogger(QueryExecutor.class);
    
    protected final BlockingQueue<Runnable> workQueue;
    protected final Set<Runnable> working;
    protected final QueryStorageCache cache;
    protected final QueryQueueManager queues;
    protected final QueryLogicFactory queryLogicFactory;
    protected final ExecutorProperties executorProperties;
    protected final QueryProperties queryProperties;
    protected final BusProperties busProperties;
    protected final ThreadPoolExecutor threadPool;
    protected final ApplicationContext appCtx;
    protected final ApplicationEventPublisher publisher;
    protected final AccumuloConnectionFactory connectionFactory;
    protected final AccumuloConnectionRequestMap connectionRequestMap = new AccumuloConnectionRequestMap();
    protected final QueryMetricFactory metricFactory;
    protected final QueryMetricClient metricClient;
    
    public QueryExecutor(ExecutorProperties executorProperties, QueryProperties queryProperties, BusProperties busProperties, ApplicationContext appCtx,
                    AccumuloConnectionFactory connectionFactory, QueryStorageCache cache, QueryQueueManager queues, QueryLogicFactory queryLogicFactory,
                    ApplicationEventPublisher publisher, QueryMetricFactory metricFactory, QueryMetricClient metricClient) {
        this.executorProperties = executorProperties;
        this.queryProperties = queryProperties;
        this.busProperties = busProperties;
        this.cache = cache;
        this.queues = queues;
        this.queryLogicFactory = queryLogicFactory;
        this.appCtx = appCtx;
        this.connectionFactory = connectionFactory;
        this.publisher = publisher;
        this.workQueue = new LinkedBlockingDeque<>(executorProperties.getMaxQueueSize());
        this.working = Collections.synchronizedSet(new HashSet<>());
        this.metricFactory = metricFactory;
        this.metricClient = metricClient;
        threadPool = new ThreadPoolExecutor(executorProperties.getCoreThreads(), executorProperties.getMaxThreads(), executorProperties.getKeepAliveMs(),
                        TimeUnit.MILLISECONDS, workQueue) {
            @Override
            protected void beforeExecute(Thread t, Runnable r) {
                working.add(r);
            }
            
            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                working.remove(r);
            }
        };
        log.info("Created QueryExecutor with an application name of " + appCtx.getApplicationName() + " and an id of " + appCtx.getId());
        log.info("Listening to bus id " + busProperties.getId() + " with a destination of " + busProperties.getDestination());
    }
    
    private void removeFromWorkQueue(String queryId) {
        List<Runnable> removals = new ArrayList<Runnable>();
        for (Runnable action : workQueue) {
            if (((ExecutorAction) action).getTaskKey().getQueryId().equals(queryId)) {
                removals.add(action);
            }
        }
        for (Runnable action : removals) {
            threadPool.remove(action);
        }
    }
    
    private void interruptWork(String queryId, String userDn) {
        // interrupt any working requests
        synchronized (working) {
            for (Runnable action : working) {
                if (((ExecutorAction) action).getTaskKey().getQueryId().equals(queryId)) {
                    ((ExecutorAction) action).interrupt();
                }
            }
        }
        // interrupt any pending connection requests
        connectionRequestMap.cancelConnectionRequest(queryId, userDn);
    }
    
    @Override
    public void handleRemoteRequest(QueryRequest queryRequest, String originService, String destinationService) {
        handleRemoteRequest(queryRequest, originService, false);
    }
    
    public void handleRemoteRequest(QueryRequest queryRequest, String originService, boolean wait) {
        final String queryId = queryRequest.getQueryId();
        final QueryRequest.Method action = queryRequest.getMethod();
        log.info("Received request " + queryRequest);
        
        handleRequest(queryId, action, originService, wait);
    }
    
    public void handleRequest(String queryId, QueryRequest.Method action, String originService, boolean wait) {
        final QueryStatus queryStatus = cache.getQueryStatus(queryId);
        
        // validate we actual have such a query
        if (queryStatus == null) {
            String msg = "Failed to find stored query status for " + queryId;
            log.error(msg);
            throw new RuntimeException(msg);
        }
        
        // validate that we got a request for the correct pool
        if (!queryStatus.getQueryKey().getQueryPool().equals(executorProperties.getPool())) {
            String msg = "Received a request for a query that belongs to a different pool: " + queryStatus.getQueryKey().getQueryPool() + " vs "
                            + executorProperties.getPool();
            log.error(msg);
            throw new RuntimeException(msg);
        }
        
        // A close request waits for the current page to finish
        switch (action) {
            case CLOSE:
                removeFromWorkQueue(queryId);
                break;
            case CANCEL:
                removeFromWorkQueue(queryId);
                interruptWork(queryId, queryStatus.getQuery().getUserDN());
                break;
            default: {
                // get the query states from the cache
                TaskStates taskStates = cache.getTaskStates(queryId);
                
                if (taskStates != null) {
                    log.debug("Searching for a task ready to run for " + queryId);
                    Map<TaskStates.TASK_STATE,Set<TaskKey>> taskStateMap = taskStates.getTaskStates();
                    TaskKey taskKey = null;
                    if (taskStateMap.containsKey(TaskStates.TASK_STATE.READY)) {
                        for (TaskKey key : taskStateMap.get(TaskStates.TASK_STATE.READY)) {
                            if (key.getAction() == action) {
                                taskKey = key;
                                log.info("Found task " + taskKey);
                                break;
                            }
                        }
                    }
                    
                    if (taskKey != null) {
                        QueryTask task = cache.getTask(taskKey);
                        if (task != null) {
                            ExecutorAction runnable = null;
                            switch (action) {
                                case CREATE:
                                    runnable = new Create(this, task, originService);
                                    break;
                                case NEXT:
                                    runnable = new Next(this, task);
                                    break;
                                case PLAN:
                                    runnable = new Plan(this, task);
                                    break;
                                default:
                                    throw new UnsupportedOperationException(task.getTaskKey().toString());
                            }
                            
                            if (wait) {
                                runnable.run();
                            } else {
                                threadPool.execute(runnable);
                            }
                        } else {
                            log.error("Need to cleanup task states because we are referencing a task that no longer exists: " + taskKey);
                            cache.updateTaskState(taskKey, TaskStates.TASK_STATE.FAILED);
                        }
                    }
                } else {
                    log.error("Need to cleanup query because we have no task states: " + queryId);
                    // TODO: cleanup the query because we are referencing a query that has no task states ??
                }
            }
        }
    }
    
    public QueryStorageCache getCache() {
        return cache;
    }
    
    public QueryQueueManager getQueues() {
        return queues;
    }
    
    public QueryLogicFactory getQueryLogicFactory() {
        return queryLogicFactory;
    }
    
    public ExecutorProperties getExecutorProperties() {
        return executorProperties;
    }
    
    public QueryProperties getQueryProperties() {
        return queryProperties;
    }
    
    public BusProperties getBusProperties() {
        return busProperties;
    }
    
    public ApplicationContext getAppCtx() {
        return appCtx;
    }
    
    public ApplicationEventPublisher getPublisher() {
        return publisher;
    }
    
    public AccumuloConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }
    
    public AccumuloConnectionRequestMap getConnectionRequestMap() {
        return connectionRequestMap;
    }
    
    public QueryMetricFactory getMetricFactory() {
        return metricFactory;
    }
    
    public QueryMetricClient getMetricClient() {
        return metricClient;
    }
    
}
