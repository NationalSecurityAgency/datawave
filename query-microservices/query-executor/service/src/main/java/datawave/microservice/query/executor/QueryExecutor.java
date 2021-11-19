package datawave.microservice.query.executor;

import com.zaxxer.sparsebits.SparseBitSet;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.executor.action.CreateTask;
import datawave.microservice.query.executor.action.ExecutorTask;
import datawave.microservice.query.executor.action.PlanTask;
import datawave.microservice.query.executor.action.ResultsTask;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.remote.QueryRequestHandler;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.QueryStorageLock;
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
public class QueryExecutor implements QueryRequestHandler.QuerySelfRequestHandler {
    private static final Logger log = Logger.getLogger(QueryExecutor.class);
    
    protected final BlockingQueue<Runnable> workQueue;
    protected final Set<Runnable> working;
    protected final QueryStorageCache cache;
    protected final QueryResultsManager queues;
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
                    AccumuloConnectionFactory connectionFactory, QueryStorageCache cache, QueryResultsManager queues, QueryLogicFactory queryLogicFactory,
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
            if (((ExecutorTask) action).getTaskKey().getQueryId().equals(queryId)) {
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
                if (((ExecutorTask) action).getTaskKey().getQueryId().equals(queryId)) {
                    ((ExecutorTask) action).interrupt();
                }
            }
        }
        // interrupt any pending connection requests
        connectionRequestMap.cancelConnectionRequest(queryId, userDn);
    }
    
    @Override
    public void handleRemoteRequest(QueryRequest queryRequest, String originService, String destinationService) {
        final String queryId = queryRequest.getQueryId();
        final QueryRequest.Method requestedAction = queryRequest.getMethod();
        log.info("Received request " + queryRequest);
        
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
        switch (requestedAction) {
            case CLOSE:
                removeFromWorkQueue(queryId);
                break;
            case CANCEL:
                removeFromWorkQueue(queryId);
                interruptWork(queryId, queryStatus.getQuery().getUserDN());
                break;
            default: {
                QueryTask task = findNextReadyTask(queryStatus, requestedAction);
                
                // if we have a task, run it
                if (task != null) {
                    log.debug("Executing task " + task.getTaskKey() + ": " + task.getQueryCheckpoint());
                    ExecutorTask runnable = null;
                    switch (task.getAction()) {
                        case CREATE:
                            runnable = new CreateTask(this, task, originService);
                            break;
                        case NEXT:
                            runnable = new ResultsTask(this, task);
                            break;
                        case PLAN:
                            runnable = new PlanTask(this, task);
                            break;
                        default:
                            throw new UnsupportedOperationException(task.getTaskKey().toString());
                    }
                    
                    threadPool.execute(runnable);
                }
            }
        }
    }
    
    private QueryTask findNextReadyTask(QueryStatus queryStatus, QueryRequest.Method requestedAction) {
        String queryId = queryStatus.getQueryKey().getQueryId();
        
        QueryTask nextTask = null;
        
        QueryStorageLock lock = cache.getTaskStatesLock(queryId);
        lock.lock();
        try {
            // get the query states from the cache
            TaskStates taskStates = cache.getTaskStates(queryId);
            
            if (taskStates != null) {
                log.debug("Searching for a task ready to run for " + queryId);
                Map<TaskStates.TASK_STATE,SparseBitSet> taskStateMap = taskStates.getTaskStates();
                if (taskStateMap.containsKey(TaskStates.TASK_STATE.READY)) {
                    SparseBitSet tasks = taskStateMap.get(TaskStates.TASK_STATE.READY);
                    int taskId = tasks.nextSetBit(0);
                    if (taskId != -1) {
                        TaskKey taskKey = new TaskKey(taskId, queryStatus.getQueryKey());
                        QueryTask task = cache.getTask(taskKey);
                        
                        // if we have such a task, and the task is for the action requested,
                        // then prepare it for execution otherwise, ignore this task
                        if (task != null && task.getAction() == requestedAction) {
                            log.debug("Found " + taskKey);
                            if (cache.updateTaskState(taskKey, TaskStates.TASK_STATE.RUNNING)) {
                                log.debug("Got lock for task " + taskKey);
                                nextTask = task;
                            }
                        } else {
                            log.warn("Task " + taskKey + " is for " + task.getAction() + " but we were looking for " + requestedAction + ", ignoring task");
                        }
                    }
                }
            } else {
                log.error("Need to cleanup query because we have no task states: " + queryId);
                // TODO: cleanup the query because we are referencing a query that has no task states ??
            }
        } finally {
            lock.unlock();
        }
        
        return nextTask;
    }
    
    public QueryStorageCache getCache() {
        return cache;
    }
    
    public QueryResultsManager getQueues() {
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
