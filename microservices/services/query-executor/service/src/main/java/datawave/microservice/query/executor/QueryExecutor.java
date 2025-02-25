package datawave.microservice.query.executor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.log4j.Logger;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.core.query.predict.QueryPredictor;
import datawave.core.query.runner.AccumuloConnectionRequestMap;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.executor.action.CreateTask;
import datawave.microservice.query.executor.action.ExecutorTask;
import datawave.microservice.query.executor.action.PlanTask;
import datawave.microservice.query.executor.action.PredictTask;
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

/**
 * This class holds the business logic for handling a task notification
 *
 **/
@Service
public class QueryExecutor implements QueryRequestHandler.QuerySelfRequestHandler {
    private static final Logger log = Logger.getLogger(QueryExecutor.class);
    
    protected final BlockingQueue<Runnable> workQueue;
    protected final Set<Runnable> working;
    protected final Multimap<String,ExecutorTask> queryToTask;
    protected final QueryStorageCache cache;
    protected final QueryResultsManager queues;
    protected final QueryLogicFactory queryLogicFactory;
    protected final QueryPredictor predictor;
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
                    QueryPredictor predictor, ApplicationEventPublisher publisher, QueryMetricFactory metricFactory, QueryMetricClient metricClient) {
        this.executorProperties = executorProperties;
        this.queryProperties = queryProperties;
        this.busProperties = busProperties;
        this.cache = cache;
        this.queues = queues;
        this.queryLogicFactory = queryLogicFactory;
        this.predictor = predictor;
        this.appCtx = appCtx;
        this.connectionFactory = connectionFactory;
        this.publisher = publisher;
        this.workQueue = new LinkedBlockingDeque<>(executorProperties.getMaxQueueSize());
        this.working = Collections.synchronizedSet(new HashSet<>());
        this.queryToTask = Multimaps.synchronizedMultimap(LinkedHashMultimap.create());
        this.metricFactory = metricFactory;
        this.metricClient = metricClient;
        threadPool = new ThreadPoolExecutor(executorProperties.getCoreThreads(), executorProperties.getMaxThreads(), executorProperties.getKeepAliveMs(),
                        TimeUnit.MILLISECONDS, workQueue) {
            @Override
            protected void beforeExecute(Thread t, Runnable r) {
                log.debug("Before execute " + ((ExecutorTask) r).getTaskKey());
                working.add(r);
                
            }
            
            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                log.debug("After execute " + ((ExecutorTask) r).getTaskKey());
                working.remove(r);
                queryToTask.remove(((ExecutorTask) r).getTaskKey().getQueryId(), r);
            }
        };
        log.info("Created QueryExecutor with an application name of " + appCtx.getApplicationName() + " and an id of " + appCtx.getId());
        log.info("Listening to bus id " + busProperties.getId() + " with a destination of " + busProperties.getDestination());
    }
    
    /**
     * Return true if we are working on any tasking for the specified query id
     * 
     * @param queryId
     * @return true if working on query, false otherwise
     */
    public boolean isWorkingOn(String queryId) {
        return queryToTask.containsKey(queryId);
    }
    
    private void removePendingTasks(String queryId) {
        Collection<ExecutorTask> tasks;
        // synchronize explicitly to avoid mutations during the iteration
        synchronized (queryToTask) {
            tasks = new HashSet<>(queryToTask.get(queryId));
            queryToTask.removeAll(queryId);
        }
        for (ExecutorTask action : tasks) {
            log.debug("Removing potentially pending task " + action.getTaskKey());
            threadPool.remove(action);
        }
    }
    
    private void stopTasks(String queryId, String userDn) {
        Collection<ExecutorTask> tasks;
        // synchronize explicitly to avoid mutations during the iteration
        synchronized (queryToTask) {
            tasks = new HashSet<>(queryToTask.get(queryId));
            queryToTask.removeAll(queryId);
        }
        while (!tasks.isEmpty()) {
            for (ExecutorTask action : tasks) {
                log.debug("Stopping task " + action.getTaskKey());
                threadPool.remove(action);
                action.interrupt();
            }
            synchronized (queryToTask) {
                tasks = new HashSet<>(queryToTask.get(queryId));
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
        
        // validate we actually have such a query
        if (queryStatus == null) {
            String msg = "Failed to find stored query status for " + queryId + ", nothing to execute";
            log.warn(msg);
            return;
        }
        
        // validate that we got a request for the correct pool
        if (!queryStatus.getQueryKey().getQueryPool().equals(executorProperties.getPool())) {
            String msg = "Received a request for a query that belongs to a different pool: " + queryStatus.getQueryKey().getQueryPool() + " vs "
                            + executorProperties.getPool();
            log.error(msg);
            return;
        }
        
        // A close request waits for the current page to finish
        switch (requestedAction) {
            case CLOSE:
                removePendingTasks(queryId);
                break;
            case CANCEL:
                stopTasks(queryId, queryStatus.getQuery().getUserDN());
                break;
            default: {
                List<QueryTask> tasks = findTasksToExecute(queryStatus, requestedAction);
                
                // if we have tasks, then run them
                for (QueryTask task : tasks) {
                    log.info("Executing task " + task.getTaskKey() + ": " + task.getQueryCheckpoint());
                    ExecutorTask runnable = null;
                    switch (task.getAction()) {
                        case CREATE:
                            runnable = new CreateTask(this, task, originService);
                            break;
                        case PLAN:
                            runnable = new PlanTask(this, task, originService);
                            break;
                        case PREDICT:
                            runnable = new PredictTask(this, task, originService);
                            break;
                        case NEXT:
                            runnable = new ResultsTask(this, task);
                            break;
                        default:
                            throw new UnsupportedOperationException(task.getTaskKey().toString());
                    }
                    
                    try {
                        queryToTask.put(queryId, runnable);
                        threadPool.execute(runnable);
                    } catch (Exception e) {
                        log.error("Failed to execute task " + task.getTaskKey() + ", returning to available tasks to execute", e);
                        
                        // reset the task state so that another executor can grab it
                        runnable.completeTask(false, false);
                        queryToTask.remove(queryId, runnable);
                    }
                }
            }
        }
    }
    
    private List<QueryTask> findTasksToExecute(QueryStatus queryStatus, QueryRequest.Method requestedAction) {
        String queryId = queryStatus.getQueryKey().getQueryId();
        
        List<QueryTask> nextTasks = new ArrayList<>();
        QueryTask nextTask = null;
        
        QueryStorageLock lock = cache.getTaskStatesLock(queryId);
        lock.lock();
        try {
            // get the query states from the cache
            TaskStates taskStates = cache.getTaskStates(queryId);
            
            if (taskStates != null) {
                log.debug("Searching for tasks to run for " + queryId);
                
                // get the number of ready tasks that we can start up immediately
                int tasksAvailableToRun = taskStates.getAvailableReadyTasksToRun();
                
                // use less if our thread pool is already full
                tasksAvailableToRun = Math.min(tasksAvailableToRun,
                                threadPool.getMaximumPoolSize() - threadPool.getQueue().size() - threadPool.getActiveCount());
                
                log.info("Getting up to " + tasksAvailableToRun + " tasks to run for " + queryId + " (" + taskStates.taskStatesString() + ",THREADPOOL:"
                                + threadPool.getMaximumPoolSize() + " - " + threadPool.getQueue().size() + " - " + threadPool.getActiveCount() + ")");
                
                // if we can run any tasks, then get em
                if (tasksAvailableToRun > 0) {
                    
                    for (TaskKey taskKey : taskStates.getTasksForState(TaskStates.TASK_STATE.READY, tasksAvailableToRun)) {
                        QueryTask task = cache.getTask(taskKey);
                        
                        // if we have such a task, and the task is for the action requested,
                        // then prepare it for execution otherwise, ignore this task
                        if (task != null && task.getAction() == requestedAction) {
                            log.debug("Found " + taskKey);
                            if (taskStates.setState(taskKey.getTaskId(), TaskStates.TASK_STATE.RUNNING)) {
                                log.debug("Got lock for task " + taskKey);
                                nextTasks.add(task);
                            }
                        } else if (task == null) {
                            log.warn("Task " + taskKey + " is for a non-existent task, ignoring task");
                        } else {
                            log.warn("Task " + taskKey + " is for " + task.getAction() + " but we were looking for " + requestedAction + ", ignoring task");
                        }
                    }
                    
                    // store the updated task states if we changed anything
                    if (!nextTasks.isEmpty()) {
                        // update the tasks last update millis to avoid the appearance of orphaned tasks
                        for (QueryTask task : nextTasks) {
                            cache.updateTask(task);
                        }
                        // now update the task states
                        cache.updateTaskStates(taskStates);
                    }
                }
            } else {
                log.error("Need to cleanup query because we have no task states for " + queryId);
            }
        } finally {
            lock.unlock();
        }
        
        return nextTasks;
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
    
    public QueryPredictor getPredictor() {
        return predictor;
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
    
    public ThreadPoolExecutor getThreadPoolExecutor() {
        return threadPool;
    }
    
    public Multimap<String,ExecutorTask> getQueryToTasks() {
        synchronized (queryToTask) {
            return LinkedHashMultimap.create(queryToTask);
        }
    }
    
    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        return builder.append("\nthreadPoolExecutor", "\n" + threadPool).append("\nqueryTasks", "\n" + queryToTask)
                        .append("\nconnectionFactory", "\n" + connectionFactory.report()).build();
    }
}
