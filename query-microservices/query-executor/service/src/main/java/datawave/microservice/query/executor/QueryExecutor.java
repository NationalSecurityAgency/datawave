package datawave.microservice.query.executor;

import datawave.microservice.common.storage.QueryQueueManager;
import datawave.microservice.common.storage.QueryStorageCache;
import datawave.microservice.common.storage.QueryTask;
import datawave.microservice.common.storage.TaskKey;
import datawave.microservice.common.storage.TaskStates;
import datawave.microservice.query.executor.action.Create;
import datawave.microservice.query.executor.action.ExecutorAction;
import datawave.microservice.query.executor.action.Next;
import datawave.microservice.query.executor.action.Plan;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.logic.QueryLogicFactory;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.remote.QueryRequestHandler;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This class holds the business logic for handling a task notification
 *
 * TODO: Query Metrics
 **/
public class QueryExecutor implements QueryRequestHandler {
    private static final Logger log = Logger.getLogger(QueryExecutor.class);
    
    protected final BlockingQueue<Runnable> workQueue;
    protected final Set<Runnable> working;
    protected final Connector connector;
    protected final QueryStorageCache cache;
    protected final QueryQueueManager queues;
    protected final QueryLogicFactory queryLogicFactory;
    protected final ExecutorProperties executorProperties;
    protected final ThreadPoolExecutor threadPool;
    
    public QueryExecutor(ExecutorProperties executorProperties, Connector connector, QueryStorageCache cache, QueryQueueManager queues,
                    QueryLogicFactory queryLogicFactory) {
        this.executorProperties = executorProperties;
        this.cache = cache;
        this.queues = queues;
        this.connector = connector;
        this.queryLogicFactory = queryLogicFactory;
        this.workQueue = new LinkedBlockingDeque<>(executorProperties.getMaxQueueSize());
        this.working = Collections.synchronizedSet(new HashSet<>());
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
    }
    
    public static QueryTask.QUERY_ACTION getQueryAction(QueryRequest.Method method) {
        switch (method) {
            case NEXT:
                return QueryTask.QUERY_ACTION.NEXT;
            case CREATE:
                return QueryTask.QUERY_ACTION.CREATE;
            case CLOSE:
                return QueryTask.QUERY_ACTION.CLOSE;
            case CANCEL:
                return QueryTask.QUERY_ACTION.CANCEL;
            case PLAN:
                return QueryTask.QUERY_ACTION.PLAN;
            default:
                throw new IllegalStateException("Cannot map " + method + " to a query action");
        }
    }
    
    public static QueryRequest.Method getQueryMethod(QueryTask.QUERY_ACTION action) {
        switch (action) {
            case NEXT:
                return QueryRequest.Method.NEXT;
            case CREATE:
                return QueryRequest.Method.CREATE;
            case CLOSE:
                return QueryRequest.Method.CLOSE;
            case CANCEL:
                return QueryRequest.Method.CANCEL;
            case PLAN:
                return QueryRequest.Method.PLAN;
            default:
                throw new IllegalStateException("Cannot map " + action + " to a query method");
        }
    }
    
    private void removeFromWorkQueue(UUID queryId) {
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
    
    private void interruptWork(UUID queryId) {
        synchronized (working) {
            for (Runnable action : working) {
                if (((ExecutorAction) action).getTaskKey().getQueryId().equals(queryId)) {
                    ((ExecutorAction) action).interrupt();
                }
            }
        }
    }
    
    @Override
    public void handleRemoteRequest(QueryRequest queryRequest) {
        handleRemoteRequest(queryRequest, false);
    }
    
    public void handleRemoteRequest(QueryRequest queryRequest, boolean wait) {
        final UUID queryId = UUID.fromString(queryRequest.getQueryId());
        final QueryTask.QUERY_ACTION action = getQueryAction(queryRequest.getMethod());
        // A close request waits for the current page to finish
        switch (action) {
            case CLOSE:
                removeFromWorkQueue(queryId);
                break;
            case CANCEL:
                removeFromWorkQueue(queryId);
                interruptWork(queryId);
                break;
            default: {
                // get the query states from the cache
                TaskStates taskStates = cache.getTaskStates(queryId);
                Map<TaskStates.TASK_STATE,Set<TaskKey>> taskStateMap = taskStates.getTaskStates();
                TaskKey taskKey = null;
                if (taskStateMap.containsKey(TaskStates.TASK_STATE.READY)) {
                    for (TaskKey key : taskStateMap.get(TaskStates.TASK_STATE.READY)) {
                        if (key.getAction() == getQueryAction(queryRequest.getMethod())) {
                            taskKey = key;
                            break;
                        }
                    }
                }
                
                if (taskKey != null) {
                    QueryTask task = cache.getTask(taskKey);
                    ExecutorAction runnable = null;
                    switch (action) {
                        case CREATE:
                            runnable = new Create(executorProperties, connector, cache, queues, queryLogicFactory, task);
                            break;
                        case NEXT:
                            runnable = new Next(executorProperties, connector, cache, queues, queryLogicFactory, task);
                            break;
                        case PLAN:
                            runnable = new Plan(executorProperties, connector, cache, queues, queryLogicFactory, task);
                            break;
                        default:
                            throw new UnsupportedOperationException(task.getTaskKey().toString());
                    }
                    
                    if (wait) {
                        runnable.run();
                    } else {
                        threadPool.execute(runnable);
                    }
                }
            }
        }
    }
}
