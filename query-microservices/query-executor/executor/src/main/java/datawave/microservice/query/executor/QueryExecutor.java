package datawave.microservice.query.executor;

import datawave.microservice.common.storage.QueryCheckpoint;
import datawave.microservice.common.storage.QueryStatus;
import datawave.microservice.common.storage.QueryStorageCache;
import datawave.microservice.common.storage.QueryStorageLock;
import datawave.microservice.common.storage.QueryTask;
import datawave.microservice.common.storage.QueryTaskNotification;
import datawave.microservice.common.storage.TaskKey;
import datawave.microservice.common.storage.TaskLockException;
import datawave.microservice.common.storage.TaskStates;
import datawave.microservice.query.logic.CheckpointableQueryLogic;
import datawave.microservice.query.logic.QueryLogic;
import datawave.microservice.query.logic.QueryLogicFactory;
import org.apache.accumulo.core.client.Connector;
import org.springframework.security.core.parameters.P;

import java.io.IOException;

/**
 * This class holds the business logic for handling a task notification
 *
 * TODO: Query Metrics TODO: Query Predictions
 **/
public class QueryExecutor {
    
    private final Connector connector;
    private final QueryStorageCache cache;
    private final QueryLogicFactory queryLogicFactory;
    
    public QueryExecutor(Connector connector, QueryStorageCache cache, QueryLogicFactory queryLogicFactory) {
        this.cache = cache;
        this.connector = connector;
        this.queryLogicFactory = queryLogicFactory;
    }
    
    public boolean handleTask(QueryTaskNotification taskNotification) throws IOException, InterruptedException {
        boolean gotLock = false;
        boolean taskComplete = false;
        QueryCheckpoint checkpoint = null;
        TaskKey taskKey = taskNotification.getTaskKey();
        QueryStorageLock lock = cache.getTaskStatesLock(taskKey.getQueryId());
        try {
            // pull the task out of the cache, locking it in the process
            // TODO: how long do we wait? Do we want to set a lease time in case we die somehow?
            QueryTask task = cache.getTask(taskKey, 100);
            if (task != null) {
                
                // check the states to see if we can run this now
                try {
                    lock.lock();
                    TaskStates states = cache.getTaskStates(taskKey.getQueryId());
                    if (states.setState(taskKey, TaskStates.TASK_STATE.RUNNING)) {
                        cache.updateTaskStates(states);
                        gotLock = true;
                    }
                } finally {
                    lock.unlock();
                }
                
                // only proceed if we got the lock
                if (gotLock) {
                    // pull the query from the cache
                    QueryStatus queryStatus = cache.getQueryStatus(taskKey.getQueryId());
                    QueryLogic<?> queryLogic = null;
                    switch (task.getAction()) {
                        case CREATE:
                        case DEFINE:
                            queryLogic = queryLogicFactory.getQueryLogic(queryStatus.getQuery().getQueryLogicName());
                            queryLogic.initialize(connector, queryStatus.getQuery(), queryStatus.getAuthorizations());
                            if (queryLogic instanceof CheckpointableQueryLogic && ((CheckpointableQueryLogic) queryLogic).isCheckpointable()) {
                                CheckpointableQueryLogic cpQueryLogic = (CheckpointableQueryLogic) queryLogic;
                                
                            } else {
                                
                            }
                            taskComplete = true;
                            break;
                        case NEXT:
                            queryLogic = queryLogicFactory.getQueryLogic(queryStatus.getQuery().getQueryLogicName());
                            queryLogic.initialize(connector, queryStatus.getQuery(), queryStatus.getAuthorizations());
                            taskComplete = true;
                            break;
                        case CLOSE:
                            taskComplete = true;
                            break;
                        case TEST:
                            // we can ignore this one
                        default:
                            throw new IllegalStateException("Unknown task action: " + task.getAction() + " for " + taskKey);
                    }
                }
            }
        } catch (TaskLockException tle) {
            // somebody is already processing this one
        } finally {
            if (gotLock) {
                TaskStates.TASK_STATE newState = TaskStates.TASK_STATE.READY;
                if (taskComplete) {
                    cache.deleteTask(taskKey);
                    newState = TaskStates.TASK_STATE.COMPLETED;
                } else if (checkpoint != null) {
                    cache.checkpointTask(taskKey, checkpoint);
                } else {
                    cache.getTaskLock(taskKey).unlock();
                }
                try {
                    lock.lock();
                    TaskStates states = cache.getTaskStates(taskKey.getQueryId());
                    if (states.setState(taskKey, newState)) {
                        cache.updateTaskStates(states);
                    }
                } finally {
                    lock.unlock();
                }
                return true;
            } else {
                return false;
            }
        }
    }
}
