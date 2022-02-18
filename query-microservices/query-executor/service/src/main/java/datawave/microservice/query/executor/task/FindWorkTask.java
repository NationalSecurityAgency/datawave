package datawave.microservice.query.executor.task;

import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.QueryStorageLock;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.query.storage.TaskStates;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.Callable;

public class FindWorkTask implements Callable<Void> {
    private Logger log = Logger.getLogger(FindWorkTask.class);
    protected final QueryStorageCache cache;
    protected final QueryExecutor executor;
    // a cache of the last N close cancel queries that we handled to avoid excessive executor executions and subsequent query cache hits
    protected final CloseCancelCache closeCancelCache;
    
    private final String originService;
    private final String destinationService;
    
    public FindWorkTask(QueryStorageCache cache, QueryExecutor executor, CloseCancelCache closeCancelCache, String originService, String destinationService) {
        this.cache = cache;
        this.executor = executor;
        this.closeCancelCache = closeCancelCache;
        this.originService = originService;
        this.destinationService = destinationService;
    }
    
    @Override
    public Void call() throws Exception {
        log.info("Executor status: " + executor.getStatus());
        for (QueryStatus queryStatus : cache.getQueryStatus()) {
            String queryId = queryStatus.getQueryKey().getQueryId();
            switch (queryStatus.getQueryState()) {
                case CLOSE:
                    if (closeCancelCache.add(queryId)) {
                        log.debug("Closing " + queryId);
                        executor.handleRemoteRequest(QueryRequest.close(queryId), originService, destinationService);
                        recoverOrphanedTasks(queryId, TaskStates.TASK_STATE.FAILED, true);
                    }
                    break;
                case CANCEL:
                    if (closeCancelCache.add(queryId)) {
                        log.debug("Cancelling " + queryId);
                        executor.handleRemoteRequest(QueryRequest.cancel(queryId), originService, destinationService);
                        recoverOrphanedTasks(queryId, TaskStates.TASK_STATE.FAILED, true);
                    }
                    break;
                case CREATE:
                    // Should we create a new thread to handle the remote request instead so that we can return immediately?
                    // even if the next task is to plan, this will take care of it
                    switch (queryStatus.getCreateStage()) {
                        case CREATE:
                        case PLAN:
                            // recover orphaned tasks
                            recoverOrphanedTasks(queryId, TaskStates.TASK_STATE.READY);
                            log.debug("Creating " + queryId);
                            executor.handleRemoteRequest(QueryRequest.create(queryId), originService, destinationService);
                            break;
                        case TASK:
                            // recover orphaned tasks, however create tasks should be failed as we were already creating tasks
                            recoverOrphanedTasks(queryId, TaskStates.TASK_STATE.READY,
                                            Collections.singletonMap(QueryRequest.Method.CREATE, TaskStates.TASK_STATE.FAILED));
                            log.debug("Nexting " + queryId);
                            executor.handleRemoteRequest(QueryRequest.next(queryId), originService, destinationService);
                            break;
                        case RESULTS:
                            // recover orphaned tasks, however create tasks should be completed as all tasks have already been created
                            recoverOrphanedTasks(queryId, TaskStates.TASK_STATE.READY,
                                            Collections.singletonMap(QueryRequest.Method.CREATE, TaskStates.TASK_STATE.COMPLETED));
                            log.debug("Nexting " + queryId);
                            executor.handleRemoteRequest(QueryRequest.next(queryId), originService, destinationService);
                            break;
                    }
                    break;
                case PLAN:
                    log.debug("Planning " + queryId);
                    recoverOrphanedTasks(queryId, TaskStates.TASK_STATE.READY);
                    // Should we create a new thread to handle the remote request instead so that we can return immediately?
                    // even if the next task is to plan, this will take care of it
                    executor.handleRemoteRequest(QueryRequest.plan(queryId), originService, destinationService);
                    break;
                case PREDICT:
                    log.debug("Predicting " + queryId);
                    recoverOrphanedTasks(queryId, TaskStates.TASK_STATE.READY);
                    // Should we create a new thread to handle the remote request instead so that we can return immediately?
                    // even if the next task is to plan, this will take care of it
                    executor.handleRemoteRequest(QueryRequest.predict(queryId), originService, destinationService);
                    break;
                case DEFINE:
                case FAIL:
                    // noop
                    break;
            }
        }
        return null;
    }
    
    /**
     * For the specified query id, find tasks that are orphaned and reset their state
     *
     * @param queryId
     *            The query id
     * @param state
     *            The state to reset orphaned tasks to
     */
    public void recoverOrphanedTasks(String queryId, TaskStates.TASK_STATE state) {
        recoverOrphanedTasks(queryId, state, false);
    }
    
    /**
     * For the specified query id, find tasks that are orphaned and reset their state
     *
     * @param queryId
     *            The query id
     * @param state
     *            The state to reset orphaned tasks to
     * @param all
     *            If true then all of the running tasks will be addressed instead of maxOrphanedTasksToCheck
     */
    public void recoverOrphanedTasks(String queryId, TaskStates.TASK_STATE state, boolean all) {
        recoverOrphanedTasks(queryId, state, Collections.emptyMap(), all);
    }
    
    /**
     * For the specified query id, find tasks that are orphaned and reset their state
     *
     * @param queryId
     *            The query id
     * @param state
     *            The state to reset orphaned tasks to
     * @param overrides
     *            The state to reset orphaned tasks for a specific method
     */
    public void recoverOrphanedTasks(String queryId, TaskStates.TASK_STATE state, Map<QueryRequest.Method,TaskStates.TASK_STATE> overrides) {
        recoverOrphanedTasks(queryId, state, overrides, false);
    }
    
    /**
     * For the specified query id, find tasks that are orphaned and reset their state
     * 
     * @param queryId
     *            The query id
     * @param state
     *            The state to reset orphaned tasks to
     * @param overrides
     *            The state to reset orphaned tasks for a specific method
     * @param all
     *            If true then all of the running tasks will be addressed instead of maxOrphanedTasksToCheck
     */
    public void recoverOrphanedTasks(String queryId, TaskStates.TASK_STATE state, Map<QueryRequest.Method,TaskStates.TASK_STATE> overrides, boolean all) {
        QueryStorageLock lock = cache.getTaskStatesLock(queryId);
        lock.lock();
        try {
            // get the query states from the cache
            TaskStates taskStates = cache.getTaskStates(queryId);
            
            if (taskStates != null) {
                log.debug("Searching for orphaned tasks for " + queryId);
                
                for (TaskKey taskKey : taskStates.getTasksForState(TaskStates.TASK_STATE.RUNNING,
                                all ? -1 : executor.getExecutorProperties().getMaxOrphanedTasksToCheck())) {
                    QueryTask task = cache.getTask(taskKey);
                    if ((System.currentTimeMillis() - task.getLastUpdatedMillis()) > executor.getExecutorProperties().getOrphanThresholdMs()) {
                        if (overrides.containsKey(task.getAction())) {
                            log.info("Resetting orphaned task " + taskKey.getTaskId() + " for " + queryId + " to " + overrides.get(task.getAction()));
                            taskStates.setState(taskKey.getTaskId(), overrides.get(task.getAction()));
                        } else {
                            log.info("Resetting orphaned task " + taskKey.getTaskId() + " for " + queryId + " to " + state);
                            taskStates.setState(taskKey.getTaskId(), state);
                        }
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * A class that provides quick containership test via a hashset and keeps a max size by aging off the oldest elements as new ones are added
     */
    public static class CloseCancelCache extends HashSet<String> {
        private final CircularFifoQueue<String> list;
        
        public CloseCancelCache(int maxSize) {
            list = new CircularFifoQueue<>(maxSize);
        }
        
        @Override
        public boolean add(String t) {
            if (super.add(t)) {
                if (list.size() == list.maxSize()) {
                    super.remove(list.remove());
                }
                list.add(t);
                return true;
            } else {
                return false;
            }
        }
        
        @Override
        public Iterator<String> iterator() {
            return new Iterator<String>() {
                private final Iterator<String> iterator = list.iterator();
                private String lastReturned = null;
                
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }
                
                @Override
                public String next() {
                    return iterator.next();
                }
                
                @Override
                public void remove() {
                    if (lastReturned == null) {
                        throw new IllegalStateException();
                    }
                    iterator.remove();
                    CloseCancelCache.this.superRemove(lastReturned);
                }
            };
        }
        
        private boolean superRemove(Object o) {
            return super.remove(o);
        }
        
        @Override
        public boolean remove(Object o) {
            if (superRemove(o)) {
                list.remove(o);
                return true;
            }
            return false;
        }
        
        @Override
        public void clear() {
            super.clear();
            list.clear();
        }
        
        @Override
        public Object clone() {
            CloseCancelCache set = new CloseCancelCache(list.maxSize());
            set.addAll(this);
            return set;
        }
        
        @Override
        public Spliterator<String> spliterator() {
            return list.spliterator();
        }
    }
}
