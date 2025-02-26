package datawave.microservice.query.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.Query;
import datawave.microservice.query.executor.action.ExecutorTask;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.messaging.QueryResultsListener;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.messaging.QueryResultsPublisher;
import datawave.microservice.query.messaging.Result;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.QueryState;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.QueryStorageLock;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.query.storage.TaskStates;

public class ShouldGenerateResultsTest {
    private static final Logger log = Logger.getLogger(ShouldGenerateResultsTest.class);
    
    @Test
    public void testShouldGenerateResults() {
        ExecutorProperties props = new ExecutorProperties();
        props.setAvailableResultsPageMultiplier(2.0f);
        TestQueryResultsManagerForSize queues = new TestQueryResultsManagerForSize();
        QueryStatus queryStatus = new QueryStatus();
        queryStatus.setQueryState(QueryStatus.QUERY_STATE.CREATE);
        queryStatus.setActiveNextCalls(1);
        queryStatus.setNumResultsGenerated(1);
        QueryStorageCache cache = new MockQueryStorageCache(queryStatus);
        TaskKey key = new TaskKey(1, new QueryKey("default", "queryid", "querylogic"));
        QueryCheckpoint checkpoint = new QueryCheckpoint(key.getQueryKey());
        QueryTask task = new QueryTask(key.getTaskId(), QueryRequest.Method.CREATE, checkpoint);
        TestExecutorShouldGenerateResults action = new TestExecutorShouldGenerateResults(props, queues, cache, task);
        
        // default positive case
        assertEquals(ExecutorTask.RESULTS_ACTION.GENERATE, action.shouldGenerateMoreResults(false, key, 10, 100));
        
        // closed, no next running
        queryStatus.setQueryState(QueryStatus.QUERY_STATE.CLOSE);
        queryStatus.setActiveNextCalls(0);
        assertEquals(ExecutorTask.RESULTS_ACTION.COMPLETE, action.shouldGenerateMoreResults(false, key, 10, 100));
        
        // closed but next running
        queryStatus.setActiveNextCalls(1);
        assertEquals(ExecutorTask.RESULTS_ACTION.GENERATE, action.shouldGenerateMoreResults(false, key, 10, 100));
        
        // canceled
        queryStatus.setQueryState(QueryStatus.QUERY_STATE.CANCEL);
        assertEquals(ExecutorTask.RESULTS_ACTION.COMPLETE, action.shouldGenerateMoreResults(false, key, 10, 100));
        
        // failed
        queryStatus.setQueryState(QueryStatus.QUERY_STATE.FAIL);
        assertEquals(ExecutorTask.RESULTS_ACTION.COMPLETE, action.shouldGenerateMoreResults(false, key, 10, 100));
        
        // max results reached
        queryStatus.setQueryState(QueryStatus.QUERY_STATE.CREATE);
        assertEquals(ExecutorTask.RESULTS_ACTION.COMPLETE, action.shouldGenerateMoreResults(false, key, 10, 1));
        assertEquals(ExecutorTask.RESULTS_ACTION.GENERATE, action.shouldGenerateMoreResults(false, key, 10, 0));
        
        // default negative case
        queues.setQueueSize(20);
        assertEquals(ExecutorTask.RESULTS_ACTION.PAUSE, action.shouldGenerateMoreResults(false, key, 10, 100));
        
        // exhaust results
        assertEquals(ExecutorTask.RESULTS_ACTION.GENERATE, action.shouldGenerateMoreResults(true, key, 10, 100));
        
        // queue size test
        queues.setQueueSize(19);
        assertEquals(ExecutorTask.RESULTS_ACTION.GENERATE, action.shouldGenerateMoreResults(false, key, 10, 100));
        
        // capped by max results
        queues.setQueueSize(10);
        queryStatus.setNumResultsGenerated(90);
        assertEquals(ExecutorTask.RESULTS_ACTION.PAUSE, action.shouldGenerateMoreResults(false, key, 10, 100));
        assertEquals(ExecutorTask.RESULTS_ACTION.GENERATE, action.shouldGenerateMoreResults(false, key, 10, 0));
    }
    
    public class TestQueryResultsManagerForSize implements QueryResultsManager {
        private int queueSize = 0;
        
        public void setQueueSize(int size) {
            this.queueSize = size;
        }
        
        @Override
        public QueryResultsListener createListener(String listenerId, String queueName) {
            return null;
        }
        
        @Override
        public QueryResultsPublisher createPublisher(String queryId) {
            return new QueryResultsPublisher() {
                @Override
                public boolean publish(Result result, long interval, TimeUnit timeUnit) {
                    sendMessage(queryId, result);
                    return true;
                }
                
                @Override
                public void close() throws IOException {
                    // do nothing
                }
            };
        }
        
        @Override
        public void deleteQuery(String queryId) {}
        
        @Override
        public void emptyQuery(String queryId) {}
        
        @Override
        public int getNumResultsRemaining(String queryId) {
            return queueSize;
        }
        
        private void sendMessage(String queryId, Result result) {}
    }
    
    public class TestExecutorShouldGenerateResults extends ExecutorTask {
        public TestExecutorShouldGenerateResults(ExecutorProperties executorProperties, QueryResultsManager queues, QueryStorageCache cache, QueryTask task) {
            super(null, executorProperties, null, null, null, null, cache, queues, null, null, null, null, task);
        }
        
        @Override
        public RESULTS_ACTION shouldGenerateMoreResults(boolean exhaust, TaskKey taskKey, int maxPageSize, long maxResults) {
            return super.shouldGenerateMoreResults(exhaust, taskKey, maxPageSize, maxResults);
        }
        
        @Override
        public boolean executeTask(QueryStatus status) throws Exception {
            return false;
        }
    }
    
    public static class MockQueryStorageCache implements QueryStorageCache {
        
        private QueryStatus status;
        
        public MockQueryStorageCache(QueryStatus status) {
            this.status = status;
        }
        
        @Override
        public TaskKey defineQuery(String queryPool, Query query, DatawaveUserDetails currentUser, Set<Authorizations> calculatedAuths, int count)
                        throws IOException {
            return null;
        }
        
        @Override
        public TaskKey createQuery(String queryPool, Query query, DatawaveUserDetails currentUser, Set<Authorizations> calculatedAuths, int count)
                        throws IOException {
            return null;
        }
        
        @Override
        public TaskKey planQuery(String queryPool, Query query, DatawaveUserDetails currentUser, Set<Authorizations> calculatedAuths) throws IOException {
            return null;
        }
        
        @Override
        public TaskKey predictQuery(String queryPool, Query query, DatawaveUserDetails currentUser, Set<Authorizations> calculatedAuths) throws IOException {
            return null;
        }
        
        @Override
        public QueryState getQueryState(String queryId) {
            return null;
        }
        
        @Override
        public QueryStatus getQueryStatus(String queryId) {
            return status;
        }
        
        @Override
        public List<QueryStatus> getQueryStatus() {
            return Collections.singletonList(status);
        }
        
        @Override
        public void updateQueryStatus(QueryStatus queryStatus) {
            status = queryStatus;
        }
        
        @Override
        public void updateQueryStatus(String queryId, QueryStatus.QUERY_STATE state) {
            
        }
        
        @Override
        public void updateCreateStage(String queryId, QueryStatus.CREATE_STAGE stage) {
            
        }
        
        @Override
        public void updateFailedQueryStatus(String queryId, Exception e) {
            
        }
        
        @Override
        public boolean updateTaskState(TaskKey taskKey, TaskStates.TASK_STATE state) {
            return false;
        }
        
        @Override
        public QueryStorageLock getQueryStatusLock(String queryId) {
            return null;
        }
        
        @Override
        public QueryStorageLock getTaskStatesLock(String queryId) {
            return null;
        }
        
        @Override
        public TaskStates getTaskStates(String queryId) {
            return null;
        }
        
        @Override
        public void updateTaskStates(TaskStates taskStates) {
            
        }
        
        @Override
        public QueryTask createTask(QueryRequest.Method action, QueryCheckpoint checkpoint) throws IOException {
            return null;
        }
        
        @Override
        public QueryTask getTask(TaskKey taskKey) {
            return null;
        }
        
        @Override
        public QueryTask checkpointTask(TaskKey taskKey, QueryCheckpoint checkpoint) {
            return null;
        }
        
        @Override
        public void deleteTask(TaskKey taskKey) throws IOException {
            
        }
        
        @Override
        public boolean deleteQuery(String queryId) throws IOException {
            return false;
        }
        
        @Override
        public void clear() throws IOException {
            
        }
        
        @Override
        public List<TaskKey> getTasks(String queryId) throws IOException {
            return null;
        }
    }
}
