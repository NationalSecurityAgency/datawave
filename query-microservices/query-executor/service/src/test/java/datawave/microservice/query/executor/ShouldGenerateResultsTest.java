package datawave.microservice.query.executor;

import datawave.microservice.query.executor.action.ExecutorAction;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.CachedQueryStatus;
import datawave.microservice.query.storage.QueryQueueListener;
import datawave.microservice.query.storage.QueryQueueManager;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.Result;
import datawave.microservice.query.storage.TaskKey;
import datawave.services.query.logic.QueryKey;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShouldGenerateResultsTest {
    private static final Logger log = Logger.getLogger(ShouldGenerateResultsTest.class);
    
    @Test
    public void testShouldGenerateResults() {
        ExecutorProperties props = new ExecutorProperties();
        props.setAvailableResultsPageMultiplier(2.0f);
        TestQueryQueueManagerForSize queues = new TestQueryQueueManagerForSize();
        TestExecutorShouldGenerateResults action = new TestExecutorShouldGenerateResults(props, queues);
        TaskKey key = new TaskKey("taskid", QueryRequest.Method.NEXT, new QueryKey("default", "queryid", "querylogic"));
        QueryStatus queryStatus = new QueryStatus();
        queryStatus.setQueryState(QueryStatus.QUERY_STATE.CREATED);
        queryStatus.setActiveNextCalls(1);
        queryStatus.setNumResultsGenerated(1);
        
        // default positive case
        assertTrue(action.shouldGenerateMoreResults(false, key, 10, 100, queryStatus));
        
        // closed, no next running
        queryStatus.setQueryState(QueryStatus.QUERY_STATE.CLOSED);
        queryStatus.setActiveNextCalls(0);
        assertFalse(action.shouldGenerateMoreResults(false, key, 10, 100, queryStatus));
        
        // closed but next running
        queryStatus.setActiveNextCalls(1);
        assertTrue(action.shouldGenerateMoreResults(false, key, 10, 100, queryStatus));
        
        // canceled
        queryStatus.setQueryState(QueryStatus.QUERY_STATE.CANCELED);
        assertFalse(action.shouldGenerateMoreResults(false, key, 10, 100, queryStatus));
        
        // failed
        queryStatus.setQueryState(QueryStatus.QUERY_STATE.FAILED);
        assertFalse(action.shouldGenerateMoreResults(false, key, 10, 100, queryStatus));
        
        // max results reached
        queryStatus.setQueryState(QueryStatus.QUERY_STATE.CREATED);
        assertFalse(action.shouldGenerateMoreResults(false, key, 10, 1, queryStatus));
        assertTrue(action.shouldGenerateMoreResults(false, key, 10, 0, queryStatus));
        
        // default negative case
        queues.setQueueSize(20);
        assertFalse(action.shouldGenerateMoreResults(false, key, 10, 100, queryStatus));
        
        // exhaust results
        assertTrue(action.shouldGenerateMoreResults(true, key, 10, 100, queryStatus));
        
        // queue size test
        queues.setQueueSize(19);
        assertTrue(action.shouldGenerateMoreResults(false, key, 10, 100, queryStatus));
        
        // capped by max results
        queues.setQueueSize(10);
        queryStatus.setNumResultsGenerated(90);
        assertFalse(action.shouldGenerateMoreResults(false, key, 10, 100, queryStatus));
        assertTrue(action.shouldGenerateMoreResults(false, key, 10, 0, queryStatus));
    }
    
    public class TestQueryQueueManagerForSize implements QueryQueueManager {
        private int queueSize = 0;
        
        public void setQueueSize(int size) {
            this.queueSize = size;
        }
        
        @Override
        public QueryQueueListener createListener(String listenerId, String queueName) {
            return null;
        }
        
        @Override
        public void ensureQueueCreated(String queryId) {}
        
        @Override
        public void deleteQueue(String queryId) {}
        
        @Override
        public void emptyQueue(String queryId) {}
        
        @Override
        public int getQueueSize(String queryId) {
            return queueSize;
        }
        
        @Override
        public void sendMessage(String queryId, Result result) {}
    }
    
    public class TestExecutorShouldGenerateResults extends ExecutorAction {
        public TestExecutorShouldGenerateResults(ExecutorProperties executorProperties, QueryQueueManager queues) {
            super(null, executorProperties, null, null, null, null, null, queues, null, null, null, null, null);
        }
        
        @Override
        public boolean shouldGenerateMoreResults(boolean exhaust, TaskKey taskKey, int maxPageSize, long maxResults, QueryStatus queryStatus) {
            return super.shouldGenerateMoreResults(exhaust, taskKey, maxPageSize, maxResults, queryStatus);
        }
        
        @Override
        public boolean executeTask(CachedQueryStatus status, Connector connector) throws Exception {
            return false;
        }
    }
}
