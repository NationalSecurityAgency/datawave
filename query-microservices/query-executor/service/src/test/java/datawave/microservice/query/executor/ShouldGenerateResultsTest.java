package datawave.microservice.query.executor;

import datawave.microservice.query.executor.action.ExecutorTask;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.messaging.QueryResultsListener;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.messaging.QueryResultsPublisher;
import datawave.microservice.query.messaging.Result;
import datawave.microservice.query.storage.CachedQueryStatus;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.TaskKey;
import datawave.services.query.logic.QueryKey;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertEquals;

public class ShouldGenerateResultsTest {
    private static final Logger log = Logger.getLogger(ShouldGenerateResultsTest.class);
    
    @Test
    public void testShouldGenerateResults() {
        ExecutorProperties props = new ExecutorProperties();
        props.setAvailableResultsPageMultiplier(2.0f);
        TestQueryResultsManagerForSize queues = new TestQueryResultsManagerForSize();
        TestExecutorShouldGenerateResults action = new TestExecutorShouldGenerateResults(props, queues);
        TaskKey key = new TaskKey(1, new QueryKey("default", "queryid", "querylogic"));
        QueryStatus queryStatus = new QueryStatus();
        queryStatus.setQueryState(QueryStatus.QUERY_STATE.CREATE);
        queryStatus.setActiveNextCalls(1);
        queryStatus.setNumResultsGenerated(1);
        
        // default positive case
        assertEquals(ExecutorTask.RESULTS_ACTION.GENERATE, action.shouldGenerateMoreResults(false, key, 10, 100, queryStatus));
        
        // closed, no next running
        queryStatus.setQueryState(QueryStatus.QUERY_STATE.CLOSE);
        queryStatus.setActiveNextCalls(0);
        assertEquals(ExecutorTask.RESULTS_ACTION.COMPLETE, action.shouldGenerateMoreResults(false, key, 10, 100, queryStatus));
        
        // closed but next running
        queryStatus.setActiveNextCalls(1);
        assertEquals(ExecutorTask.RESULTS_ACTION.GENERATE, action.shouldGenerateMoreResults(false, key, 10, 100, queryStatus));
        
        // canceled
        queryStatus.setQueryState(QueryStatus.QUERY_STATE.CANCEL);
        assertEquals(ExecutorTask.RESULTS_ACTION.COMPLETE, action.shouldGenerateMoreResults(false, key, 10, 100, queryStatus));
        
        // failed
        queryStatus.setQueryState(QueryStatus.QUERY_STATE.FAIL);
        assertEquals(ExecutorTask.RESULTS_ACTION.COMPLETE, action.shouldGenerateMoreResults(false, key, 10, 100, queryStatus));
        
        // max results reached
        queryStatus.setQueryState(QueryStatus.QUERY_STATE.CREATE);
        assertEquals(ExecutorTask.RESULTS_ACTION.COMPLETE, action.shouldGenerateMoreResults(false, key, 10, 1, queryStatus));
        assertEquals(ExecutorTask.RESULTS_ACTION.GENERATE, action.shouldGenerateMoreResults(false, key, 10, 0, queryStatus));
        
        // default negative case
        queues.setQueueSize(20);
        assertEquals(ExecutorTask.RESULTS_ACTION.PAUSE, action.shouldGenerateMoreResults(false, key, 10, 100, queryStatus));
        
        // exhaust results
        assertEquals(ExecutorTask.RESULTS_ACTION.GENERATE, action.shouldGenerateMoreResults(true, key, 10, 100, queryStatus));
        
        // queue size test
        queues.setQueueSize(19);
        assertEquals(ExecutorTask.RESULTS_ACTION.GENERATE, action.shouldGenerateMoreResults(false, key, 10, 100, queryStatus));
        
        // capped by max results
        queues.setQueueSize(10);
        queryStatus.setNumResultsGenerated(90);
        assertEquals(ExecutorTask.RESULTS_ACTION.PAUSE, action.shouldGenerateMoreResults(false, key, 10, 100, queryStatus));
        assertEquals(ExecutorTask.RESULTS_ACTION.GENERATE, action.shouldGenerateMoreResults(false, key, 10, 0, queryStatus));
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
            return (result, interval, timeUnit) -> {
                sendMessage(queryId, result);
                return true;
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
        public TestExecutorShouldGenerateResults(ExecutorProperties executorProperties, QueryResultsManager queues) {
            super(null, executorProperties, null, null, null, null, null, queues, null, null, null, null, null);
        }
        
        @Override
        public RESULTS_ACTION shouldGenerateMoreResults(boolean exhaust, TaskKey taskKey, int maxPageSize, long maxResults, QueryStatus queryStatus) {
            return super.shouldGenerateMoreResults(exhaust, taskKey, maxPageSize, maxResults, queryStatus);
        }
        
        @Override
        public boolean executeTask(CachedQueryStatus status, Connector connector) throws Exception {
            return false;
        }
    }
}
