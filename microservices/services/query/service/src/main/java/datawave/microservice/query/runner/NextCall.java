package datawave.microservice.query.runner;

import static datawave.microservice.query.messaging.AcknowledgementCallback.Status.ACK;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.core.query.cache.ResultsPage;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.ResultPostprocessor;
import datawave.microservice.query.config.NextCallProperties;
import datawave.microservice.query.config.QueryExpirationProperties;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.messaging.QueryResultsListener;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.messaging.QueryResultsPublisher;
import datawave.microservice.query.messaging.Result;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.TaskStates;
import datawave.microservice.query.util.QueryStatusUpdateUtil;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetric;
import datawave.webservice.query.data.ObjectSizeOf;
import datawave.webservice.query.exception.QueryException;

public class NextCall implements Callable<ResultsPage<Object>> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final NextCallProperties nextCallProperties;
    private final QueryResultsManager queryResultsManager;
    private final QueryStorageCache queryStorageCache;
    private final String queryId;
    private final QueryStatusUpdateUtil queryStatusUpdateUtil;
    
    private volatile boolean canceled = false;
    private volatile Future<ResultsPage<Object>> future = null;
    
    private final long callTimeoutMillis;
    private final long shortCircuitCheckTimeMillis;
    private final long shortCircuitTimeoutMillis;
    private final long queryStartTimeMillis;
    private final long longRunningQueryTimeoutMillis;
    private final boolean allowLongRunningQueryEmptyPages;
    
    private final int userResultsPerPage;
    private final boolean maxResultsOverridden;
    private final long maxResultsOverride;
    private final long maxResults;
    private final int logicResultsPerPage;
    private final long logicBytesPerPage;
    private final long logicMaxWork;
    private final long maxResultsPerPage;
    private final ResultPostprocessor resultPostprocessor;
    
    private final List<Object> results = new LinkedList<>();
    private long pageSizeBytes;
    private long startTimeMillis;
    private long stopTimeMillis;
    private ResultsPage.Status status = ResultsPage.Status.COMPLETE;
    
    private long lastQueryStatusUpdateTime = 0L;
    private QueryStatus queryStatus;
    private long lastTaskStatesUpdateTime = 0L;
    private TaskStates taskStates;
    private long numResultsConsumed = 0L;
    private boolean returnIntermediateResult = false;
    
    private long hitMaxResultsTimeMillis = 0L;
    
    private BaseQueryMetric.Lifecycle lifecycle;
    
    private NextCall(Builder builder) {
        this.nextCallProperties = builder.nextCallProperties;
        this.queryResultsManager = builder.queryResultsManager;
        this.queryStorageCache = builder.queryStorageCache;
        this.queryId = builder.queryId;
        this.queryStatusUpdateUtil = builder.queryStatusUpdateUtil;
        
        QueryStatus status = getQueryStatus();
        long pageTimeoutMillis = TimeUnit.MINUTES.toMillis(status.getQuery().getPageTimeout());
        if (pageTimeoutMillis >= builder.expirationProperties.getPageMinTimeoutMillis()
                        && pageTimeoutMillis <= builder.expirationProperties.getPageMaxTimeoutMillis()) {
            callTimeoutMillis = pageTimeoutMillis;
            shortCircuitCheckTimeMillis = callTimeoutMillis / 2;
            shortCircuitTimeoutMillis = Math.round(0.97 * callTimeoutMillis);
        } else {
            callTimeoutMillis = builder.expirationProperties.getCallTimeoutMillis();
            shortCircuitCheckTimeMillis = builder.expirationProperties.getShortCircuitCheckTimeMillis();
            shortCircuitTimeoutMillis = builder.expirationProperties.getShortCircuitTimeoutMillis();
        }
        
        this.userResultsPerPage = status.getQuery().getPagesize();
        this.maxResultsOverridden = status.getQuery().isMaxResultsOverridden();
        this.maxResultsOverride = status.getQuery().getMaxResultsOverride();
        this.allowLongRunningQueryEmptyPages = status.isAllowLongRunningQueryEmptyPages();
        this.queryStartTimeMillis = status.getQueryStartMillis();
        this.longRunningQueryTimeoutMillis = builder.expirationProperties.getLongRunningQueryTimeoutMillis();
        
        this.logicResultsPerPage = builder.queryLogic.getMaxPageSize();
        this.logicBytesPerPage = builder.queryLogic.getPageByteTrigger();
        this.logicMaxWork = builder.queryLogic.getMaxWork();
        
        this.maxResultsPerPage = Math.min(userResultsPerPage, logicResultsPerPage);
        
        this.maxResults = builder.queryLogic.getResultLimit(status.getQuery());
        if (this.maxResults != builder.queryLogic.getMaxResults()) {
            log.info("Maximum results set to " + this.maxResults + " instead of default " + builder.queryLogic.getMaxResults() + ", user "
                            + status.getQuery().getUserDN() + " has a DN configured with a different limit");
        }
        
        this.resultPostprocessor = builder.queryLogic.getResultPostprocessor(getQueryStatus().getConfig());
    }
    
    @Override
    public ResultsPage<Object> call() throws Exception {
        startTimeMillis = System.currentTimeMillis();
        
        try (QueryResultsListener resultListener = queryResultsManager.createListener(UUID.randomUUID().toString(), queryId)) {
            // keep waiting for results until we're finished
            // Note: isFinished should be checked once per result
            while (!isFinished(queryId)) {
                Result result = resultListener.receive(nextCallProperties.getResultPollInterval(), nextCallProperties.getResultPollIntervalUnit());
                if (result != null) {
                    result.acknowledge(ACK);
                    
                    Object payload = result.getPayload();
                    if (payload != null) {
                        results.add(payload);
                        
                        resultPostprocessor.apply(results);
                        
                        numResultsConsumed++;
                        
                        if (logicBytesPerPage > 0) {
                            pageSizeBytes += ObjectSizeOf.Sizer.getObjectSize(payload);
                        }
                    } else {
                        log.debug("Null result encountered, no more results");
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.error("Encountered an error while fetching results from the listener", e);
            throw e;
        }
        
        // if we are aggregating results and we short-circuit,
        // return the intermediate result(s) to the queue
        if (returnIntermediateResult) {
            try (QueryResultsPublisher publisher = queryResultsManager.createPublisher(queryId)) {
                for (Object result : results) {
                    publisher.publish(new Result(UUID.randomUUID().toString(), result));
                }
            }
            results.clear();
        }
        
        // update some values for metrics
        stopTimeMillis = System.currentTimeMillis();
        if (lifecycle == null && !results.isEmpty()) {
            lifecycle = BaseQueryMetric.Lifecycle.RESULTS;
        }
        
        // update num results consumed for query status
        updateNumResultsConsumed();
        
        return new ResultsPage<>(results, status);
    }
    
    public void updateQueryMetric(BaseQueryMetric baseQueryMetric) {
        baseQueryMetric.addPageTime(results.size(), stopTimeMillis - startTimeMillis, startTimeMillis, stopTimeMillis);
        baseQueryMetric.setLifecycle(lifecycle);
    }
    
    private boolean isFinished(String queryId) throws QueryException {
        boolean finished = false;
        long callTimeMillis = System.currentTimeMillis() - startTimeMillis;
        QueryStatus queryStatus = getQueryStatus();
        
        // if the query state is FAILED, throw an exception up to the query management service with the failure message
        if (queryStatus.getQueryState() == QueryStatus.QUERY_STATE.FAIL) {
            log.error("Query [{}]: query failed, aborting next call. Cause: {}", queryId, queryStatus.getFailureMessage());
            
            throw new QueryException(queryStatus.getErrorCode(), queryStatus.getFailureMessage());
        }
        
        // 1) have we hit the user's results-per-page limit?
        if (results.size() >= userResultsPerPage) {
            log.info("Query [{}]: user requested max page size [{}] has been reached, aborting next call", queryId, userResultsPerPage);
            
            finished = true;
        }
        
        // 2) have we hit the query logic's results-per-page limit?
        if (!finished && logicResultsPerPage > 0 && results.size() >= logicResultsPerPage) {
            log.info("Query [{}]: query logic max page size [{}] has been reached, aborting next call", queryId, logicResultsPerPage);
            
            finished = true;
        }
        
        // 3) was this query canceled?
        if (!finished && (canceled || queryStatus.getQueryState() == QueryStatus.QUERY_STATE.CANCEL)) {
            log.info("Query [{}]: query cancelled, aborting next call", queryId);
            
            // no query metric lifecycle update - assumption is that the cancel call handled that
            // set to partial for now - if there are no results, it will switch to NONE later
            status = ResultsPage.Status.PARTIAL;
            
            finished = true;
        }
        
        // 4) have we hit the query logic's bytes-per-page limit?
        if (!finished && logicBytesPerPage > 0 && pageSizeBytes >= logicBytesPerPage) {
            log.info("Query [{}]: query logic max page byte size has been reached, aborting next call", queryId);
            
            status = ResultsPage.Status.PARTIAL;
            
            finished = true;
        }
        
        // 5) have we retrieved all of the results?
        if (!finished && (queryStatus.getCreateStage() == QueryStatus.CREATE_STAGE.RESULTS) && !getTaskStates().hasUnfinishedTasks()) {
            
            // update the number of results consumed
            queryStatus = updateNumResultsConsumed();
            
            // how many results do the query services think are left
            long queryResultsRemaining = queryStatus.getNumResultsGenerated() - queryStatus.getNumResultsConsumed();
            
            // check to see if the number of results consumed is >= to the number of results generated
            if (queryResultsRemaining < 0) {
                log.warn("Query [{}]: The number of results consumed [{}] exceeds the number of results generated [{}]", queryId,
                                queryStatus.getNumResultsConsumed(), queryStatus.getNumResultsGenerated());
            }
            
            // how many results does the broker think are left
            long brokerResultsRemaining = queryResultsManager.getNumResultsRemaining(queryId);
            
            log.info("All tasks appear to be completed for " + queryId + " with " + queryResultsRemaining + " yet to be retrieved and " + brokerResultsRemaining
                            + " left in broker");
            
            // if the broker thinks there are not results left, we may be done
            if (brokerResultsRemaining == 0) {
                
                // if the query service thinks there are no results left, we are done
                // we can have negative results remaining if we consumed duplicate records
                if (queryResultsRemaining <= 0) {
                    log.info("Query [{}]: all query tasks complete, and all results retrieved, aborting next call", queryId);
                    
                    status = ResultsPage.Status.PARTIAL;
                    
                    finished = true;
                }
                // if the query services think there are results left, we may need to wait
                // this can happen if messages are in flux with the message broker due to nacking
                else {
                    // if we aren't in a max results waiting period, start waiting
                    if (hitMaxResultsTimeMillis == 0) {
                        hitMaxResultsTimeMillis = System.currentTimeMillis();
                    }
                    // if we are finished waiting, we are done
                    else if (System.currentTimeMillis() >= (hitMaxResultsTimeMillis + nextCallProperties.getMaxResultsTimeoutMillis())) {
                        log.info("Query [{}]: all query tasks complete, but timed out waiting for all results to be retrieved, aborting next call", queryId);
                        
                        status = ResultsPage.Status.PARTIAL;
                        
                        finished = true;
                    }
                }
            }
        }
        
        // 6) have we hit the max results (or the max results override)?
        if (!finished) {
            long numResultsReturned = queryStatus.getNumResultsReturned();
            long numResults = numResultsReturned + results.size();
            if (this.maxResultsOverridden) {
                if (maxResultsOverride >= 0 && numResults >= maxResultsOverride) {
                    log.info("Query [{}]: max results override has been reached, aborting next call", queryId);
                    
                    lifecycle = QueryMetric.Lifecycle.MAXRESULTS;
                    
                    status = ResultsPage.Status.PARTIAL;
                    
                    finished = true;
                }
            } else if (maxResults >= 0 && numResults >= maxResults) {
                log.info("Query [{}]: logic max results has been reached, aborting next call", queryId);
                
                lifecycle = QueryMetric.Lifecycle.MAXRESULTS;
                
                status = ResultsPage.Status.PARTIAL;
                
                finished = true;
            }
        }
        
        // 7) have we reached the "max work" limit? (i.e. next count + seek count)
        if (!finished && logicMaxWork > 0 && (queryStatus.getNextCount() + queryStatus.getSeekCount()) >= logicMaxWork) {
            log.info("Query [{}]: logic max work has been reached, aborting next call", queryId);
            
            lifecycle = BaseQueryMetric.Lifecycle.MAXWORK;
            
            status = ResultsPage.Status.PARTIAL;
            
            finished = true;
        }
        
        // 8) are we going to timeout before getting a full page? if so, return partial results
        if (!finished && shortCircuitTimeout(callTimeMillis)) {
            log.info("Query [{}]: logic max expire before page is full, returning existing results: {} of {} results in {}ms", queryId, results.size(),
                            maxResultsPerPage, callTimeMillis);
            
            status = ResultsPage.Status.PARTIAL;
            
            finished = true;
        }
        
        // 9) have we been in this next call too long?
        if (!finished && callExpiredTimeout(callTimeMillis)) {
            log.info("Query [{}]: max call time reached, returning existing results: {} of {} results in {}ms", queryId, results.size(), maxResultsPerPage,
                            callTimeMillis);
            
            lifecycle = BaseQueryMetric.Lifecycle.NEXTTIMEOUT;
            
            status = ResultsPage.Status.PARTIAL;
            
            finished = true;
        }
        
        return finished;
    }
    
    private QueryStatus updateNumResultsConsumed() {
        if (numResultsConsumed > 0) {
            try {
                queryStatus = queryStatusUpdateUtil.lockedUpdate(queryId, queryStatus1 -> {
                    queryStatus1.incrementNumResultsConsumed(numResultsConsumed);
                    numResultsConsumed = 0;
                });
                lastQueryStatusUpdateTime = System.currentTimeMillis();
            } catch (Exception e) {
                log.warn("Unable to update number of results consumed for query {}", queryId);
            }
        }
        return queryStatus;
    }
    
    private boolean shortCircuitTimeout(long callTimeMillis) {
        boolean timeout = false;
        
        // return prematurely if we have at least 1 result, and we aren't aggregating results
        if (!results.isEmpty() && !queryStatus.getConfig().isReduceResults()) {
            // if after the page size short circuit check time
            if (callTimeMillis >= shortCircuitCheckTimeMillis) {
                float percentTimeComplete = (float) callTimeMillis / (float) (callTimeoutMillis);
                float percentResultsComplete = (float) results.size() / (float) maxResultsPerPage;
                // if the percent results complete is less than the percent time complete, then break out
                if (percentResultsComplete < percentTimeComplete) {
                    timeout = true;
                }
            }
            
            // if after the page short circuit timeout, then break out
            if (callTimeMillis >= shortCircuitTimeoutMillis) {
                timeout = true;
            }
        } else if (allowLongRunningQueryEmptyPages) {
            // in the case of allowing long-running query timeouts, we can return an empty page
            // before the query times out. However we can only allow this query to run so long...
            if (callTimeMillis >= shortCircuitTimeoutMillis) {
                if ((System.currentTimeMillis() - queryStartTimeMillis) < longRunningQueryTimeoutMillis) {
                    timeout = true;
                    
                    // if we are aggregating results, return the intermediate
                    // result to the queue before returning a blank page
                    returnIntermediateResult = queryStatus.getConfig().isReduceResults();
                }
            }
        }
        
        return timeout;
    }
    
    private boolean callExpiredTimeout(long callTimeMillis) {
        return callTimeMillis >= callTimeoutMillis;
    }
    
    private QueryStatus getQueryStatus() {
        if (queryStatus == null || isQueryStatusExpired()) {
            lastQueryStatusUpdateTime = System.currentTimeMillis();
            queryStatus = queryStorageCache.getQueryStatus(queryId);
        }
        return queryStatus;
    }
    
    private TaskStates getTaskStates() {
        if (taskStates == null || isTaskStatesExpired()) {
            lastTaskStatesUpdateTime = System.currentTimeMillis();
            taskStates = queryStorageCache.getTaskStates(queryId);
        }
        return taskStates;
    }
    
    private boolean isQueryStatusExpired() {
        return (System.currentTimeMillis() - lastQueryStatusUpdateTime) > nextCallProperties.getStatusUpdateIntervalMillis();
    }
    
    private boolean isTaskStatesExpired() {
        return (System.currentTimeMillis() - lastTaskStatesUpdateTime) > nextCallProperties.getStatusUpdateIntervalMillis();
    }
    
    public boolean isCanceled() {
        return canceled;
    }
    
    public void cancel() {
        this.canceled = true;
    }
    
    public Future<ResultsPage<Object>> getFuture() {
        return future;
    }
    
    public void setFuture(Future<ResultsPage<Object>> future) {
        this.future = future;
    }
    
    public BaseQueryMetric.Lifecycle getLifecycle() {
        return lifecycle;
    }
    
    public static class Builder {
        private NextCallProperties nextCallProperties;
        private QueryExpirationProperties expirationProperties;
        private QueryResultsManager queryResultsManager;
        private QueryStorageCache queryStorageCache;
        private String queryId;
        private QueryStatusUpdateUtil queryStatusUpdateUtil;
        private QueryLogic<?> queryLogic;
        
        public Builder setQueryProperties(QueryProperties queryProperties) {
            this.nextCallProperties = queryProperties.getNextCall();
            this.expirationProperties = queryProperties.getExpiration();
            return this;
        }
        
        public Builder setNextCallProperties(NextCallProperties nextCallProperties) {
            this.nextCallProperties = nextCallProperties;
            return this;
        }
        
        public Builder setExpirationProperties(QueryExpirationProperties expirationProperties) {
            this.expirationProperties = expirationProperties;
            return this;
        }
        
        public Builder setResultsQueueManager(QueryResultsManager queryResultsManager) {
            this.queryResultsManager = queryResultsManager;
            return this;
        }
        
        public Builder setQueryStorageCache(QueryStorageCache queryStorageCache) {
            this.queryStorageCache = queryStorageCache;
            return this;
        }
        
        public Builder setQueryId(String queryId) {
            this.queryId = queryId;
            return this;
        }
        
        public Builder setQueryStatusUpdateUtil(QueryStatusUpdateUtil queryStatusUpdateUtil) {
            this.queryStatusUpdateUtil = queryStatusUpdateUtil;
            return this;
        }
        
        public Builder setQueryLogic(QueryLogic<?> queryLogic) {
            this.queryLogic = queryLogic;
            return this;
        }
        
        public NextCall build() {
            return new NextCall(this);
        }
    }
}
