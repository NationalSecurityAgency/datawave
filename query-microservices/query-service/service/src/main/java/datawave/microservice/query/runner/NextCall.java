package datawave.microservice.query.runner;

import datawave.microservice.query.config.NextCallProperties;
import datawave.microservice.query.config.QueryExpirationProperties;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.logic.QueryLogic;
import datawave.microservice.query.storage.QueryQueueListener;
import datawave.microservice.query.storage.QueryQueueManager;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.Result;
import datawave.microservice.query.storage.TaskStates;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetric;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.data.ObjectSizeOf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class NextCall implements Callable<ResultsPage<Object>> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    private final NextCallProperties nextCallProperties;
    private final QueryQueueManager queryQueueManager;
    private final QueryStorageCache queryStorageCache;
    private final String queryId;
    
    private volatile boolean canceled = false;
    private volatile Future<ResultsPage<Object>> future = null;
    
    private final long callTimeoutMillis;
    private final long shortCircuitCheckTimeMillis;
    private final long shortCircuitTimeoutMillis;
    
    private final int userResultsPerPage;
    private final boolean maxResultsOverridden;
    private final long maxResultsOverride;
    private final long maxResults;
    private final int logicResultsPerPage;
    private final long logicBytesPerPage;
    private final long logicMaxWork;
    private final long maxResultsPerPage;
    
    private final List<Object> results = new LinkedList<>();
    private long pageSizeBytes;
    private long startTimeMillis;
    private long stopTimeMillis;
    private ResultsPage.Status status = ResultsPage.Status.COMPLETE;
    
    private long lastQueryStatusUpdateTime = 0L;
    private QueryStatus queryStatus;
    private long lastTaskStatesUpdateTime = 0L;
    private TaskStates taskStates;
    private boolean tasksFinished = false;
    private long prevResultCount = 0L;
    
    private BaseQueryMetric.Lifecycle lifecycle;
    
    private NextCall(Builder builder) {
        this.nextCallProperties = builder.nextCallProperties;
        this.queryQueueManager = builder.queryQueueManager;
        this.queryStorageCache = builder.queryStorageCache;
        this.queryId = builder.queryId;
        
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
        
        this.logicResultsPerPage = builder.queryLogic.getMaxPageSize();
        this.logicBytesPerPage = builder.queryLogic.getPageByteTrigger();
        this.logicMaxWork = builder.queryLogic.getMaxWork();
        
        this.maxResultsPerPage = Math.min(userResultsPerPage, logicResultsPerPage);
        
        this.maxResults = builder.queryLogic.getResultLimit(status.getQuery().getDnList());
        if (this.maxResults != builder.queryLogic.getMaxResults()) {
            log.info("Maximum results set to " + this.maxResults + " instead of default " + builder.queryLogic.getMaxResults() + ", user "
                            + status.getQuery().getUserDN() + " has a DN configured with a different limit");
        }
    }
    
    @Override
    public ResultsPage<Object> call() {
        startTimeMillis = System.currentTimeMillis();
        
        QueryQueueListener resultListener = queryQueueManager.createListener(UUID.randomUUID().toString(), queryId);
        
        // keep waiting for results until we're finished
        while (!isFinished(queryId)) {
            Message<Result> message = resultListener.receive(nextCallProperties.getResultPollIntervalMillis());
            if (message != null) {
                Object result = message.getPayload().getPayload();
                
                // have to check to make sure we haven't reached the page size
                if (!isFinished(queryId)) {
                    if (result != null) {
                        results.add(result);
                        
                        if (logicBytesPerPage > 0) {
                            pageSizeBytes += ObjectSizeOf.Sizer.getObjectSize(result);
                        }
                    } else {
                        log.debug("Null result encountered, no more results");
                        break;
                    }
                }
            }
        }
        
        // stop the result listener
        resultListener.stop();
        
        // update some values for metrics
        stopTimeMillis = System.currentTimeMillis();
        if (lifecycle == null && !results.isEmpty()) {
            lifecycle = BaseQueryMetric.Lifecycle.RESULTS;
        }
        
        return new ResultsPage<>(results, status);
    }
    
    public void updateQueryMetric(BaseQueryMetric baseQueryMetric) {
        baseQueryMetric.addPageTime(results.size(), stopTimeMillis - startTimeMillis, startTimeMillis, stopTimeMillis);
        baseQueryMetric.setLifecycle(lifecycle);
    }
    
    private boolean isFinished(String queryId) {
        boolean finished = false;
        long callTimeMillis = System.currentTimeMillis() - startTimeMillis;
        final QueryStatus queryStatus = getQueryStatus();
        
        // 1) have we hit the user's results-per-page limit?
        if (results.size() >= userResultsPerPage) {
            log.info("Query [{}]: user requested max page size has been reached, aborting next call", queryId);
            
            finished = true;
        }
        
        // 2) have we hit the query logic's results-per-page limit?
        if (!finished && logicResultsPerPage > 0 && results.size() >= logicResultsPerPage) {
            log.info("Query [{}]: query logic max page size has been reached, aborting next call", queryId);
            
            finished = true;
        }
        
        // 3) was this query canceled?
        if (!finished && (canceled || queryStatus.getQueryState() == QueryStatus.QUERY_STATE.CANCELED)) {
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
        if (!finished && !getTaskStates().hasUnfinishedTasks()) {
            // if all tasks have completed (or failed), start keeping track of the result count
            if (tasksFinished) {
                // if we stop getting results, we are done
                if (prevResultCount == results.size()) {
                    log.info("Query [{}]: all query tasks complete, and all results retrieved, aborting next call", queryId);
                    
                    status = ResultsPage.Status.PARTIAL;
                    
                    finished = true;
                }
            } else {
                tasksFinished = true;
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
        
        // 9) have we been in this next call too long? if so, return
        if (!finished && callExpiredTimeout(callTimeMillis)) {
            log.info("Query [{}]: max call time reached, returning existing results: {} of {} results in {}ms", queryId, results.size(), maxResultsPerPage,
                            callTimeMillis);
            
            lifecycle = BaseQueryMetric.Lifecycle.NEXTTIMEOUT;
            
            status = ResultsPage.Status.PARTIAL;
            
            finished = true;
        }
        
        // save the previous result count
        prevResultCount = results.size();
        
        return finished;
    }
    
    private boolean shortCircuitTimeout(long callTimeMillis) {
        boolean timeout = false;
        
        // only return prematurely if we have at least 1 result
        if (!results.isEmpty()) {
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
    
    public static class Builder {
        private NextCallProperties nextCallProperties;
        private QueryExpirationProperties expirationProperties;
        private QueryQueueManager queryQueueManager;
        private QueryStorageCache queryStorageCache;
        private String queryId;
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
        
        public Builder setQueryQueueManager(QueryQueueManager queryQueueManager) {
            this.queryQueueManager = queryQueueManager;
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
        
        public Builder setQueryLogic(QueryLogic<?> queryLogic) {
            this.queryLogic = queryLogic;
            return this;
        }
        
        public NextCall build() {
            return new NextCall(this);
        }
    }
}
