package datawave.microservice.query.stream.runner;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.QueryManagementService;
import datawave.microservice.query.stream.listener.CountingResponseBodyEmitterListener;
import datawave.microservice.query.stream.listener.StreamingResponseListener;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricClient;
import datawave.microservice.querymetric.QueryMetricType;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NoResultsQueryException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.BaseQueryResponse;

public class StreamingCall implements Callable<Void> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    final private QueryManagementService queryManagementService;
    final private QueryMetricClient queryMetricClient;
    final private BaseQueryMetric baseQueryMetric;
    
    final private DatawaveUserDetails currentUser;
    final private DatawaveUserDetails serverUser;
    final private String queryId;
    
    final private StreamingResponseListener listener;
    
    private StreamingCall(Builder builder) {
        this.queryManagementService = builder.queryManagementService;
        this.queryMetricClient = builder.queryMetricClient;
        this.baseQueryMetric = builder.queryManagementService.getBaseQueryMetric().duplicate();
        
        this.currentUser = builder.currentUser;
        this.serverUser = builder.serverUser;
        this.queryId = builder.queryId;
        
        this.listener = builder.listener;
    }
    
    @Override
    public Void call() throws Exception {
        // since this is running in a separate thread, we need to set and use the thread-local baseQueryMetric
        ThreadLocal<BaseQueryMetric> baseQueryMetricOverride = queryManagementService.getBaseQueryMetricOverride();
        baseQueryMetricOverride.set(baseQueryMetric);
        
        try {
            boolean isFinished = false;
            do {
                final BaseQueryResponse nextResponse = next(queryId, currentUser);
                if (nextResponse != null) {
                    onResponse(nextResponse);
                    updateMetrics();
                } else {
                    isFinished = true;
                }
            } while (!isFinished);
            
            listener.close();
            return null;
        } catch (Exception e) {
            log.error("Error encountered while processing streaming results for query {}", queryId, e);
            listener.closeWithError(e);
            throw e;
        } finally {
            baseQueryMetricOverride.remove();
        }
    }
    
    private BaseQueryResponse next(String queryId, DatawaveUserDetails currentUser) {
        BaseQueryResponse nextResponse = null;
        try {
            long startTimeMillis = System.currentTimeMillis();
            nextResponse = queryManagementService.next(queryId, currentUser);
            long nextCallTimeMillis = System.currentTimeMillis() - startTimeMillis;
            
            BaseQueryMetric.PageMetric lastPageMetric = getLastPageMetric();
            if (lastPageMetric != null) {
                lastPageMetric.setCallTime(nextCallTimeMillis);
            }
        } catch (NoResultsQueryException e) {
            log.debug("No results found for query '{}'", queryId);
        } catch (QueryException e) {
            log.info("Encountered error while getting results for query '{}'", queryId);
        }
        return nextResponse;
    }
    
    private void onResponse(BaseQueryResponse nextResponse) throws QueryException {
        try {
            long startBytesWritten = getBytesWritten();
            long startTimeMillis = System.currentTimeMillis();
            listener.onResponse(nextResponse);
            long serializationTimeMillis = System.currentTimeMillis() - startTimeMillis;
            
            BaseQueryMetric.PageMetric lastPageMetric = getLastPageMetric();
            if (lastPageMetric != null) {
                lastPageMetric.setSerializationTime(serializationTimeMillis);
                lastPageMetric.setBytesWritten(getBytesWritten() - startBytesWritten);
            }
        } catch (IOException e) {
            throw new QueryException(DatawaveErrorCode.UNKNOWN_SERVER_ERROR, e, "Unknown error sending next page for query " + queryId);
        }
    }
    
    private long getBytesWritten() {
        long bytesWritten = 0L;
        if (listener instanceof CountingResponseBodyEmitterListener) {
            bytesWritten = ((CountingResponseBodyEmitterListener) listener).getBytesWritten();
        }
        return bytesWritten;
    }
    
    private void updateMetrics() {
        // send out the metrics
        try {
            // @formatter:off
            queryMetricClient.submit(
                    new QueryMetricClient.Request.Builder()
                            .withUser(serverUser)
                            .withMetric(baseQueryMetric.duplicate())
                            .withMetricType(QueryMetricType.DISTRIBUTED)
                            .build());
            // @formatter:on
        } catch (Exception e) {
            log.error("Error updating query metric", e);
        }
    }
    
    private BaseQueryMetric.PageMetric getLastPageMetric() {
        BaseQueryMetric.PageMetric pageMetric = null;
        List<BaseQueryMetric.PageMetric> pageTimes = baseQueryMetric.getPageTimes();
        if (!pageTimes.isEmpty()) {
            pageMetric = pageTimes.get(pageTimes.size() - 1);
        }
        return pageMetric;
    }
    
    public static class Builder {
        private QueryManagementService queryManagementService;
        private QueryMetricClient queryMetricClient;
        
        private DatawaveUserDetails currentUser;
        private DatawaveUserDetails serverUser;
        private String queryId;
        
        private StreamingResponseListener listener;
        
        public Builder setQueryManagementService(QueryManagementService queryManagementService) {
            this.queryManagementService = queryManagementService;
            return this;
        }
        
        public Builder setQueryMetricClient(QueryMetricClient queryMetricClient) {
            this.queryMetricClient = queryMetricClient;
            return this;
        }
        
        public Builder setCurrentUser(DatawaveUserDetails currentUser) {
            this.currentUser = currentUser;
            return this;
        }
        
        public Builder setServerUser(DatawaveUserDetails serverUser) {
            this.serverUser = serverUser;
            return this;
        }
        
        public Builder setQueryId(String queryId) {
            this.queryId = queryId;
            return this;
        }
        
        public Builder setListener(StreamingResponseListener listener) {
            this.listener = listener;
            return this;
        }
        
        public StreamingCall build() {
            return new StreamingCall(this);
        }
    }
}
