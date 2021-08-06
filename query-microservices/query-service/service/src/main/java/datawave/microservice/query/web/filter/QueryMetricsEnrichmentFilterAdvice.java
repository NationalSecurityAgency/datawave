package datawave.microservice.query.web.filter;

import datawave.microservice.query.logic.QueryLogicFactory;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.web.annotation.EnrichQueryMetrics;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricClient;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import org.apache.log4j.Logger;
import org.springframework.core.MethodParameter;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@ControllerAdvice
public class QueryMetricsEnrichmentFilterAdvice extends BaseMethodStatsFilter implements ResponseBodyAdvice<Object> {
    
    private final Logger log = Logger.getLogger(this.getClass());
    
    private final QueryLogicFactory queryLogicFactory;
    
    private final QueryStorageCache queryStorageCache;
    
    private final QueryMetricClient queryMetricClient;
    
    // Note: BaseQueryMetric needs to be request scoped
    private final BaseQueryMetric baseQueryMetric;
    
    public QueryMetricsEnrichmentFilterAdvice(QueryLogicFactory queryLogicFactory, QueryStorageCache queryStorageCache, QueryMetricClient queryMetricClient,
                    BaseQueryMetric baseQueryMetric) {
        this.queryLogicFactory = queryLogicFactory;
        this.queryStorageCache = queryStorageCache;
        this.queryMetricClient = queryMetricClient;
        this.baseQueryMetric = baseQueryMetric;
    }
    
    @Override
    public boolean supports(MethodParameter returnType, @NonNull Class<? extends HttpMessageConverter<?>> converterType) {
        boolean supports = false;
        EnrichQueryMetrics annotation = (EnrichQueryMetrics) Arrays.stream(returnType.getMethodAnnotations()).filter(EnrichQueryMetrics.class::isInstance)
                        .findAny().orElse(null);
        if (annotation != null) {
            try {
                Class<?> returnClass = Objects.requireNonNull(returnType.getMethod()).getReturnType();
                if (GenericResponse.class.isAssignableFrom(returnClass)) {
                    supports = true;
                    QueryMetricsEnrichmentContext.setMethodType(annotation.methodType());
                } else if (BaseQueryResponse.class.isAssignableFrom(returnClass)) {
                    supports = true;
                    QueryMetricsEnrichmentContext.setMethodType(annotation.methodType());
                } else {
                    log.error("Unexpected response class for metrics annotated query method " + returnType.getMethod().getName() + ". Response class was "
                                    + returnClass.toString());
                }
            } catch (NullPointerException e) {
                // do nothing
            }
        }
        
        return supports;
    }
    
    @Override
    public Object beforeBodyWrite(Object body, @NonNull MethodParameter returnType, @NonNull MediaType selectedContentType,
                    @NonNull Class<? extends HttpMessageConverter<?>> selectedConverterType, @NonNull ServerHttpRequest request,
                    @NonNull ServerHttpResponse response) {
        if (body instanceof GenericResponse) {
            @SuppressWarnings({"unchecked", "rawtypes"})
            GenericResponse<String> genericResponse = (GenericResponse) body;
            QueryMetricsEnrichmentContext.setQueryId(genericResponse.getResult());
        } else if (body instanceof BaseQueryResponse) {
            BaseQueryResponse baseResponse = (BaseQueryResponse) body;
            QueryMetricsEnrichmentContext.setQueryId(baseResponse.getQueryId());
        }
        
        return body;
    }
    
    @Override
    public void postProcess(ResponseMethodStats responseStats) {
        String queryId = QueryMetricsEnrichmentContext.getQueryId();
        EnrichQueryMetrics.MethodType methodType = QueryMetricsEnrichmentContext.getMethodType();
        
        if (queryId != null && methodType != null) {
            // determine which query logic is being used
            String queryLogic = null;
            if (baseQueryMetric.getQueryLogic() != null) {
                queryLogic = baseQueryMetric.getQueryLogic();
            } else {
                QueryStatus queryStatus = queryStorageCache.getQueryStatus(queryId);
                if (queryStatus != null) {
                    queryLogic = queryStatus.getQuery().getQueryLogicName();
                }
            }
            
            // determine whether metrics are enabled
            boolean isMetricsEnabled = false;
            try {
                isMetricsEnabled = queryLogicFactory.getQueryLogic(queryLogic).getCollectQueryMetrics();
            } catch (Exception e) {
                log.warn("Unable to determine if query logic '" + queryLogic + "' supports metrics");
            }
            
            if (isMetricsEnabled) {
                try {
                    switch (QueryMetricsEnrichmentContext.getMethodType()) {
                        case CREATE:
                            baseQueryMetric.setCreateCallTime(responseStats.getCallTime());
                            baseQueryMetric.setLoginTime(responseStats.getLoginTime());
                            break;
                        case CREATE_AND_NEXT:
                            baseQueryMetric.setCreateCallTime(responseStats.getCallTime());
                            baseQueryMetric.setLoginTime(responseStats.getLoginTime());
                            List<BaseQueryMetric.PageMetric> pageTimes = baseQueryMetric.getPageTimes();
                            if (pageTimes != null && !pageTimes.isEmpty()) {
                                BaseQueryMetric.PageMetric pm = pageTimes.get(pageTimes.size() - 1);
                                pm.setCallTime(responseStats.getCallTime());
                                pm.setLoginTime(responseStats.getLoginTime());
                                pm.setSerializationTime(responseStats.getSerializationTime());
                                pm.setBytesWritten(responseStats.getBytesWritten());
                            }
                            break;
                        case NEXT:
                            pageTimes = baseQueryMetric.getPageTimes();
                            if (pageTimes != null && !pageTimes.isEmpty()) {
                                BaseQueryMetric.PageMetric pm = pageTimes.get(pageTimes.size() - 1);
                                pm.setCallTime(responseStats.getCallTime());
                                pm.setLoginTime(responseStats.getLoginTime());
                                pm.setSerializationTime(responseStats.getSerializationTime());
                                pm.setBytesWritten(responseStats.getBytesWritten());
                            }
                            break;
                    }
                    
                    // @formatter:off
                    queryMetricClient.submit(
                            new QueryMetricClient.Request.Builder()
                                    .withMetric(baseQueryMetric)
                                    .build());
                    // @formatter:on
                } catch (Exception e) {
                    log.error("Unable to record metrics for query '" + QueryMetricsEnrichmentContext.getQueryId() + "' and method '"
                                    + QueryMetricsEnrichmentContext.getMethodType() + "': " + e.getLocalizedMessage(), e);
                }
            }
        }
    }
    
    @Override
    public void destroy() {
        super.destroy();
        QueryMetricsEnrichmentContext.remove();
    }
    
    public static class QueryMetricsEnrichmentContext {
        
        private static final ThreadLocal<String> queryId = new ThreadLocal<>();
        private static final ThreadLocal<EnrichQueryMetrics.MethodType> methodType = new ThreadLocal<>();
        
        public static String getQueryId() {
            return queryId.get();
        }
        
        public static void setQueryId(String queryId) {
            QueryMetricsEnrichmentContext.queryId.set(queryId);
        }
        
        public static EnrichQueryMetrics.MethodType getMethodType() {
            return methodType.get();
        }
        
        public static void setMethodType(EnrichQueryMetrics.MethodType methodType) {
            QueryMetricsEnrichmentContext.methodType.set(methodType);
        }
        
        private static void remove() {
            queryId.remove();
        }
    }
}
