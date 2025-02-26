package datawave.microservice.query.web.filter;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import org.apache.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.MethodParameter;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.context.annotation.RequestScope;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;

import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.web.annotation.EnrichQueryMetrics;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricClient;
import datawave.microservice.querymetric.QueryMetricType;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;

@ControllerAdvice
public class QueryMetricsEnrichmentFilterAdvice extends BaseMethodStatsFilter implements ResponseBodyAdvice<Object> {
    
    private final Logger log = Logger.getLogger(this.getClass());
    
    private final QueryLogicFactory queryLogicFactory;
    
    private final QueryStorageCache queryStorageCache;
    
    private final QueryMetricClient queryMetricClient;
    
    // Note: BaseQueryMetric needs to be request scoped
    private final BaseQueryMetric baseQueryMetric;
    
    // Note: QueryMetricsEnrichmentContext needs to be request scoped
    private final QueryMetricsEnrichmentContext queryMetricsEnrichmentContext;
    
    public QueryMetricsEnrichmentFilterAdvice(QueryLogicFactory queryLogicFactory, QueryStorageCache queryStorageCache, QueryMetricClient queryMetricClient,
                    BaseQueryMetric baseQueryMetric, QueryMetricsEnrichmentContext queryMetricsEnrichmentContext) {
        this.queryLogicFactory = queryLogicFactory;
        this.queryStorageCache = queryStorageCache;
        this.queryMetricClient = queryMetricClient;
        this.baseQueryMetric = baseQueryMetric;
        this.queryMetricsEnrichmentContext = queryMetricsEnrichmentContext;
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
                    queryMetricsEnrichmentContext.setMethodType(annotation.methodType());
                } else if (BaseQueryResponse.class.isAssignableFrom(returnClass)) {
                    supports = true;
                    queryMetricsEnrichmentContext.setMethodType(annotation.methodType());
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
            queryMetricsEnrichmentContext.setQueryId(genericResponse.getResult());
        } else if (body instanceof BaseQueryResponse) {
            BaseQueryResponse baseResponse = (BaseQueryResponse) body;
            queryMetricsEnrichmentContext.setQueryId(baseResponse.getQueryId());
        }
        
        return body;
    }
    
    @Override
    public void postProcess(ResponseMethodStats responseStats) {
        String queryId = queryMetricsEnrichmentContext.getQueryId();
        EnrichQueryMetrics.MethodType methodType = queryMetricsEnrichmentContext.getMethodType();
        
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
            
            // retrieve the server user and determine whether metrics are enabled
            boolean isMetricsEnabled = false;
            DatawaveUserDetails serverUser = null;
            try {
                QueryLogic<?> logic = queryLogicFactory.getQueryLogic(queryLogic);
                isMetricsEnabled = logic.getCollectQueryMetrics();
                serverUser = (DatawaveUserDetails) logic.getServerUser();
            } catch (Exception e) {
                log.warn("Unable to retrieve the server user and determine if query logic '" + queryLogic + "' supports metrics");
            }
            
            if (isMetricsEnabled) {
                try {
                    switch (queryMetricsEnrichmentContext.getMethodType()) {
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
                    
                    baseQueryMetric.setLastUpdated(new Date());
                    // @formatter:off
                    queryMetricClient.submit(
                            new QueryMetricClient.Request.Builder()
                                    .withUser(serverUser)
                                    .withMetric(baseQueryMetric.duplicate())
                                    .withMetricType(QueryMetricType.DISTRIBUTED)
                                    .build());
                    // @formatter:on
                } catch (Exception e) {
                    log.error("Unable to record metrics for query '" + queryMetricsEnrichmentContext.getQueryId() + "' and method '"
                                    + queryMetricsEnrichmentContext.getMethodType() + "': " + e.getLocalizedMessage(), e);
                }
            }
        }
    }
    
    public static class QueryMetricsEnrichmentContext {
        private String queryId;
        private EnrichQueryMetrics.MethodType methodType;
        
        public String getQueryId() {
            return queryId;
        }
        
        public void setQueryId(String queryId) {
            this.queryId = queryId;
        }
        
        public EnrichQueryMetrics.MethodType getMethodType() {
            return methodType;
        }
        
        public void setMethodType(EnrichQueryMetrics.MethodType methodType) {
            this.methodType = methodType;
        }
    }
    
    @Configuration
    public static class QueryMetricsEnrichmentFilterAdviceConfig {
        @Bean
        @ConditionalOnMissingBean
        @RequestScope
        public QueryMetricsEnrichmentContext queryMetricsEnrichmentContext() {
            return new QueryMetricsEnrichmentContext();
        }
    }
}
