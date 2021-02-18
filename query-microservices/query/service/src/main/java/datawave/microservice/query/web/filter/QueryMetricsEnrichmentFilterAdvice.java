package datawave.microservice.query.web.filter;

import datawave.microservice.common.storage.QueryCache;
import datawave.microservice.common.storage.QueryState;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.web.QueryMetrics;
import datawave.microservice.query.web.annotation.EnrichQueryMetrics;
import datawave.webservice.query.metric.BaseQueryMetric;
import datawave.webservice.query.metric.QueryMetric;
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
import java.util.UUID;

@ControllerAdvice
public class QueryMetricsEnrichmentFilterAdvice extends BaseMethodStatsFilter implements ResponseBodyAdvice<Object> {

    private final Logger log = Logger.getLogger(this.getClass());

    private final QueryProperties queryProperties;

    private final QueryCache queryCache;

    private final QueryMetrics queryMetrics;

    public QueryMetricsEnrichmentFilterAdvice(QueryProperties queryProperties, QueryCache queryCache, QueryMetrics queryMetrics) {
        this.queryProperties = queryProperties;
        this.queryCache = queryCache;
        this.queryMetrics = queryMetrics;
    }

    @Override
    public boolean supports(MethodParameter returnType, @NonNull Class<? extends HttpMessageConverter<?>> converterType) {
        boolean supports = false;
        EnrichQueryMetrics annotation = (EnrichQueryMetrics) Arrays.stream(returnType.getMethodAnnotations()).filter(EnrichQueryMetrics.class::isInstance).findAny().orElse(null);
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
        System.out.println("QueryMetricsEnrichmentFilterAdvice writing metrics");

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
        if (queryId != null) {
            if (queryCache != null) {
                QueryState queryState = queryCache.getQuery(UUID.fromString(queryId));
                if (queryState != null && queryProperties.getLogic().get(queryState.getQueryLogic()).isMetricsEnabled()) {
                    try {
                        // TODO: This probably shouldn't be instantiated here, and we also shouldn't hard code the basequerymetric implementation.
                        // just using this as a placeholder until we start to bring things together.
                        BaseQueryMetric metric = new QueryMetric();
                        switch (QueryMetricsEnrichmentContext.getMethodType()) {
                            case CREATE:
                                metric.setCreateCallTime(responseStats.getCallTime());
                                metric.setLoginTime(responseStats.getLoginTime());
                                break;
                            case CREATE_AND_NEXT:
                                metric.setCreateCallTime(responseStats.getCallTime());
                                metric.setLoginTime(responseStats.getLoginTime());
                                List<BaseQueryMetric.PageMetric> pageTimes = metric.getPageTimes();
                                if (pageTimes != null && !pageTimes.isEmpty()) {
                                    BaseQueryMetric.PageMetric pm = pageTimes.get(pageTimes.size() - 1);
                                    pm.setCallTime(responseStats.getCallTime());
                                    pm.setLoginTime(responseStats.getLoginTime());
                                    pm.setSerializationTime(responseStats.getSerializationTime());
                                    pm.setBytesWritten(responseStats.getBytesWritten());
                                }
                                break;
                            case NEXT:
                                pageTimes = metric.getPageTimes();
                                if (pageTimes != null && !pageTimes.isEmpty()) {
                                    BaseQueryMetric.PageMetric pm = pageTimes.get(pageTimes.size() - 1);
                                    pm.setCallTime(responseStats.getCallTime());
                                    pm.setLoginTime(responseStats.getLoginTime());
                                    pm.setSerializationTime(responseStats.getSerializationTime());
                                    pm.setBytesWritten(responseStats.getBytesWritten());
                                }
                                break;
                        }

                        if (queryMetrics != null)
                            queryMetrics.updateMetric(metric);
                        else
                            log.error("QueryMetricsBean JNDI lookup returned null");
                    } catch (Exception e) {
                        log.error("Unable to record metrics for " + QueryMetricsEnrichmentContext.getMethodType() + " method: " + e.getLocalizedMessage(), e);
                    }
                } else {
                    log.error("RunningQuery instance not in the cache!, queryId: " + queryId);
                }
            } else {
                log.error("Query cache not injected! No metrics will be recorded for serialization times.");
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
