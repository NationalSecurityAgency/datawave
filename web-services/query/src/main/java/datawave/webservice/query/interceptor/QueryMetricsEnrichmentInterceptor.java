package datawave.webservice.query.interceptor;

import java.io.IOException;
import java.util.List;

import javax.annotation.Priority;
import javax.inject.Inject;
import javax.ws.rs.Priorities;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.ext.WriterInterceptorContext;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.resteasy.interceptor.BaseMethodStatsInterceptor;
import datawave.webservice.query.annotation.EnrichQueryMetrics;
import datawave.webservice.query.annotation.EnrichQueryMetrics.MethodType;
import datawave.webservice.query.cache.QueryCache;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogic;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.PageMetric;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.query.runner.QueryExecutorBean;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;

import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;
import org.jboss.resteasy.core.interception.ContainerResponseContextImpl;
import org.jboss.resteasy.core.interception.PreMatchContainerRequestContext;
import org.jboss.resteasy.util.FindAnnotation;

@Provider
@Priority(Priorities.USER)
@EnrichQueryMetrics(methodType = MethodType.NONE)
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class QueryMetricsEnrichmentInterceptor extends BaseMethodStatsInterceptor {
    protected static class QueryCall {
        MethodType methodType;
        String queryID;
        
        public QueryCall(MethodType methodType, String queryID) {
            super();
            this.methodType = methodType;
            this.queryID = queryID;
        }
    }
    
    private Logger log = Logger.getLogger(QueryMetricsEnrichmentInterceptor.class);
    
    @Inject
    private QueryMetricsBean queryMetricsBean;
    @Inject
    private QueryCache queryCache;
    
    @Override
    public void filter(ContainerRequestContext request) throws IOException {
        // Just call the parent doPreProcess so that we save stats for this call and look them up in the postProcess/write
        doPreProcess((PreMatchContainerRequestContext) request);
    }
    
    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext response) throws IOException {
        super.filter(request, response);
        
        if (response instanceof ContainerResponseContextImpl) {
            ContainerResponseContextImpl containerResponseImpl = (ContainerResponseContextImpl) response;
            EnrichQueryMetrics e = FindAnnotation.findAnnotation(containerResponseImpl.getJaxrsResponse().getAnnotations(), EnrichQueryMetrics.class);
            if (e != null) {
                
                Object entity = response.getEntity();
                if (entity instanceof GenericResponse) {
                    @SuppressWarnings("unchecked")
                    GenericResponse<String> qidResponse = (GenericResponse<String>) entity;
                    request.setProperty(QueryCall.class.getName(), new QueryCall(e.methodType(), qidResponse.getResult()));
                } else if (entity instanceof BaseQueryResponse) {
                    BaseQueryResponse baseResponse = (BaseQueryResponse) entity;
                    request.setProperty(QueryCall.class.getName(), new QueryCall(e.methodType(), baseResponse.getQueryId()));
                } else if (entity instanceof QueryExecutorBean.ExecuteStreamingOutputResponse) {
                    // The ExecuteStreamingOutputResponse class updates the metrics, no need to do it here
                } else {
                    log.error("Unexpected response class for metrics annotated query method " + request.getUriInfo().getPath() + ". Response class was "
                                    + (entity == null ? "null response" : entity.getClass().toString()));
                }
            }
        }
    }
    
    @Override
    public void aroundWriteTo(WriterInterceptorContext context) throws IOException, WebApplicationException {
        ResponseMethodStats stats = doWrite(context);
        
        QueryCall queryCall = (QueryCall) context.getProperty(QueryCall.class.getName());
        if (queryCall != null) {
            if (queryCache != null) {
                RunningQuery rq = queryCache.get(queryCall.queryID);
                if (rq != null) {
                    BaseQueryLogic<?> baseLogic = null;
                    QueryLogic<?> logic = rq.getLogic();
                    if (logic != null && logic instanceof BaseQueryLogic) {
                        baseLogic = (BaseQueryLogic) logic;
                    }
                    if (baseLogic != null && baseLogic.getCollectQueryMetrics()) {
                        try {
                            BaseQueryMetric metric = rq.getMetric();
                            switch (queryCall.methodType) {
                                case CREATE:
                                    metric.setCreateCallTime(stats.getCallTime());
                                    metric.setLoginTime(stats.getLoginTime());
                                    break;
                                case CREATE_AND_NEXT:
                                    metric.setCreateCallTime(stats.getCallTime());
                                    metric.setLoginTime(stats.getLoginTime());
                                    List<PageMetric> pageTimes = metric.getPageTimes();
                                    if (pageTimes != null && !pageTimes.isEmpty()) {
                                        PageMetric pm = pageTimes.get(pageTimes.size() - 1);
                                        pm.setCallTime(stats.getCallTime());
                                        pm.setLoginTime(stats.getLoginTime());
                                        pm.setSerializationTime(stats.getSerializationTime());
                                        pm.setBytesWritten(stats.getBytesWritten());
                                    }
                                    break;
                                case NEXT:
                                    pageTimes = metric.getPageTimes();
                                    if (pageTimes != null && !pageTimes.isEmpty()) {
                                        PageMetric pm = pageTimes.get(pageTimes.size() - 1);
                                        pm.setCallTime(stats.getCallTime());
                                        pm.setLoginTime(stats.getLoginTime());
                                        pm.setSerializationTime(stats.getSerializationTime());
                                        pm.setBytesWritten(stats.getBytesWritten());
                                    }
                                    break;
                            }
                            
                            if (queryMetricsBean != null)
                                queryMetricsBean.updateMetric(metric);
                            else
                                log.error("QueryMetricsBean JNDI lookup returned null");
                        } catch (Exception e) {
                            log.error("Unable to record metrics for " + queryCall.methodType + " method: " + e.getLocalizedMessage(), e);
                        }
                    }
                } else {
                    log.error("RunningQuery instance not in the cache!, queryId: " + queryCall.queryID);
                }
            } else {
                log.error("Query cache not injected! No metrics will be recorded for serialization times.");
            }
        }
    }
}
