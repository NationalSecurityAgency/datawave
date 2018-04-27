package datawave.webservice.query.runner;

import static datawave.webservice.query.annotation.EnrichQueryMetrics.MethodType;
import static datawave.webservice.query.cache.QueryTraceCache.CacheListener;
import static datawave.webservice.query.cache.QueryTraceCache.PatternWrapper;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.security.Principal;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.ejb.AsyncResult;
import javax.ejb.Asynchronous;
import javax.ejb.EJBContext;
import javax.ejb.EJBException;
import javax.ejb.LocalBean;
import javax.ejb.SessionContext;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.inject.Inject;
import javax.interceptor.Interceptors;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.CountingOutputStream;

import datawave.webservice.query.metric.BaseQueryMetric;
import datawave.webservice.query.metric.BaseQueryMetric.Prediction;
import io.protostuff.LinkedBuffer;
import io.protostuff.Message;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.Schema;
import io.protostuff.YamlIOUtil;
import datawave.annotation.ClearQuerySessionId;
import datawave.annotation.DateFormat;
import datawave.annotation.GenerateQuerySessionId;
import datawave.annotation.Required;
import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.configuration.spring.SpringBean;
import datawave.interceptor.RequiredInterceptor;
import datawave.interceptor.ResponseInterceptor;
import datawave.marking.SecurityMarking;
import datawave.query.data.UUIDType;
import datawave.resteasy.interceptor.CreateQuerySessionIDFilter;
import datawave.security.authorization.DatawavePrincipal;
import datawave.webservice.common.audit.AuditBean;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor.AuditType;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.exception.BadRequestException;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.common.exception.NoResultsException;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.QueryImpl.Parameter;
import datawave.webservice.query.QueryParameters;
import datawave.webservice.query.QueryPersistence;
import datawave.webservice.query.annotation.EnrichQueryMetrics;
import datawave.webservice.query.cache.CreatedQueryLogicCacheBean;
import datawave.webservice.query.cache.QueryCache;
import datawave.webservice.query.cache.QueryExpirationConfiguration;
import datawave.webservice.query.cache.QueryMetricFactory;
import datawave.webservice.query.cache.QueryTraceCache;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.cache.RunningQueryTimingImpl;
import datawave.webservice.query.configuration.LookupUUIDConfiguration;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NoResultsQueryException;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.PreConditionFailedQueryException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.exception.UnauthorizedQueryException;
import datawave.webservice.query.factory.Persister;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.logic.QueryLogicFactory;
import datawave.webservice.query.logic.QueryLogicTransformer;
import datawave.webservice.query.metric.BaseQueryMetric.PageMetric;
import datawave.webservice.query.metric.QueryMetric;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.result.logic.QueryLogicDescription;
import datawave.webservice.query.util.GetUUIDCriteria;
import datawave.webservice.query.util.LookupUUIDUtil;
import datawave.webservice.query.util.NextContentCriteria;
import datawave.webservice.query.util.PostUUIDCriteria;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import datawave.webservice.query.util.QueryUtil;
import datawave.webservice.query.util.UIDQueryCriteria;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.BaseResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.QueryImplListResponse;
import datawave.webservice.result.QueryLogicResponse;
import datawave.webservice.result.VoidResponse;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.commons.jexl2.parser.TokenMgrError;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;
import org.jboss.resteasy.annotations.GZIP;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.springframework.util.StringUtils;

@Path("/Query")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator", "JBossAdministrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator", "JBossAdministrator"})
@Stateless
@LocalBean
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class QueryExecutorBean implements QueryExecutor {
    
    private final Logger log = Logger.getLogger(QueryExecutorBean.class);
    
    @Inject
    private QueryCache queryCache;
    
    @Inject
    private QueryTraceCache queryTraceCache;
    
    @Inject
    private AccumuloConnectionFactory connectionFactory;
    
    @Inject
    private AuditBean auditor;
    
    @Inject
    private QueryMetricsBean metrics;
    
    @Resource
    private ManagedExecutorService executor;
    
    @Inject
    private QueryLogicFactory queryLogicFactory;
    
    @Inject
    private QueryExpirationConfiguration queryExpirationConf;
    
    @Inject
    private Persister persister;
    
    @Resource
    private EJBContext ctx;
    
    @Resource
    private SessionContext sessionContext;
    
    @Inject
    @SpringBean(refreshable = true)
    private LookupUUIDConfiguration lookupUUIDConfiguration;
    
    @Inject
    private SecurityMarking marking;
    
    @Inject
    private AuditParameters auditParameters;
    
    @Inject
    private ResponseObjectFactory responseObjectFactory;
    
    private LookupUUIDUtil lookupUUIDUtil;
    
    @Inject
    private CreatedQueryLogicCacheBean qlCache;
    
    @Inject
    private QueryPredictor predictor;
    
    @Inject
    private QueryMetricFactory metricFactory;
    
    @Inject
    private AccumuloConnectionRequestBean accumuloConnectionRequestBean;
    
    private Multimap<String,PatternWrapper> traceInfos;
    private CacheListener traceCacheListener;
    
    private final int PAGE_TIMEOUT_MIN = 1;
    private final int PAGE_TIMEOUT_MAX = QueryExpirationConfiguration.PAGE_TIMEOUT_MIN_DEFAULT;
    
    @Inject
    private QueryParameters qp;
    
    // A few items that are cached by the validateQuery call
    private static class QueryData {
        QueryLogic<?> logic = null;
        Principal p = null;
        Set<String> proxyServers = null;
        String userDn = null;
        String userid = null;
        List<String> dnList = null;
    }
    
    @PostConstruct
    public void init() {
        
        Multimap<String,PatternWrapper> infos = HashMultimap.create();
        traceInfos = queryTraceCache.putIfAbsent("traceInfos", infos);
        if (traceInfos == null)
            traceInfos = infos;
        traceCacheListener = new CacheListener() {
            @Override
            public void cacheEntryModified(String key, Multimap<String,PatternWrapper> traceInfo) {
                if ("traceInfos".equals(key)) {
                    traceInfos = traceInfo;
                }
            }
        };
        queryTraceCache.addListener(traceCacheListener);
        
        this.lookupUUIDUtil = new LookupUUIDUtil(this.lookupUUIDConfiguration, this, this.ctx, this.responseObjectFactory);
    }
    
    @PreDestroy
    public void close() {
        if (queryTraceCache != null && traceCacheListener != null)
            queryTraceCache.removeListener(traceCacheListener);
    }
    
    /**
     * List QueryLogic types that are currently available
     *
     * @HTTP 200 Success
     * @return datawave.webservice.result.QueryLogicResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     */
    @Path("/listQueryLogic")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "text/html"})
    @GET
    @Interceptors({ResponseInterceptor.class})
    @Override
    @Timed(name = "dw.query.listQueryLogic", absolute = true)
    public QueryLogicResponse listQueryLogic() {
        QueryLogicResponse response = new QueryLogicResponse();
        List<QueryLogic<?>> logicList = queryLogicFactory.getQueryLogicList();
        List<QueryLogicDescription> logicConfigurationList = new ArrayList<>();
        
        // reference query necessary to avoid NPEs in getting the Transformer and BaseResponse
        Query q = new QueryImpl();
        Date now = new Date();
        q.setExpirationDate(now);
        q.setQuery("test");
        q.setQueryAuthorizations("ALL");
        ResultsPage emptyList = new ResultsPage();
        
        for (QueryLogic<?> l : logicList) {
            try {
                QueryLogicDescription d = new QueryLogicDescription(l.getLogicName());
                d.setAuditType(l.getAuditType(null).toString());
                d.setLogicDescription(l.getLogicDescription());
                
                Set<String> optionalQueryParameters = l.getOptionalQueryParameters();
                if (optionalQueryParameters != null) {
                    d.setSupportedParams(new ArrayList<>(optionalQueryParameters));
                }
                Set<String> requiredQueryParameters = l.getRequiredQueryParameters();
                if (requiredQueryParameters != null) {
                    d.setRequiredParams(new ArrayList<>(requiredQueryParameters));
                }
                Set<String> exampleQueries = l.getExampleQueries();
                if (exampleQueries != null) {
                    d.setExampleQueries(new ArrayList<>(exampleQueries));
                }
                Set<String> requiredRoles = l.getRoleManager().getRequiredRoles();
                if (requiredRoles != null) {
                    List<String> requiredRolesList = new ArrayList<>();
                    requiredRolesList.addAll(l.getRoleManager().getRequiredRoles());
                    d.setRequiredRoles(requiredRolesList);
                }
                q.setQueryLogicName(l.getLogicName());
                try {
                    QueryLogicTransformer t = l.getTransformer(q);
                    BaseResponse refResponse = t.createResponse(emptyList);
                    d.setResponseClass(refResponse.getClass().getCanonicalName());
                } catch (RuntimeException e) {
                    QueryException qe = new QueryException(DatawaveErrorCode.QUERY_TRANSFORM_ERROR, e);
                    log.error(qe);
                    response.addException(qe);
                    d.setResponseClass("unknown");
                }
                
                List<String> querySyntax = new ArrayList<>();
                try {
                    Method m = l.getClass().getMethod("getQuerySyntaxParsers");
                    Object result = m.invoke(l);
                    if (result instanceof Map<?,?>) {
                        Map<?,?> map = (Map<?,?>) result;
                        for (Object o : map.keySet())
                            querySyntax.add(o.toString());
                    }
                } catch (Exception e) {
                    log.warn("Unable to get query syntax for query logic: " + l.getClass().getCanonicalName());
                }
                if (querySyntax.isEmpty()) {
                    querySyntax.add("CUSTOM");
                }
                d.setQuerySyntax(querySyntax);
                
                logicConfigurationList.add(d);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        Collections.sort(logicConfigurationList, new Comparator<QueryLogicDescription>() {
            @Override
            public int compare(QueryLogicDescription ql1, QueryLogicDescription ql2) {
                return ql1.getName().compareTo(ql2.getName());
            }
        });
        response.setQueryLogicList(logicConfigurationList);
        
        return response;
    }
    
    @Override
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    public GenericResponse<String> defineQuery(@Required("logicName") String queryLogicName, MultivaluedMap<String,String> queryParameters) {
        return defineQuery(queryLogicName, queryParameters, null);
    }
    
    /**
     * Helper to throw Response Error for create/define Query
     */
    private void throwBadRequest(DatawaveErrorCode ec, GenericResponse<String> response) {
        BadRequestQueryException qe = new BadRequestQueryException(ec);
        response.addException(qe);
        throw new BadRequestException(qe, response);
    }
    
    /**
     * This method will provide some initial query validation for the define and create query calls.
     */
    private QueryData validateQuery(String queryLogicName, MultivaluedMap<String,String> queryParameters, HttpHeaders httpHeaders) {
        QueryData qd = new QueryData();
        
        log.debug(queryParameters);
        qp.clear();
        qp.setRequestHeaders(httpHeaders != null ? httpHeaders.getRequestHeaders() : null);
        qp.validate(queryParameters);
        
        // The pagesize and expirationDate checks will always be false when called from the RemoteQueryExecutor.
        // Leaving for now until we can test to ensure that is always the case.
        if (qp.getPagesize() <= 0) {
            GenericResponse<String> response = new GenericResponse<>();
            throwBadRequest(DatawaveErrorCode.INVALID_PAGE_SIZE, response);
        }
        
        if (qp.getPageTimeout() != -1 && (qp.getPageTimeout() < PAGE_TIMEOUT_MIN || qp.getPageTimeout() > PAGE_TIMEOUT_MAX)) {
            GenericResponse<String> response = new GenericResponse<>();
            throwBadRequest(DatawaveErrorCode.INVALID_PAGE_TIMEOUT, response);
        }
        
        if (System.currentTimeMillis() >= qp.getExpirationDate().getTime()) {
            GenericResponse<String> response = new GenericResponse<>();
            throwBadRequest(DatawaveErrorCode.INVALID_EXPIRATION_DATE, response);
        }
        
        // Ensure begin date does not occur after the end date
        if (qp.getBeginDate().after(qp.getEndDate())) {
            GenericResponse<String> response = new GenericResponse<>();
            throwBadRequest(DatawaveErrorCode.BEGIN_DATE_AFTER_END_DATE, response);
        }
        
        // Pull "params" values into individual query parameters for validation on the query logic.
        // This supports the deprecated "params" value (both on the old and new API). Once we remove the deprecated
        // parameter, this code block can go away.
        String params = queryParameters.getFirst(QueryParameters.QUERY_PARAMS);
        if (params != null) {
            for (Parameter pm : QueryUtil.parseParameters(params)) {
                queryParameters.putSingle(pm.getParameterName(), pm.getParameterValue());
            }
            queryParameters.remove(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ);
            queryParameters.remove(AuditParameters.USER_DN);
            queryParameters.remove(AuditParameters.QUERY_AUDIT_TYPE);
        }
        
        // will throw IllegalArgumentException if not defined
        try {
            qd.logic = queryLogicFactory.getQueryLogic(queryLogicName, ctx.getCallerPrincipal());
        } catch (Exception e) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.QUERY_LOGIC_ERROR, e);
            GenericResponse<String> response = new GenericResponse<>();
            response.addException(qe.getBottomQueryException());
            throw new BadRequestException(qe, response);
        }
        qd.logic.validate(queryParameters);
        
        queryParameters.add("logicClass", queryLogicName);
        try {
            marking.clear();
            marking.validate(queryParameters);
        } catch (IllegalArgumentException e) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.SECURITY_MARKING_CHECK_ERROR, e);
            GenericResponse<String> response = new GenericResponse<>();
            response.addException(qe);
            throw new BadRequestException(qe, response);
        }
        queryParameters.add(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, marking.toColumnVisibilityString());
        // Find out who/what called this method
        qd.proxyServers = null;
        qd.p = ctx.getCallerPrincipal();
        qd.userDn = qd.p.getName();
        qd.userid = qd.userDn;
        qd.dnList = Collections.singletonList(qd.userid);
        if (qd.p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) qd.p;
            qd.userid = dp.getShortName();
            qd.userDn = dp.getUserDN().subjectDN();
            String[] dns = dp.getDNs();
            Arrays.sort(dns);
            qd.dnList = Arrays.asList(dns);
            qd.proxyServers = dp.getProxyServers();
        }
        log.trace(qd.userid + " has authorizations " + ((qd.p instanceof DatawavePrincipal) ? ((DatawavePrincipal) qd.p).getAuthorizations() : ""));
        queryParameters.add(AuditParameters.USER_DN, qd.userDn);
        
        // always check against the max
        if (qd.logic.getMaxPageSize() > 0 && qp.getPagesize() > qd.logic.getMaxPageSize()) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.PAGE_SIZE_TOO_LARGE, MessageFormat.format("Max = {0}.",
                            qd.logic.getMaxPageSize()));
            GenericResponse<String> response = new GenericResponse<>();
            response.addException(qe);
            throw new BadRequestException(qe, response);
        }
        
        return qd;
    }
    
    /**
     * @param queryLogicName
     * @param queryParameters
     * @return
     */
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/{logicName}/define")
    @GZIP
    @GenerateQuerySessionId(cookieBasePath = "/DataWave/Query/")
    @EnrichQueryMetrics(methodType = MethodType.CREATE)
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @Timed(name = "dw.query.defineQuery", absolute = true)
    public GenericResponse<String> defineQuery(@Required("logicName") @PathParam("logicName") String queryLogicName,
                    MultivaluedMap<String,String> queryParameters, @Context HttpHeaders httpHeaders) {
        CreateQuerySessionIDFilter.QUERY_ID.set(null);
        
        QueryData qd = validateQuery(queryLogicName, queryParameters, httpHeaders);
        
        GenericResponse<String> response = new GenericResponse<>();
        
        // We need to put a disconnected RunningQuery instance into the cache. Otherwise TRANSIENT queries
        // will not exist when reset is called.
        Span defineSpan = null;
        RunningQuery rq;
        try {
            MultivaluedMap<String,String> optionalQueryParameters = qp.getUnknownParameters(queryParameters);
            Query q = persister.create(qd.userDn, qd.dnList, marking, queryLogicName, qp, optionalQueryParameters);
            response.setResult(q.getId().toString());
            
            // If we're supposed to trace this query, then turn tracing on and set information about the query
            // onto the span so that it is saved in the trace table.
            TInfo traceInfo = null;
            boolean shouldTraceQuery = shouldTraceQuery(qp.getQuery(), qd.userid, false);
            if (shouldTraceQuery) {
                Span span = Trace.on("query:" + q.getId().toString());
                log.debug("Tracing query " + q.getId() + " [" + qp.getQuery() + "] on trace ID " + Long.toHexString(span.traceId()));
                for (Entry<String,List<String>> param : queryParameters.entrySet()) {
                    span.data(param.getKey(), param.getValue().get(0));
                }
                traceInfo = Tracer.traceInfo();
                
                defineSpan = Trace.start("query:define");
            }
            
            AccumuloConnectionFactory.Priority priority = qd.logic.getConnectionPriority();
            
            rq = new RunningQuery(metrics, null, priority, qd.logic, q, qp.getAuths(), qd.p, new RunningQueryTimingImpl(queryExpirationConf,
                            qp.getPageTimeout()), this.executor, this.predictor, this.metricFactory);
            rq.setActiveCall(true);
            rq.getMetric().setProxyServers(qd.proxyServers);
            rq.setTraceInfo(traceInfo);
            queryCache.put(q.getId().toString(), rq);
            rq.setActiveCall(false);
            CreateQuerySessionIDFilter.QUERY_ID.set(q.getId().toString());
            return response;
        } catch (DatawaveWebApplicationException e) {
            throw e;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            QueryException qe = new QueryException(DatawaveErrorCode.RUNNING_QUERY_CACHE_ERROR, e);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        } finally {
            if (null != defineSpan) {
                // Spans aren't recorded if they take no time, so sleep for a
                // couple milliseconds just to ensure we get something saved.
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    // ignore
                }
                
                defineSpan.stop();
                
                // TODO: not sure this makes any sense anymore in Accumulo 1.8.1
                // if (null != defineSpan.parent()) {
                // // Stop the main query span since we're done working with it on this thread.
                // // We'll continue it later.
                // defineSpan.parent().stop();
                // }
            }
        }
    }
    
    @Override
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    public GenericResponse<String> createQuery(@Required("logicName") String queryLogicName, MultivaluedMap<String,String> queryParameters) {
        return createQuery(queryLogicName, queryParameters, null);
    }
    
    /**
     * @param queryLogicName
     * @param queryParameters
     * @return
     */
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/{logicName}/create")
    @GZIP
    @GenerateQuerySessionId(cookieBasePath = "/DataWave/Query/")
    @EnrichQueryMetrics(methodType = MethodType.CREATE)
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @Timed(name = "dw.query.createQuery", absolute = true)
    public GenericResponse<String> createQuery(@Required("logicName") @PathParam("logicName") String queryLogicName,
                    MultivaluedMap<String,String> queryParameters, @Context HttpHeaders httpHeaders) {
        CreateQuerySessionIDFilter.QUERY_ID.set(null);
        
        QueryData qd = validateQuery(queryLogicName, queryParameters, httpHeaders);
        
        GenericResponse<String> response = new GenericResponse<>();
        
        Query q = null;
        Connector connection = null;
        AccumuloConnectionFactory.Priority priority;
        Span createSpan = null;
        RunningQuery rq = null;
        try {
            // Default hasResults to true. If a query logic is actually able to set this value,
            // then their value will overwrite this one. Otherwise, we return true so that
            // callers know they have to call next (even though next may not return results).
            response.setHasResults(true);
            
            AuditType auditType = qd.logic.getAuditType(null);
            try {
                MultivaluedMap<String,String> optionalQueryParameters = qp.getUnknownParameters(queryParameters);
                q = persister.create(qd.userDn, qd.dnList, marking, queryLogicName, qp, optionalQueryParameters);
                auditType = qd.logic.getAuditType(q);
            } finally {
                queryParameters.add(AuditParameters.QUERY_AUDIT_TYPE, auditType.name());
                
                // audit the query before its executed.
                if (!auditType.equals(AuditType.NONE)) {
                    try {
                        auditParameters.clear();
                        auditParameters.validate(queryParameters);
                        try {
                            auditParameters.setSelectors(qd.logic.getSelectors(q));
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
                        log.debug("sending audit message: " + auditParameters);
                        auditor.audit(auditParameters);
                    } catch (IllegalArgumentException e) {
                        log.error("Error validating audit parameters", e);
                        BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.MISSING_REQUIRED_PARAMETER, e);
                        response.addException(qe);
                        throw new BadRequestException(qe, response);
                    } catch (Exception e) {
                        log.error("Error auditing query", e);
                        response.addMessage("Error auditing query - " + e.getMessage());
                    }
                }
            }
            
            priority = qd.logic.getConnectionPriority();
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            addQueryToTrackingMap(trackingMap, q);
            accumuloConnectionRequestBean.requestBegin(q.getId().toString());
            try {
                connection = connectionFactory.getConnection(qd.logic.getConnPoolName(), priority, trackingMap);
            } finally {
                accumuloConnectionRequestBean.requestEnd(q.getId().toString());
            }
            // If we're supposed to trace this query, then turn tracing on and set information about the query
            // onto the span so that it is saved in the trace table.
            TInfo traceInfo = null;
            boolean shouldTraceQuery = shouldTraceQuery(qp.getQuery(), qd.userid, qp.isTrace());
            if (shouldTraceQuery) {
                Span span = Trace.on("query:" + q.getId().toString());
                log.debug("Tracing query " + q.getId() + " [" + qp.getQuery() + "] on trace ID " + Long.toHexString(span.traceId()));
                for (Entry<String,List<String>> param : queryParameters.entrySet()) {
                    span.data(param.getKey(), param.getValue().get(0));
                }
                traceInfo = Tracer.traceInfo();
                
                createSpan = Trace.start("query:create");
            }
            
            // hold on to a reference of the query logic so we cancel it if need be.
            qlCache.add(q.getId().toString(), qd.userid, qd.logic, connection);
            rq = new RunningQuery(metrics, null, priority, qd.logic, q, qp.getAuths(), qd.p, new RunningQueryTimingImpl(queryExpirationConf,
                            qp.getPageTimeout()), this.executor, this.predictor, this.metricFactory);
            rq.setActiveCall(true);
            rq.setTraceInfo(traceInfo);
            rq.getMetric().setProxyServers(qd.proxyServers);
            rq.setConnection(connection);
            
            // Put in the cache by id. Don't put the cache in by name because multiple users may use the same name
            // and only the last one will be in the cache.
            queryCache.put(q.getId().toString(), rq);
            
            response.setResult(q.getId().toString());
            rq.setActiveCall(false);
            CreateQuerySessionIDFilter.QUERY_ID.set(q.getId().toString());
            return response;
        } catch (Throwable t) {
            response.setHasResults(false);
            
            if (rq != null) {
                rq.getMetric().setError(t);
            }
            
            // close the logic on exception
            try {
                if (null != qd.logic) {
                    qd.logic.close();
                }
            } catch (Exception e) {
                log.error("Error closing query logic", e);
                response.addException(new QueryException(DatawaveErrorCode.CLOSE_ERROR, e).getBottomQueryException());
            }
            
            if (null != connection) {
                try {
                    connectionFactory.returnConnection(connection);
                } catch (Exception e) {
                    log.error("Error returning connection on failed create", e);
                }
            }
            try {
                if (null != q)
                    persister.remove(q);
            } catch (Exception e) {
                response.addException(new QueryException(DatawaveErrorCode.DEPERSIST_ERROR, e).getBottomQueryException());
            }
            
            /*
             * Allow web services to throw their own WebApplicationExceptions
             */
            if (t instanceof Error && !(t instanceof TokenMgrError)) {
                log.error(t.getMessage(), t);
                throw (Error) t;
            } else if (t instanceof WebApplicationException) {
                log.error(t.getMessage(), t);
                throw ((WebApplicationException) t);
            } else if (t instanceof InterruptedException) {
                if (rq != null) {
                    rq.getMetric().setLifecycle(QueryMetric.Lifecycle.CANCELLED);
                }
                log.info("Query " + q.getId().toString() + " canceled on request");
                QueryException qe = new QueryException(DatawaveErrorCode.QUERY_CANCELED, t);
                response.addException(qe.getBottomQueryException());
                int statusCode = qe.getBottomQueryException().getStatusCode();
                throw new DatawaveWebApplicationException(qe, response, statusCode);
            } else {
                log.error(t.getMessage(), t);
                QueryException qe = new QueryException(DatawaveErrorCode.RUNNING_QUERY_CACHE_ERROR, t);
                response.addException(qe.getBottomQueryException());
                int statusCode = qe.getBottomQueryException().getStatusCode();
                throw new DatawaveWebApplicationException(qe, response, statusCode);
            }
        } finally {
            if (createSpan != null) {
                createSpan.stop();
                // TODO: not sure this makes any sense anymore in Accumulo 1.8.1
                // Stop the main query span since we're done working with it on this thread.
                // We'll continue it later.
                // createSpan.parent().stop();
            }
            if (null != q) {
                // - Remove the logic from the cache
                qlCache.poll(q.getId().toString());
            }
        }
    }
    
    /**
     * @param queryLogicName
     * @param queryParameters
     * @return query predictions
     */
    @POST
    @Path("/{logicName}/predict")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @Timed(name = "dw.query.predictQuery", absolute = true)
    public GenericResponse<String> predictQuery(@Required("logicName") @PathParam("logicName") String queryLogicName,
                    MultivaluedMap<String,String> queryParameters) {
        
        CreateQuerySessionIDFilter.QUERY_ID.set(null);
        
        QueryData qd = validateQuery(queryLogicName, queryParameters, null);
        
        GenericResponse<String> response = new GenericResponse<>();
        
        if (predictor != null) {
            try {
                qp.setPersistenceMode(QueryPersistence.TRANSIENT);
                MultivaluedMap<String,String> optionalQueryParameters = qp.getUnknownParameters(queryParameters);
                Query q = persister.create(qd.userDn, qd.dnList, marking, queryLogicName, qp, optionalQueryParameters);
                
                BaseQueryMetric metric = metricFactory.createMetric();
                q.populateMetric(metric);
                metric.setQueryType(RunningQuery.class.getSimpleName());
                
                Set<Prediction> predictions = predictor.predict(metric);
                if (predictions != null && !predictions.isEmpty()) {
                    String predictionString = predictions.toString();
                    // now we have a predictions, lets broadcast
                    log.info("Model predictions: " + predictionString);
                    response.setHasResults(true);
                    response.setResult(predictionString);
                } else {
                    response.setHasResults(false);
                }
            } catch (Throwable t) {
                response.setHasResults(false);
                
                /*
                 * Allow web services to throw their own WebApplicationExceptions
                 */
                
                if (t instanceof Error && !(t instanceof TokenMgrError)) {
                    log.error(t.getMessage(), t);
                    throw (Error) t;
                } else if (t instanceof WebApplicationException) {
                    log.error(t.getMessage(), t);
                    throw ((WebApplicationException) t);
                } else {
                    log.error(t.getMessage(), t);
                    QueryException qe = new QueryException(DatawaveErrorCode.QUERY_PREDICTIONS_ERROR, t);
                    response.addException(qe.getBottomQueryException());
                    int statusCode = qe.getBottomQueryException().getStatusCode();
                    throw new DatawaveWebApplicationException(qe, response, statusCode);
                }
                
            }
        } else {
            response.setHasResults(false);
        }
        return response;
    }
    
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/{logicName}/async/create")
    @GZIP
    @GenerateQuerySessionId(cookieBasePath = "/DataWave/Query/")
    @EnrichQueryMetrics(methodType = MethodType.CREATE)
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @Asynchronous
    @Timed(name = "dw.query.createQueryAsync", absolute = true)
    public void createQueryAsync(@Required("logicName") @PathParam("logicName") String queryLogicName, MultivaluedMap<String,String> queryParameters,
                    @Suspended AsyncResponse asyncResponse) {
        try {
            GenericResponse<String> response = createQuery(queryLogicName, queryParameters);
            asyncResponse.resume(response);
        } catch (Throwable t) {
            asyncResponse.resume(t);
        }
    }
    
    private List<RunningQuery> getQueryByName(String name) throws Exception {
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String userid = p.getName();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            userid = dp.getShortName();
        }
        log.trace(userid + " has authorizations " + ((p instanceof DatawavePrincipal) ? ((DatawavePrincipal) p).getAuthorizations() : ""));
        List<RunningQuery> results = new ArrayList<>();
        
        List<Query> queries = persister.findByName(name);
        if (null == queries)
            throw new NotFoundQueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH);
        
        for (Query q : queries) {
            // Check to make sure that this query belongs to current user.
            // Not sure this can ever happen with the current persister. It scans with a range set to your userid, so you
            // never get back other people queries. Leaving for now just in case the persister changes.
            if (!q.getOwner().equals(userid)) {
                throw new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}", userid, q.getOwner()));
            }
            
            // will throw IllegalArgumentException if not defined
            QueryLogic<?> logic = queryLogicFactory.getQueryLogic(q.getQueryLogicName(), p);
            AccumuloConnectionFactory.Priority priority = logic.getConnectionPriority();
            RunningQuery query = new RunningQuery(metrics, null, priority, logic, q, q.getQueryAuthorizations(), p, new RunningQueryTimingImpl(
                            queryExpirationConf, qp.getPageTimeout()), this.executor, this.predictor, this.metricFactory);
            results.add(query);
            // Put in the cache by id if its not already in the cache.
            if (!queryCache.containsKey(q.getId().toString()))
                queryCache.put(q.getId().toString(), query);
        }
        return results;
    }
    
    private RunningQuery getQueryById(String id) throws Exception {
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String userid = p.getName();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            userid = dp.getShortName();
        }
        log.trace(userid + " has authorizations " + ((p instanceof DatawavePrincipal) ? ((DatawavePrincipal) p).getAuthorizations() : ""));
        
        RunningQuery query = queryCache.get(id);
        if (null == query) {
            log.info("Query not found in cache, retrieving from accumulo");
            List<Query> queries = persister.findById(id);
            if (null == queries || queries.isEmpty())
                throw new NotFoundQueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH);
            if (queries.size() > 1)
                throw new NotFoundQueryException(DatawaveErrorCode.TOO_MANY_QUERY_OBJECT_MATCHES);
            else {
                Query q = queries.get(0);
                
                // will throw IllegalArgumentException if not defined
                QueryLogic<?> logic = queryLogicFactory.getQueryLogic(q.getQueryLogicName(), p);
                AccumuloConnectionFactory.Priority priority = logic.getConnectionPriority();
                query = new RunningQuery(metrics, null, priority, logic, q, q.getQueryAuthorizations(), p, new RunningQueryTimingImpl(queryExpirationConf,
                                qp.getPageTimeout()), this.executor, this.predictor, this.metricFactory);
                // Put in the cache by id and name, we will have two copies that reference the same object
                queryCache.put(q.getId().toString(), query);
            }
        } else {
            // Check to make sure that this query belongs to current user.
            if (!query.getSettings().getOwner().equals(userid)) {
                throw new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}", userid, query.getSettings()
                                .getOwner()));
            }
        }
        return query;
    }
    
    private RunningQuery adminGetQueryById(String id) throws Exception {
        RunningQuery query = queryCache.get(id);
        
        if (query == null) {
            log.info("Query not found in cache, retrieving from accumulo");
            List<Query> queries = persister.adminFindById(id);
            
            if (queries == null || queries.isEmpty())
                throw new NotFoundQueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH);
            if (queries.size() > 1)
                throw new NotFoundQueryException(DatawaveErrorCode.TOO_MANY_QUERY_OBJECT_MATCHES);
            
            Query q = queries.get(0);
            
            final String auths = q.getQueryAuthorizations();
            
            // will throw IllegalArgumentException if not defined
            final QueryLogic<?> logic = queryLogicFactory.getQueryLogic(q.getQueryLogicName(), ctx.getCallerPrincipal());
            final AccumuloConnectionFactory.Priority priority = logic.getConnectionPriority();
            query = RunningQuery.createQueryWithAuthorizations(metrics, null, priority, logic, q, auths,
                            new RunningQueryTimingImpl(queryExpirationConf, qp.getPageTimeout()), this.executor, this.predictor, this.metricFactory);
            
            // Put in the cache by id and name, we will have two copies that reference the same object
            queryCache.put(q.getId().toString(), query);
        }
        
        return query;
    }
    
    /**
     * Resets the query named by {@code id}. If the query is not alive, meaning that the current session has expired (due to either timeout, or server failure),
     * then this will reload the query and start it over. If the query is alive, it closes it and starts the query over.
     *
     * @param id
     *            the ID of the query to reload/reset
     * @return an empty response
     *
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader query-session-id this header and value will be in the Set-Cookie header, subsequent calls for this session will need to supply the
     *                 query-session-id header in the request in a Cookie header or as a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 400 invalid or missing parameter
     * @HTTP 500 internal server error
     */
    @PUT
    @POST
    @Path("/{id}/reset")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @GZIP
    @GenerateQuerySessionId(cookieBasePath = "/DataWave/Query/")
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    @TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
    @Override
    @Timed(name = "dw.query.reset", absolute = true)
    public VoidResponse reset(@Required("id") @PathParam("id") String id) {
        CreateQuerySessionIDFilter.QUERY_ID.set(null);
        VoidResponse response = new VoidResponse();
        AccumuloConnectionFactory.Priority priority;
        
        Connector connection = null;
        RunningQuery query = null;
        Span span = null;
        
        try {
            ctx.getUserTransaction().begin();
            
            query = getQueryById(id);
            
            // If we're tracing this query, then continue the trace for the reset call.
            TInfo traceInfo = query.getTraceInfo();
            if (traceInfo != null) {
                span = Trace.trace(traceInfo, "query:reset");
            }
            
            // Lock this so that this query cannot be used concurrently.
            // The lock should be released at the end of the method call.
            if (!queryCache.lock(id))
                throw new QueryException(DatawaveErrorCode.QUERY_LOCKED_ERROR);
            
            // We did not allocate a connection when we looked up the query. If
            // there's a connection when we get here, then we know it can only be
            // because the query was alive and in use, so we need to close that
            // connection in order to reset the query. Otherwise, we are truly
            // restarting the query, so we should re-audit ().
            if (query.getConnection() != null) {
                query.closeConnection(connectionFactory);
            } else {
                AuditType auditType = query.getLogic().getAuditType(query.getSettings());
                
                MultivaluedMap<String,String> queryParameters = query.getSettings().toMap();
                queryParameters.add("logicClass", query.getLogic().getLogicName());
                queryParameters.add(AuditParameters.USER_DN, query.getSettings().getUserDN());
                queryParameters.add(AuditParameters.QUERY_AUDIT_TYPE, auditType.name());
                queryParameters.add(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, query.getSettings().getColumnVisibility());
                
                if (!auditType.equals(AuditType.NONE)) {
                    try {
                        try {
                            auditParameters.clear();
                            auditParameters.validate(queryParameters);
                        } catch (IllegalArgumentException e) {
                            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.MISSING_REQUIRED_PARAMETER, e);
                            response.addException(qe);
                            throw new BadRequestException(qe, response);
                        }
                        try {
                            auditParameters.setSelectors(query.getLogic().getSelectors(query.getSettings()));
                        } catch (Exception e) {
                            log.error(e.getMessage());
                        }
                        auditor.audit(auditParameters);
                    } catch (Exception e) {
                        log.error("Error auditing query", e);
                        response.addMessage("Error auditing query - " + e.getMessage());
                    }
                }
            }
            
            // Allocate the connection for this query so we are ready to go when
            // they call next.
            priority = query.getConnectionPriority();
            Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
            addQueryToTrackingMap(trackingMap, query.getSettings());
            accumuloConnectionRequestBean.requestBegin(id);
            try {
                connection = connectionFactory.getConnection(query.getLogic().getConnPoolName(), priority, trackingMap);
            } finally {
                accumuloConnectionRequestBean.requestEnd(id);
            }
            query.setConnection(connection);
            response.addMessage(id + " reset.");
            CreateQuerySessionIDFilter.QUERY_ID.set(id);
            return response;
        } catch (InterruptedException e) {
            if (query != null) {
                query.getMetric().setLifecycle(QueryMetric.Lifecycle.CANCELLED);
            }
            log.info("Query " + id + " canceled on request");
            QueryException qe = new QueryException(DatawaveErrorCode.QUERY_CANCELED, e);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        } catch (Exception e) {
            log.error("Exception caught on resetting query", e);
            try {
                if (null != connection) {
                    /*
                     * if the query exists, we need to make sure the connection isn't set on it because the "proper" work flow is to close and/or cancel the
                     * query after a failure. we don't want to purge it from the query cache, so setting the connector to null avoids having the connector
                     * returned multiple times to the connector pool.
                     */
                    if (query != null) {
                        query.setConnection(null);
                    }
                    connectionFactory.returnConnection(connection);
                }
            } catch (Exception e2) {
                log.error("Error returning connection on failed reset", e2);
            }
            QueryException qe = new QueryException(DatawaveErrorCode.QUERY_RESET_ERROR, e);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        } finally {
            queryCache.unlock(id);
            try {
                if (ctx.getUserTransaction().getStatus() != Status.STATUS_NO_TRANSACTION) {
                    // no reason to commit if transaction not started, ie Query not found exception
                    ctx.getUserTransaction().commit();
                }
            } catch (Exception e) {
                QueryException qe = new QueryException(DatawaveErrorCode.QUERY_TRANSACTION_ERROR, e);
                response.addException(qe.getBottomQueryException());
                throw new DatawaveWebApplicationException(qe, response);
            } finally {
                // Stop timing on this trace, if any
                if (span != null) {
                    span.stop();
                }
            }
        }
        
    }
    
    @Override
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    @TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
    public BaseQueryResponse createQueryAndNext(String logicName, MultivaluedMap<String,String> queryParameters) {
        return createQueryAndNext(logicName, queryParameters, null);
    }
    
    /**
     * @param logicName
     * @param queryParameters
     * @return
     */
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/{logicName}/createAndNext")
    @GZIP
    @GenerateQuerySessionId(cookieBasePath = "/DataWave/Query/")
    @EnrichQueryMetrics(methodType = MethodType.CREATE_AND_NEXT)
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    @TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
    @Timed(name = "dw.query.createAndNext", absolute = true)
    public BaseQueryResponse createQueryAndNext(@PathParam("logicName") String logicName, MultivaluedMap<String,String> queryParameters,
                    @Context HttpHeaders httpHeaders) {
        CreateQuerySessionIDFilter.QUERY_ID.set(null);
        
        GenericResponse<String> createResponse = createQuery(logicName, queryParameters, httpHeaders);
        String queryId = createResponse.getResult();
        try {
            CreateQuerySessionIDFilter.QUERY_ID.set(queryId);
            return next(queryId, false);
        } catch (NoResultsException e) {
            close(queryId);
            throw e;
        } catch (EJBException e) {
            if (e.getCause() instanceof NoResultsException) {
                close(queryId);
            }
            throw e;
        }
    }
    
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/{logicName}/async/createAndNext")
    @GZIP
    @GenerateQuerySessionId(cookieBasePath = "/DataWave/Query/")
    @EnrichQueryMetrics(methodType = MethodType.CREATE_AND_NEXT)
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    @TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
    @Asynchronous
    @Timed(name = "dw.query.createAndNextAsync", absolute = true)
    public void createQueryAndNextAsync(@PathParam("logicName") String logicName, MultivaluedMap<String,String> queryParameters,
                    @Suspended AsyncResponse asyncResponse) {
        try {
            BaseQueryResponse response = createQueryAndNext(logicName, queryParameters);
            asyncResponse.resume(response);
        } catch (Throwable t) {
            asyncResponse.resume(t);
        }
    }
    
    private BaseQueryResponse _next(RunningQuery query, String queryId, Collection<String> proxyServers, Span span) throws Exception {
        // If we're tracing this query, then continue the trace for the next call.
        TInfo traceInfo = query.getTraceInfo();
        if (traceInfo != null) {
            span = Trace.trace(traceInfo, "query:next");
        }
        
        ResultsPage resultList;
        try {
            resultList = query.next();
        } catch (RejectedExecutionException e) {
            // - race condition, query expired while user called next
            throw new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_TIMEOUT_OR_SERVER_ERROR, e, MessageFormat.format("id = {0}", queryId));
        }
        
        long pageNum = query.getLastPageNumber();
        
        BaseQueryResponse response = query.getLogic().getTransformer(query.getSettings()).createResponse(resultList);
        if (resultList.getResults().size() > 0) {
            response.setHasResults(true);
        } else {
            response.setHasResults(false);
        }
        response.setPageNumber(pageNum);
        response.setLogicName(query.getLogic().getLogicName());
        response.setQueryId(queryId);
        
        if (span != null) {
            span.data("pageNumber", Long.toString(pageNum));
        }
        
        query.getMetric().setProxyServers(proxyServers);
        
        testForUncaughtException(query.getSettings(), resultList);
        
        if (resultList.getResults().size() == 0) {
            NoResultsQueryException qe = new NoResultsQueryException(DatawaveErrorCode.NO_QUERY_RESULTS_FOUND, MessageFormat.format("{0}", queryId));
            response.addException(qe);
            throw new NoResultsException(qe);
        } else {
            return response;
        }
        
    }
    
    /**
     *
     * @param uuid
     * @param uuidType
     * @param uriInfo
     * @param httpHeaders
     * @return content results, either as a paged BaseQueryResponse or StreamingOutput
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader query-session-id this header and value will be in the Set-Cookie header, subsequent calls for this session will need to supply the
     *                 query-session-id header in the request in a Cookie header or as a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @ResponseHeader X-Partial-Results true if the page contains less than the requested number of results
     *
     * @HTTP 200 success
     * @HTTP 204 success and no results
     * @HTTP 400 invalid or missing parameter
     * @HTTP 500 internal server error
     */
    @GET
    @Path("/lookupContentUUID/{uuidType}/{uuid}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @Override
    @Timed(name = "dw.query.lookupContentUUID", absolute = true)
    public <T> T lookupContentByUUID(@Required("uuidType") @PathParam("uuidType") String uuidType, @Required("uuid") @PathParam("uuid") String uuid,
                    @Context UriInfo uriInfo, @Required("httpHeaders") @Context HttpHeaders httpHeaders) {
        MultivaluedMapImpl<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putAll(uriInfo.getQueryParameters());
        return this.lookupContentByUUID(uuidType, uuid, queryParameters, httpHeaders);
    }
    
    private <T> T lookupContentByUUID(String uuidType, String uuid, MultivaluedMap<String,String> queryParameters, HttpHeaders httpHeaders) {
        T response = null;
        String queryId = null;
        try {
            String streaming = queryParameters.getFirst("streaming");
            boolean streamingOutput = false;
            if (!StringUtils.isEmpty(streaming)) {
                streamingOutput = Boolean.parseBoolean(streaming);
            }
            // Create the criteria for looking up the respective events, which we need to get the shard IDs and column families
            // required for the content lookup
            final UUIDType matchingType = this.lookupUUIDUtil.getUUIDType(uuidType.toUpperCase());
            final GetUUIDCriteria criteria;
            final String view = (null != matchingType) ? matchingType.getDefinedView() : null;
            if ((LookupUUIDUtil.UID_QUERY.equals(view) || LookupUUIDUtil.LOOKUP_UID_QUERY.equals(view))) {
                criteria = new UIDQueryCriteria(uuid, uuidType, queryParameters);
            } else {
                criteria = new GetUUIDCriteria(uuid, uuidType, queryParameters);
            }
            
            // Set the HTTP headers if a streamed response is required
            if (streamingOutput) {
                criteria.setStreamingOutputHeaders(httpHeaders);
            }
            
            response = this.lookupUUIDUtil.lookupContentByUUIDs(criteria);
            if (response instanceof BaseQueryResponse) {
                queryId = ((BaseQueryResponse) response).getQueryId();
            }
            return response;
        } finally {
            if (null != queryId) {
                close(queryId);
            }
        }
    }
    
    /**
     *
     * @param queryParameters
     * @param httpHeaders
     * @return content results, either as a paged BaseQueryResponse or StreamingOutput
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader query-session-id this header and value will be in the Set-Cookie header, subsequent calls for this session will need to supply the
     *                 query-session-id header in the request in a Cookie header or as a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @ResponseHeader X-Partial-Results true if the page contains less than the requested number of results
     *
     * @HTTP 200 success
     * @HTTP 204 success and no results
     * @HTTP 400 invalid or missing parameter
     * @HTTP 500 internal server error
     */
    @POST
    @Path("/lookupContentUUID")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @GZIP
    @GenerateQuerySessionId(cookieBasePath = "/DataWave/Query/")
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    @Override
    @Timed(name = "dw.query.lookupContentUUIDBatch", absolute = true)
    public <T> T lookupContentByUUIDBatch(MultivaluedMap<String,String> queryParameters, HttpHeaders httpHeaders) {
        if (!queryParameters.containsKey("uuidPairs")) {
            throw new BadRequestException(new IllegalArgumentException("uuidPairs missing from query parameters"), new VoidResponse());
        }
        T response = null;
        String queryId = null;
        try {
            String uuidPairs = queryParameters.getFirst("uuidPairs");
            String streaming = queryParameters.getFirst("streaming");
            boolean streamingOutput = false;
            if (!StringUtils.isEmpty(streaming)) {
                streamingOutput = Boolean.parseBoolean(streaming);
            }
            // Create the criteria for looking up the respective events, which we need to get the shard IDs and column families
            // required for the content lookup
            final PostUUIDCriteria criteria = new PostUUIDCriteria(uuidPairs, queryParameters);
            
            // Set the HTTP headers if a streamed response is required
            if (streamingOutput) {
                criteria.setStreamingOutputHeaders(httpHeaders);
            }
            
            response = this.lookupUUIDUtil.lookupContentByUUIDs(criteria);
            if (response instanceof BaseQueryResponse) {
                queryId = ((BaseQueryResponse) response).getQueryId();
            }
            return response;
        } finally {
            if (null != queryId) {
                close(queryId);
            }
        }
    }
    
    /**
     *
     * @param uuidType
     * @param uuid
     * @param uriInfo
     * @param httpHeaders
     * @return
     * @return event results, either as a paged BaseQueryResponse (automatically closed upon return) or StreamingOutput
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader query-session-id this header and value will be in the Set-Cookie header, subsequent calls for this session will need to supply the
     *                 query-session-id header in the request in a Cookie header or as a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @ResponseHeader X-Partial-Results true if the page contains less than the requested number of results
     *
     * @HTTP 200 success
     * @HTTP 204 success and no results
     * @HTTP 400 invalid or missing parameter
     * @HTTP 500 internal server error
     */
    @GET
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/lookupUUID/{uuidType}/{uuid}")
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @Override
    @Timed(name = "dw.query.lookupUUID", absolute = true)
    public <T> T lookupUUID(@Required("uuidType") @PathParam("uuidType") String uuidType, @Required("uuid") @PathParam("uuid") String uuid,
                    @Context UriInfo uriInfo, @Required("httpHeaders") @Context HttpHeaders httpHeaders) {
        MultivaluedMapImpl<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putAll(uriInfo.getQueryParameters());
        return this.lookupUUID(uuidType, uuid, queryParameters, httpHeaders);
    }
    
    <T> T lookupUUID(String uuidType, String uuid, MultivaluedMap<String,String> queryParameters, HttpHeaders httpHeaders) {
        String streaming = queryParameters.getFirst("streaming");
        boolean streamingOutput = false;
        if (!StringUtils.isEmpty(streaming)) {
            streamingOutput = Boolean.parseBoolean(streaming);
        }
        final UUIDType matchingType = this.lookupUUIDUtil.getUUIDType(uuidType);
        String queryId = null;
        T response;
        try {
            // Construct the criteria used to perform the query
            final GetUUIDCriteria criteria;
            final String view = (null != matchingType) ? matchingType.getDefinedView() : null;
            if ((LookupUUIDUtil.UID_QUERY.equals(view) || LookupUUIDUtil.LOOKUP_UID_QUERY.equals(view))) {
                criteria = new UIDQueryCriteria(uuid, uuidType, queryParameters);
            } else {
                criteria = new GetUUIDCriteria(uuid, uuidType, queryParameters);
            }
            
            // Add the HTTP headers in case streaming is required
            if (streamingOutput) {
                criteria.setStreamingOutputHeaders(httpHeaders);
            }
            
            // Perform the query and get the first set of results
            response = this.lookupUUIDUtil.createUUIDQueryAndNext(criteria);
            if (response instanceof BaseQueryResponse) {
                queryId = ((BaseQueryResponse) response).getQueryId();
            }
            return response;
        } finally {
            if (null != queryId) {
                this.close(queryId);
            }
        }
    }
    
    /**
     *
     * @param queryParameters
     * @param httpHeaders
     * @return event results, either as a paged BaseQueryResponse or StreamingOutput
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader query-session-id this header and value will be in the Set-Cookie header, subsequent calls for this session will need to supply the
     *                 query-session-id header in the request in a Cookie header or as a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @ResponseHeader X-Partial-Results true if the page contains less than the requested number of results
     *
     * @HTTP 200 success
     * @HTTP 204 success and no results
     * @HTTP 400 invalid or missing parameter
     * @HTTP 500 internal server error
     */
    @POST
    @Path("/lookupUUID")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @GZIP
    @GenerateQuerySessionId(cookieBasePath = "/DataWave/Query/")
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    @Override
    @Timed(name = "dw.query.lookupUUIDBatch", absolute = true)
    public <T> T lookupUUIDBatch(MultivaluedMap<String,String> queryParameters, @Required("httpHeaders") @Context HttpHeaders httpHeaders) {
        if (!queryParameters.containsKey("uuidPairs")) {
            throw new BadRequestException(new IllegalArgumentException("uuidPairs missing from query parameters"), new VoidResponse());
        }
        T response;
        String queryId = null;
        try {
            String uuidPairs = queryParameters.getFirst("uuidPairs");
            String streaming = queryParameters.getFirst("streaming");
            boolean streamingOutput = false;
            if (!StringUtils.isEmpty(streaming)) {
                streamingOutput = Boolean.parseBoolean(streaming);
            }
            final PostUUIDCriteria criteria = new PostUUIDCriteria(uuidPairs, queryParameters);
            if (streamingOutput) {
                criteria.setStreamingOutputHeaders(httpHeaders);
            }
            response = this.lookupUUIDUtil.createUUIDQueryAndNext(criteria);
            if (response instanceof BaseQueryResponse) {
                queryId = ((BaseQueryResponse) response).getQueryId();
            }
            return response;
        } finally {
            if (null != queryId) {
                close(queryId);
            }
        }
        
    }
    
    /**
     * Pulls back the current plan for a query.
     *
     * @param id
     *            - (@Required)
     *
     * @return GenericResponse containing plan
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @RequestHeader query-session-id session id value used for load balancing purposes. query-session-id can be placed in the request in a Cookie header or as
     *                a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 204 success and no results
     * @HTTP 404 if id not found
     * @HTTP 412 if the query is no longer alive, client should call {@link #reset(String)} and try again
     * @HTTP 500 internal server error
     */
    @GET
    @Path("/{id}/plan")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @GZIP
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    @Override
    @Timed(name = "dw.query.plan", absolute = true)
    public GenericResponse<String> plan(@Required("id") @PathParam("id") String id) {
        // in case we don't make it to creating the response from the QueryLogic
        GenericResponse<String> response = new GenericResponse<>();
        
        Principal p = ctx.getCallerPrincipal();
        String userid = p.getName();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            userid = dp.getShortName();
        }
        
        try {
            // Not calling getQueryById() here. We don't want to pull the persisted definition.
            RunningQuery query = queryCache.get(id);
            
            // When we pulled the query from the cache, we told it not to allocate a connection.
            // So if the connection is null here, then either the query wasn't in the cache
            // at all, or it was but only because of a call to list. In either case, it's
            // an error.
            if (null == query || null == query.getConnection()) {
                // If the query just wasn't in the cache, then check the persister to see if the
                // ID exists at all. If it doesn't, then we need to return a 404 rather than 412
                // status code.
                if (null == query) {
                    List<Query> queries = persister.findById(id);
                    if (queries == null || queries.size() != 1) {
                        throw new NotFoundQueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH, MessageFormat.format("{0}", id));
                    }
                }
                
                throw new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_TIMEOUT_OR_SERVER_ERROR, MessageFormat.format("id = {0}", id));
            } else {
                // Validate the query belongs to the caller
                if (!query.getSettings().getOwner().equals(userid)) {
                    throw new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}", userid, query.getSettings()
                                    .getOwner()));
                }
                
                // pull the plan out of the query metric
                String plan = query.getMetric().getPlan();
                if (plan != null) {
                    response.setResult(plan);
                    response.setHasResults(true);
                }
            }
        } catch (Exception e) {
            log.error("Failed to get query plan", e);
            
            QueryException qe = new QueryException(DatawaveErrorCode.QUERY_PLAN_ERROR, e, MessageFormat.format("query id: {0}", id));
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
        
        return response;
    }
    
    /**
     * Pulls back the current predictions for a query.
     *
     * @param id
     *            - (@Required)
     *
     * @return GenericResponse containing predictions
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @RequestHeader query-session-id session id value used for load balancing purposes. query-session-id can be placed in the request in a Cookie header or as
     *                a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 204 success and no results
     * @HTTP 404 if id not found
     * @HTTP 412 if the query is no longer alive, client should call {@link #reset(String)} and try again
     * @HTTP 500 internal server error
     */
    @GET
    @Path("/{id}/predictions")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @GZIP
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    @Override
    @Timed(name = "dw.query.predictions", absolute = true)
    public GenericResponse<String> predictions(@Required("id") @PathParam("id") String id) {
        // in case we don't make it to creating the response from the QueryLogic
        GenericResponse<String> response = new GenericResponse<>();
        
        Principal p = ctx.getCallerPrincipal();
        String userid = p.getName();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            userid = dp.getShortName();
        }
        
        try {
            // Not calling getQueryById() here. We don't want to pull the persisted definition.
            RunningQuery query = queryCache.get(id);
            
            // When we pulled the query from the cache, we told it not to allocate a connection.
            // So if the connection is null here, then either the query wasn't in the cache
            // at all, or it was but only because of a call to list. In either case, it's
            // an error.
            if (null == query || null == query.getConnection()) {
                // If the query just wasn't in the cache, then check the persister to see if the
                // ID exists at all. If it doesn't, then we need to return a 404 rather than 412
                // status code.
                if (null == query) {
                    List<Query> queries = persister.findById(id);
                    if (queries == null || queries.size() != 1) {
                        throw new NotFoundQueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH, MessageFormat.format("{0}", id));
                    }
                }
                
                throw new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_TIMEOUT_OR_SERVER_ERROR, MessageFormat.format("id = {0}", id));
            } else {
                // Validate the query belongs to the caller
                if (!query.getSettings().getOwner().equals(userid)) {
                    throw new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}", userid, query.getSettings()
                                    .getOwner()));
                }
                
                // pull the predictions out of the query metric
                Set<Prediction> predictions = query.getMetric().getPredictions();
                if (predictions != null && !predictions.isEmpty()) {
                    response.setResult(predictions.toString());
                    response.setHasResults(true);
                }
            }
        } catch (Exception e) {
            log.error("Failed to get query predictions", e);
            
            QueryException qe = new QueryException(DatawaveErrorCode.QUERY_PREDICTIONS_ERROR, e, MessageFormat.format("query id: {0}", id));
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
        
        return response;
    }
    
    /**
     * Asynchronous version of {@link #next(String)}.
     * 
     * @see #next(String)
     */
    @GET
    @Path("/{id}/async/next")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @GZIP
    @EnrichQueryMetrics(methodType = MethodType.NEXT)
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    @TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
    @Asynchronous
    @Timed(name = "dw.query.nextAsync", absolute = true)
    public void nextAsync(@Required("id") @PathParam("id") String id, @Suspended AsyncResponse asyncResponse) {
        try {
            BaseQueryResponse response = next(id);
            asyncResponse.resume(response);
        } catch (Throwable t) {
            asyncResponse.resume(t);
        }
    }
    
    /**
     * Gets the next page of results from the query object. If the object is no longer alive, meaning that the current session has expired, then this fail. The
     * response object type is dynamic, see the listQueryLogic operation to determine what the response type object will be.
     *
     * @param id
     *            - (@Required)
     * @see datawave.webservice.query.runner.QueryExecutorBean#next(String) for the @Required definition
     *
     * @return datawave.webservice.result.BaseQueryResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @RequestHeader query-session-id session id value used for load balancing purposes. query-session-id can be placed in the request in a Cookie header or as
     *                a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @ResponseHeader X-query-page-number page number returned by this call
     * @ResponseHeader X-query-last-page if true then there are no more pages for this query, caller should call close()
     * @ResponseHeader X-Partial-Results true if the page contains less than the requested number of results
     *
     * @HTTP 200 success
     * @HTTP 204 success and no results
     * @HTTP 404 if id not found
     * @HTTP 412 if the query is no longer alive, client should call {@link #reset(String)} and try again
     * @HTTP 500 internal server error
     */
    @GET
    @Path("/{id}/next")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @GZIP
    @EnrichQueryMetrics(methodType = MethodType.NEXT)
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    @TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
    @Override
    @Timed(name = "dw.query.next", absolute = true)
    public BaseQueryResponse next(@Required("id") @PathParam("id") String id) {
        return this.next(id, true);
    }
    
    private BaseQueryResponse next(final String id, boolean checkForContentLookup) {
        // in case we don't make it to creating the response from the QueryLogic
        BaseQueryResponse response = responseObjectFactory.getEventQueryResponse();
        
        Collection<String> proxyServers = null;
        Principal p = ctx.getCallerPrincipal();
        String userid = p.getName();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            userid = dp.getShortName();
            proxyServers = dp.getProxyServers();
        }
        
        Span span = null;
        RunningQuery query = null;
        Query contentLookupSettings = null;
        try {
            
            ctx.getUserTransaction().begin();
            
            // Not calling getQueryById() here. We don't want to pull the persisted definition.
            query = queryCache.get(id);
            
            // Lock this so that this query cannot be used concurrently.
            // The lock should be released at the end of the method call.
            if (!queryCache.lock(id)) {
                throw new QueryException(DatawaveErrorCode.QUERY_LOCKED_ERROR);
            }
            
            // When we pulled the query from the cache, we told it not to allocate a connection.
            // So if the connection is null here, then either the query wasn't in the cache
            // at all, or it was but only because of a call to list. In either case, it's
            // an error.
            if (null == query || null == query.getConnection()) {
                // If the query just wasn't in the cache, then check the persister to see if the
                // ID exists at all. If it doesn't, then we need to return a 404 rather than 412
                // status code.
                if (null == query) {
                    List<Query> queries = persister.findById(id);
                    if (queries == null || queries.size() != 1) {
                        throw new NotFoundQueryException(DatawaveErrorCode.NO_QUERY_OBJECT_MATCH, MessageFormat.format("{0}", id));
                    }
                }
                
                throw new PreConditionFailedQueryException(DatawaveErrorCode.QUERY_TIMEOUT_OR_SERVER_ERROR, MessageFormat.format("id = {0}", id));
            } else {
                // Validate the query belongs to the caller
                if (!query.getSettings().getOwner().equals(userid)) {
                    throw new UnauthorizedQueryException(DatawaveErrorCode.QUERY_OWNER_MISMATCH, MessageFormat.format("{0} != {1}", userid, query.getSettings()
                                    .getOwner()));
                }
                
                // Set the active call and get next
                query.setActiveCall(true);
                response = _next(query, id, proxyServers, span);
                
                // Conditionally swap the standard response with content
                if (checkForContentLookup) {
                    final Query settings = query.getSettings();
                    final Parameter contentLookupParam = settings.findParameter(LookupUUIDUtil.PARAM_CONTENT_LOOKUP);
                    if ((null != contentLookupParam) && Boolean.parseBoolean(contentLookupParam.getParameterValue())) {
                        contentLookupSettings = settings;
                    }
                }
                
                // Unset the active call and return
                query.setActiveCall(false);
            }
        } catch (NoResultsException e) {
            if (query != null) {
                query.setActiveCall(false);
                if (query.getLogic().getCollectQueryMetrics()) {
                    try {
                        // do not set the error message here - zero results is not an error that should be added to metrics
                        metrics.updateMetric(query.getMetric());
                    } catch (Exception e1) {
                        log.error(e1.getMessage());
                    }
                }
            }
            try {
                ctx.getUserTransaction().setRollbackOnly();
            } catch (Exception ex) {
                log.error("Error marking transaction for roll back", ex);
            }
            throw e;
        } catch (DatawaveWebApplicationException e) {
            if (query != null) {
                query.setActiveCall(false);
                if (query.getLogic().getCollectQueryMetrics()) {
                    query.getMetric().setError(e);
                    try {
                        metrics.updateMetric(query.getMetric());
                    } catch (Exception e1) {
                        log.error(e1.getMessage());
                    }
                }
            }
            try {
                ctx.getUserTransaction().setRollbackOnly();
            } catch (Exception ex) {
                log.error("Error marking transaction for roll back", ex);
            }
            throw e;
        } catch (Exception e) {
            log.error("Query Failed", e);
            if (query != null) {
                query.setActiveCall(false);
                if (query.getLogic().getCollectQueryMetrics() == true) {
                    query.getMetric().setError(e);
                    try {
                        metrics.updateMetric(query.getMetric());
                    } catch (Exception e1) {
                        log.error(e1.getMessage());
                    }
                }
            }
            try {
                ctx.getUserTransaction().setRollbackOnly();
            } catch (Exception ex) {
                log.error("Error marking transaction for roll back", ex);
            }
            
            QueryException qe = new QueryException(DatawaveErrorCode.QUERY_NEXT_ERROR, e, MessageFormat.format("query id: {0}", id));
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        } finally {
            queryCache.unlock(id);
            try {
                if (ctx.getUserTransaction().getStatus() == Status.STATUS_MARKED_ROLLBACK) {
                    ctx.getUserTransaction().rollback();
                } else if (ctx.getUserTransaction().getStatus() != Status.STATUS_NO_TRANSACTION) {
                    // no reason to commit if transaction not started, ie Query not found exception
                    ctx.getUserTransaction().commit();
                }
            } catch (IllegalStateException e) {
                log.error("Error committing transaction: thread not associated with transaction", e);
            } catch (RollbackException e) {
                log.error("Error committing transaction: marked for rollback due to error", e);
            } catch (HeuristicMixedException e) {
                log.error("Error committing transaction: partial commit of resources", e);
            } catch (HeuristicRollbackException e) {
                log.error("Error committing transaction: resources rolled back transaction", e);
            } catch (Exception e) {
                log.error("Error committing transaction: Unknown error", e);
            } finally {
                // Stop timing on this trace, if any
                if (span != null) {
                    span.stop();
                }
            }
        }
        
        // If applicable, perform a paged content lookup (i.e., not streamed), replacing its results in the returned response
        if (null != contentLookupSettings) {
            final NextContentCriteria criteria = new NextContentCriteria(id, contentLookupSettings);
            response = this.lookupUUIDUtil.lookupContentByNextResponse(criteria, response);
        }
        
        return response;
    }
    
    /**
     * Releases the resources associated with this query. Any currently running calls to 'next' on the query will continue until they finish. Calls to 'next'
     * after a 'close' will start over at page 1.
     * 
     * @param id
     *
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @RequestHeader query-session-id session id value used for load balancing purposes. query-session-id can be placed in the request in a Cookie header or as
     *                a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 404 queries not found using {@code id}
     * @HTTP 500 internal server error
     */
    @PUT
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/{id}/close")
    @GZIP
    @ClearQuerySessionId
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    @Override
    @Timed(name = "dw.query.close", absolute = true)
    public VoidResponse close(@Required("id") @PathParam("id") String id) {
        VoidResponse response = new VoidResponse();
        try {
            boolean connectionRequestCanceled = accumuloConnectionRequestBean.cancelConnectionRequest(id);
            Pair<QueryLogic<?>,Connector> tuple = qlCache.pollIfOwnedBy(id, ((DatawavePrincipal) ctx.getCallerPrincipal()).getShortName());
            if (tuple == null) {
                try {
                    RunningQuery query = getQueryById(id);
                    close(query);
                } catch (NotFoundQueryException e) {
                    // if connection request was canceled, then the call was successful even if a RunningQuery was not found
                    if (!connectionRequestCanceled) {
                        throw e;
                    }
                }
                response.addMessage(id + " closed.");
            } else {
                QueryLogic<?> logic = tuple.getFirst();
                try {
                    logic.close();
                } catch (Exception e) {
                    log.error("Exception occured while closing query logic; may be innocuous if scanners were running.", e);
                }
                connectionFactory.returnConnection(tuple.getSecond());
                response.addMessage(id + " closed before create completed.");
            }
            return response;
        } catch (DatawaveWebApplicationException e) {
            throw e;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.CLOSE_ERROR, e, MessageFormat.format("query_id: {0}", id));
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
    
    /**
     * <strong>JBossAdministrator or Administrator credentials required.</strong>
     *
     * @param id
     * @return
     */
    @PUT
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/{id}/adminClose")
    @GZIP
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @Override
    public VoidResponse adminClose(@Required("id") @PathParam("id") String id) {
        VoidResponse response = new VoidResponse();
        try {
            boolean connectionRequestCanceled = accumuloConnectionRequestBean.adminCancelConnectionRequest(id);
            Pair<QueryLogic<?>,Connector> tuple = qlCache.poll(id);
            if (tuple == null) {
                try {
                    RunningQuery query = adminGetQueryById(id);
                    close(query);
                } catch (NotFoundQueryException e) {
                    // if connection request was canceled, then the call was successful even if a RunningQuery was not found
                    if (!connectionRequestCanceled) {
                        throw e;
                    }
                }
                response.addMessage(id + " closed.");
            } else {
                QueryLogic<?> logic = tuple.getFirst();
                try {
                    logic.close();
                } catch (Exception e) {
                    log.error("Exception occured while closing query logic; may be innocuous if scanners were running.", e);
                }
                connectionFactory.returnConnection(tuple.getSecond());
                response.addMessage(id + " closed before create completed.");
            }
            return response;
        } catch (DatawaveWebApplicationException e) {
            throw e;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.CLOSE_ERROR);
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
    
    private void close(RunningQuery query) throws Exception {
        
        query.closeConnection(connectionFactory);
        queryCache.remove(query.getSettings().getId().toString());
        
        // The trace was already stopped, but mark the time we closed it in the trace data.
        TInfo traceInfo = query.getTraceInfo();
        if (traceInfo != null) {
            Span span = Trace.trace(traceInfo, "query:close");
            span.data("closedAt", new Date().toString());
            // Spans aren't recorded if they take no time, so sleep for a
            // couple milliseconds just to ensure we get something saved.
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                // ignore
            }
            span.stop();
            // TODO: not sure this makes any sense anymore in Accumulo 1.8.1
            // Tracer.getInstance().flush();
        }
    }
    
    /**
     * Releases the resources associated with this query. Any currently running calls to 'next' on the query will be stopped. Calls to 'next' after a 'cancel'
     * will start over at page 1.
     * 
     * @param id
     *
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @RequestHeader query-session-id session id value used for load balancing purposes. query-session-id can be placed in the request in a Cookie header or as
     *                a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 404 queries not found using {@code id}
     * @HTTP 500 internal server error
     */
    @PUT
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/{id}/cancel")
    @GZIP
    @ClearQuerySessionId
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    @Override
    @Timed(name = "dw.query.cancel", absolute = true)
    public VoidResponse cancel(@Required("id") @PathParam("id") String id) {
        VoidResponse response = new VoidResponse();
        try {
            boolean connectionRequestCanceled = accumuloConnectionRequestBean.cancelConnectionRequest(id);
            Pair<QueryLogic<?>,Connector> tuple = qlCache.pollIfOwnedBy(id, ctx.getCallerPrincipal().getName());
            
            if (tuple == null) {
                try {
                    RunningQuery query = getQueryById(id);
                    query.cancel();
                    close(query);
                } catch (NotFoundQueryException e) {
                    // if connection request was canceled, then the call was successful even if a RunningQuery was not found
                    if (!connectionRequestCanceled) {
                        throw e;
                    }
                }
                response.addMessage(id + " canceled.");
            } else {
                QueryLogic<?> logic = tuple.getFirst();
                try {
                    logic.close();
                } catch (Exception e) {
                    log.error("Exception occured while canceling query logic; may be innocuous if scanners were running.", e);
                }
                connectionFactory.returnConnection(tuple.getSecond());
                response.addMessage(id + " closed before create completed due to cancel.");
            }
            
            return response;
        } catch (DatawaveWebApplicationException e) {
            throw e;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.CANCELLATION_ERROR, e, MessageFormat.format("query_id: {0}", id));
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
    
    /**
     * <strong>JBossAdministrator or Administrator credentials required.</strong>
     *
     * @param id
     * @return
     */
    @PUT
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/{id}/adminCancel")
    @GZIP
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @Override
    public VoidResponse adminCancel(@Required("id") @PathParam("id") String id) {
        VoidResponse response = new VoidResponse();
        try {
            boolean connectionRequestCanceled = accumuloConnectionRequestBean.adminCancelConnectionRequest(id);
            Pair<QueryLogic<?>,Connector> tuple = qlCache.poll(id);
            if (tuple == null) {
                try {
                    RunningQuery query = adminGetQueryById(id);
                    query.cancel();
                    close(query);
                } catch (NotFoundQueryException e) {
                    // if connection request was canceled, then the call was successful even if a RunningQuery was not found
                    if (!connectionRequestCanceled) {
                        throw e;
                    }
                }
                response.addMessage(id + " closed.");
            } else {
                QueryLogic<?> logic = tuple.getFirst();
                try {
                    logic.close();
                } catch (Exception e) {
                    log.error("Exception occured while canceling query logic; may be innocuous if scanners were running.", e);
                }
                connectionFactory.returnConnection(tuple.getSecond());
                response.addMessage(id + " closed before create completed due to cancel.");
            }
            return response;
        } catch (DatawaveWebApplicationException e) {
            throw e;
        } catch (Exception e) {
            log.error("Error cancelling query: " + id, e);
            QueryException qe = new QueryException(DatawaveErrorCode.CANCELLATION_ERROR, e, MessageFormat.format("query_id: {0}", id));
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
    
    /**
     * List of current users queries.
     * 
     * @see datawave.webservice.query.runner.QueryExecutorBean#listUserQueries()
     *
     * @return datawave.webservice.result.QueryImplListResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 204 success but no results
     * @HTTP 500 internal server error
     */
    @GET
    @Path("/listAll")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @GZIP
    @Interceptors(ResponseInterceptor.class)
    @Override
    @Timed(name = "dw.query.listUserQueries", absolute = true)
    public QueryImplListResponse listUserQueries() {
        QueryImplListResponse response = new QueryImplListResponse();
        try {
            List<Query> userQueries = persister.findByUser();
            if (null == userQueries) {
                throw new NoResultsQueryException(DatawaveErrorCode.NO_QUERIES_FOUND);
            } else {
                response.setQuery(userQueries);
                return response;
            }
        } catch (DatawaveWebApplicationException e) {
            throw e;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.QUERY_LISTING_ERROR, e);
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
    
    /**
     * Lists query info for the given id.
     *
     * @param id
     *            - the id of the query to locate (@Required)
     * @see datawave.webservice.query.runner.QueryExecutorBean#get(String) get(String) for the @Required definition
     *
     * @return datawave.webservice.result.QueryImplListResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 404 queries not found using {@code id}
     * @HTTP 500 internal server error
     */
    @GET
    @Path("/{id}")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @GZIP
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @Timed(name = "dw.query.listQueryByID", absolute = true)
    public QueryImplListResponse get(@Required("id") @PathParam("id") String id) {
        QueryImplListResponse response = new QueryImplListResponse();
        List<Query> results = new ArrayList<>();
        try {
            RunningQuery query = getQueryById(id);
            if (null != query) {
                results.add(query.getSettings());
            }
            response.setQuery(results);
            return response;
        } catch (DatawaveWebApplicationException e) {
            throw e;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.QUERY_GET_ERROR, e, MessageFormat.format("queryID: {0}", id));
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
    
    /**
     * Lists queries for the current user with the given name.
     *
     * @param name
     *            the name of the query to locate (@Required)
     * @see datawave.webservice.query.runner.QueryExecutorBean#list(String) list(String) for the @Required definition
     *
     * @return datawave.webservice.result.QueryImplListResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 404 if the query with {@code name} does not belong to caller, you will never see it
     * @HTTP 404 queries not found using {@code name}
     * @HTTP 400 if {@code name} parameter is not included
     * @HTTP 500 internal server error
     */
    @GET
    @Path("/list")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @GZIP
    @Interceptors({RequiredInterceptor.class, ResponseInterceptor.class})
    @Override
    @Timed(name = "dw.query.listQueryByName", absolute = true)
    public QueryImplListResponse list(@Required("name") @QueryParam("name") String name) {
        QueryImplListResponse response = new QueryImplListResponse();
        List<Query> results = new ArrayList<>();
        try {
            List<RunningQuery> query = getQueryByName(name);
            if (null != query) {
                for (RunningQuery rq : query)
                    results.add(rq.getSettings());
            }
            response.setQuery(results);
            return response;
        } catch (DatawaveWebApplicationException e) {
            throw e;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.LIST_ERROR, e);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
    
    /**
     * remove (delete) the query
     * 
     * @param id
     *
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @RequestHeader query-session-id session id value used for load balancing purposes. query-session-id can be placed in the request in a Cookie header or as
     *                a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 404 queries not found using {@code id}
     * @HTTP 500 internal server error
     */
    @DELETE
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/{id}/remove")
    @GZIP
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    @Override
    @Timed(name = "dw.query.remove", absolute = true)
    public VoidResponse remove(@Required("id") @PathParam("id") String id) {
        VoidResponse response = new VoidResponse();
        try {
            boolean connectionRequestCanceled = accumuloConnectionRequestBean.cancelConnectionRequest(id);
            try {
                RunningQuery query = getQueryById(id);
                close(query);
                persister.remove(query.getSettings());
            } catch (NotFoundQueryException e) {
                // if connection request was canceled, then the call was successful even if a RunningQuery was not found
                if (!connectionRequestCanceled) {
                    throw e;
                }
            }
            response.addMessage(id + " removed.");
            return response;
        } catch (DatawaveWebApplicationException e) {
            throw e;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.QUERY_REMOVAL_ERROR, e, MessageFormat.format("query_id: {0}", id));
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
    
    /**
     * Duplicates a query and allows modification of optional properties
     *
     * @param id
     *            - the ID of the query to copy (required)
     * @param newQueryName
     *            - name of the new query (@Required)
     * @param newQueryLogicName
     *            - defaults to old logic, name of class that this query should be run with (optional)
     * @param newQuery
     *            - defaults to old query, string used in lookup (optional, auditing required if changed)
     * @param newColumnVisibility
     *            - defaults to old column visibility, for query AND justification (optional, auditing required if changed)
     * @param newBeginDate
     *            - defaults to old begin date, begin range for the query (optional, auditing required if changed)
     * @param newEndDate
     *            - defaults to old end date, end range for the query (optional, auditing required if changed)
     * @param newQueryAuthorizations
     *            - defaults to old authorizations, use in the query (optional, auditing required if changed)
     * @param newExpirationDate
     *            - defaults to old expiration, meaningless if transient (optional)
     * @param newPagesize
     *            - defaults to old pagesize, number of results to return on each call to next() (optional)
     * @param newPageTimeout
     *            - specify timeout (in minutes) for each call to next(), defaults to -1 indicating disabled (optional)
     * @param newPersistenceMode
     *            - defaults to PERSISTENT, indicates whether or not the query is persistent (optional)
     * @param newParameters
     *            - defaults to old, optional parameters to the query, a semi-colon separated list name=value pairs (optional, auditing required if changed)
     * @param trace
     *            - optional (defaults to {@code false}) indication of whether or not the query should be traced using the distributed tracing mechanism
     * @see datawave.webservice.query.runner.QueryExecutorBean#duplicateQuery(String, String, String, String, String, Date, Date, String, Date, Integer,
     *      Integer, QueryPersistence, String, boolean)
     *
     * @return {@code datawave.webservice.result.GenericResponse<String>}
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 400 if invalid params or missing queryName param
     * @HTTP 404 if query not found
     * @HTTP 500 internal server error
     */
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/{id}/duplicate")
    @GZIP
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    @Override
    @Timed(name = "dw.query.duplicateQuery", absolute = true)
    public GenericResponse<String> duplicateQuery(@PathParam("id") String id, @Required("queryName") @FormParam("queryName") String newQueryName,
                    @FormParam("logicName") String newQueryLogicName, @FormParam("query") String newQuery,
                    @FormParam("columnVisibility") String newColumnVisibility,
                    @FormParam("begin") @DateFormat(defaultTime = "000000", defaultMillisec = "000") Date newBeginDate, @FormParam("end") @DateFormat(
                                    defaultTime = "235959", defaultMillisec = "999") Date newEndDate, @FormParam("auths") String newQueryAuthorizations,
                    @FormParam("expiration") @DateFormat(defaultTime = "235959", defaultMillisec = "999") Date newExpirationDate,
                    @FormParam("pagesize") Integer newPagesize, @FormParam("pageTimeout") Integer newPageTimeout,
                    @FormParam("persistence") QueryPersistence newPersistenceMode, @FormParam("params") String newParameters,
                    @FormParam("trace") @DefaultValue("false") boolean trace) {
        
        GenericResponse<String> response = new GenericResponse<>();
        
        try {
            if (null == newQueryName || newQueryName.length() < 1) {
                throw new BadRequestQueryException(DatawaveErrorCode.QUERY_NAME_REQUIRED);
            }
            
            RunningQuery templateQuery = getQueryById(id);
            
            Query q = templateQuery.getSettings().duplicate(newQueryName);
            QueryPersistence persistence = QueryPersistence.PERSISTENT; // default value
            // TODO: figure out a way to set this to the same as the existing query
            if (null != newPersistenceMode) {
                persistence = newPersistenceMode;
            }
            
            // TODO: add validation for all these sets
            // maybe set variables instead of stuffing in query
            if (newQueryLogicName != null) {
                q.setQueryLogicName(queryLogicFactory.getQueryLogic(newQueryLogicName, ctx.getCallerPrincipal()).getLogicName());
            }
            if (newQuery != null) {
                q.setQuery(newQuery);
            }
            if (newBeginDate != null) {
                q.setBeginDate(newBeginDate);
            }
            if (newEndDate != null) {
                q.setEndDate(newEndDate);
            }
            if (newQueryAuthorizations != null) {
                q.setQueryAuthorizations(newQueryAuthorizations);
            }
            if (newExpirationDate != null) {
                q.setExpirationDate(newExpirationDate);
            }
            if (newPagesize != null) {
                q.setPagesize(newPagesize);
            }
            if (newPageTimeout != null) {
                q.setPageTimeout(newPageTimeout);
            }
            Set<Parameter> params = new HashSet<>();
            if (newParameters != null) {
                String[] param = newParameters.split(QueryImpl.PARAMETER_SEPARATOR);
                for (String yyy : param) {
                    String[] parts = yyy.split(QueryImpl.PARAMETER_NAME_VALUE_SEPARATOR);
                    if (parts.length == 2) {
                        params.add(new Parameter(parts[0], parts[1]));
                    }
                }
            }
            MultivaluedMap<String,String> newSettings = q.toMap();
            newSettings.putSingle(QueryParameters.QUERY_PERSISTENCE, persistence.name());
            return createQuery(q.getQueryLogicName(), newSettings);
        } catch (DatawaveWebApplicationException e) {
            throw e;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.QUERY_DUPLICATION_ERROR, e);
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            if (e.getClass() == IllegalArgumentException.class) {
                throw new BadRequestException(qe, response);
            }
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
        
    }
    
    /**
     * Updates a query object identified by the id using the updated parameters.
     *
     * @param id
     *            - the ID of the query to update (required)
     * @param queryLogicName
     *            - name of class that this query should be run with (optional)
     * @param query
     *            - query string (optional, auditing required if changed)
     * @param beginDate
     *            - begin date range for the query (optional, auditing required if changed)
     * @param endDate
     *            - end date range for the query (optional, auditing required if changed)
     * @param queryAuthorizations
     *            - authorizations for use in the query (optional, auditing required if changed)
     * @param expirationDate
     *            - meaningless if transient (optional)
     * @param pagesize
     *            - number of results to return on each call to next() (optional)
     * @param pageTimeout
     *            - specify timeout (in minutes) for each call to next(), defaults to -1 indicating disabled (optional)
     * @param persistenceMode
     *            - indicates whether or not the query is persistent (optional)
     * @param parameters
     *            - optional parameters to the query, a semi-colon separated list name=value pairs (optional, auditing required if changed)
     * @see datawave.webservice.query.runner.QueryExecutorBean#updateQuery(String, String, String, String, java.util.Date, java.util.Date, String,
     *      java.util.Date, Integer, Integer, datawave.webservice.query.QueryPersistence, String)
     *
     * @return {@code datawave.webservice.result.GenericResponse<String>}
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 400 if invalid param or no params
     * @HTTP 404 queries not found using {@code name}
     * @HTTP 500 internal server error
     */
    @PUT
    @POST
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @Path("/{id}/update")
    @GZIP
    @Interceptors(ResponseInterceptor.class)
    @Timed(name = "dw.query.updateQuery", absolute = true)
    public GenericResponse<String> updateQuery(@PathParam("id") String id, @FormParam("logicName") String queryLogicName, @FormParam("query") String query,
                    @FormParam("columnVisibility") String newColumnVisibility,
                    @FormParam("begin") @DateFormat(defaultTime = "000000", defaultMillisec = "000") Date beginDate, @FormParam("end") @DateFormat(
                                    defaultTime = "235959", defaultMillisec = "999") Date endDate, @FormParam("auths") String queryAuthorizations,
                    @FormParam("expiration") @DateFormat(defaultTime = "235959", defaultMillisec = "999") Date expirationDate,
                    @FormParam("pagesize") Integer pagesize, @FormParam("pageTimeout") Integer pageTimeout,
                    @FormParam("persistence") QueryPersistence persistenceMode, @FormParam("params") String parameters) {
        GenericResponse<String> response = new GenericResponse<>();
        try {
            RunningQuery rq = getQueryById(id);
            updateQuery(response, rq, queryLogicName, query, beginDate, endDate, queryAuthorizations, expirationDate, pagesize, pageTimeout, persistenceMode,
                            parameters);
            
            response.setResult(id);
            return response;
        } catch (DatawaveWebApplicationException e) {
            throw e;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.QUERY_UPDATE_ERROR, e, MessageFormat.format("query_id: {0}", id));
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            if (e.getClass() == IllegalArgumentException.class) {
                throw new BadRequestException(qe, response);
            }
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
    
    private void updateQuery(GenericResponse<String> response, RunningQuery runningQuery, String queryLogicName, String query, Date beginDate, Date endDate,
                    String queryAuthorizations, Date expirationDate, Integer pagesize, Integer pageTimeout, QueryPersistence persistenceMode, String parameters)
                    throws Exception {
        // Find out who/what called this method
        Principal p = ctx.getCallerPrincipal();
        String userid = p.getName();
        Collection<Collection<String>> cbAuths = new HashSet<>();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) p;
            userid = dp.getShortName();
            cbAuths.addAll(dp.getAuthorizations());
        }
        log.trace(userid + " has authorizations " + cbAuths.toString());
        
        boolean auditRequired = false;
        Query q = runningQuery.getSettings();
        
        // TODO: add validation for all these sets
        if (queryLogicName != null) {
            QueryLogic<?> logic = queryLogicFactory.getQueryLogic(queryLogicName, p);
            q.setQueryLogicName(logic.getLogicName());
        }
        if (query != null) {
            auditRequired = true;
            q.setQuery(query);
        }
        if (beginDate != null) {
            auditRequired = true;
            q.setBeginDate(beginDate);
        }
        if (endDate != null) {
            auditRequired = true;
            q.setEndDate(endDate);
        }
        if (queryAuthorizations != null) {
            auditRequired = true;
            q.setQueryAuthorizations(queryAuthorizations);
        }
        if (expirationDate != null) {
            q.setExpirationDate(expirationDate);
        }
        if (pagesize != null) {
            q.setPagesize(pagesize);
        }
        if (pageTimeout != null) {
            q.setPageTimeout(pageTimeout);
        }
        if (parameters != null) {
            Set<Parameter> params = new HashSet<>();
            String[] param = parameters.split(QueryImpl.PARAMETER_SEPARATOR);
            for (String yyy : param) {
                String[] parts = yyy.split(QueryImpl.PARAMETER_NAME_VALUE_SEPARATOR);
                if (parts.length == 2) {
                    params.add(new Parameter(parts[0], parts[1]));
                }
            }
            q.setParameters(params);
        }
        
        if (persistenceMode != null) {
            switch (persistenceMode) {
                case PERSISTENT:
                    if (q.getQueryName() == null)
                        throw new BadRequestQueryException(DatawaveErrorCode.QUERY_NAME_REQUIRED);
                    persister.update(q);
                    break;
                case TRANSIENT:
                    persister.remove(q);
                    break;
                default:
                    throw new BadRequestQueryException(DatawaveErrorCode.UNKNOWN_PERSISTENCE_MODE, MessageFormat.format("Mode = {0}", persistenceMode));
            }
        }
        
        // Put in the cache by id
        queryCache.put(q.getId().toString(), runningQuery);
        
        // Fire off an audit
        if (auditRequired) {
            Set<String> methodAuths = new HashSet<>(Arrays.asList(q.getQueryAuthorizations().split("\\s*,\\s*")));
            cbAuths.retainAll(methodAuths);
            AuditType auditType = runningQuery.getLogic().getAuditType(runningQuery.getSettings());
            if (!auditType.equals(AuditType.NONE)) {
                try {
                    auditParameters.clear();
                    auditParameters.validate(q.toMap());
                    auditor.audit(auditParameters);
                } catch (Exception e) {
                    QueryException qe = new QueryException(DatawaveErrorCode.QUERY_AUDITING_ERROR, e);
                    log.error(qe);
                    response.addException(qe.getBottomQueryException());
                }
            }
        }
    }
    
    /**
     * <strong>Administrator credentials required.</strong> Returns list of queries for some other user
     *
     * @return datawave.webservice.result.QueryImplListResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 204 no results for userid
     * @HTTP 401 if the user does not have Administrative credentials
     * @HTTP 500 internal server error
     */
    @GET
    @Path("/{userid}/listAll")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @GZIP
    @Interceptors(ResponseInterceptor.class)
    @RolesAllowed("Administrator")
    public QueryImplListResponse listQueriesForUser(@Required("userId") @PathParam("userid") String userId) {
        QueryImplListResponse response = new QueryImplListResponse();
        try {
            List<Query> userQueries = persister.findByUser(userId);
            if (null == userQueries) {
                throw new NoResultsException(null);
            } else {
                response.setQuery(userQueries);
                return response;
            }
        } catch (DatawaveWebApplicationException e) {
            throw e;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.QUERY_LISTING_ERROR, e, MessageFormat.format("UserId: {0}", userId));
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
    
    /**
     * <strong>Administrator credentials required.</strong> Purges the cache of query objects
     *
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 401 if the user does not have Administrative credentials
     * @HTTP 500 internal server error
     */
    @POST
    @Path("/purgeQueryCache")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @GZIP
    @Interceptors(ResponseInterceptor.class)
    @RolesAllowed("Administrator")
    public VoidResponse purgeQueryCache() {
        VoidResponse response = new VoidResponse();
        try {
            queryCache.clear();
            return response;
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.QUERY_CACHE_PURGE_ERROR, e);
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
    }
    
    /**
     * <strong>JBossAdministrator or Administrator credentials required.</strong> Enables tracing for all queries whose query string matches a regular
     * expression and/or are submitted by a named user. Note that at least one of {@code queryRegex} or {@code user} must be specified. If both are specified,
     * then queries submitted by {@code user} that match {@code queryRegex} are traced.
     * <p>
     * All traces are stored under the query UUID.
     *
     * @param queryRegex
     *            (optional) the query regular expression defining queries to trace
     * @param user
     *            (optional) the user name for which to trace queries
     * @return datawave.webservice.result.VoidResponse
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     *
     * @HTTP 200 success
     * @HTTP 400 if neither queryRegex nor user are specified
     * @HTTP 401 if the user does not have Administrative credentials
     */
    @GET
    @Path("/enableTracing")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @Override
    public VoidResponse enableTracing(@QueryParam("queryRegex") String queryRegex, @QueryParam("user") String user) {
        VoidResponse response = new VoidResponse();
        if (queryRegex == null && user == null) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.QUERY_REGEX_OR_USER_REQUIRED);
            response.addException(qe);
            throw new BadRequestException(qe, response);
        } else {
            PatternWrapper p = PatternWrapper.wrap(queryRegex);
            if (!traceInfos.containsEntry(user, p))
                traceInfos.put(user, p);
            // Put updated map back in the cache
            queryTraceCache.put("traceInfos", traceInfos);
            return response;
        }
    }
    
    /**
     * <strong>JBossAdministrator or Administrator credentials required.</strong> Disables tracing that was previously enabled using the
     * {@link #enableTracing(String, String)} method.
     *
     * @param queryRegex
     *            (optional) the query regular expression defining queries to disable tracing
     * @param user
     *            (optional) the user name for which to disable query tracing
     * @return datawave.webservice.result.VoidResponse
     *
     * @HTTP 200 success
     * @HTTP 400 if neither queryRegex nor user are specified
     * @HTTP 401 if the user does not have Administrative credentials
     */
    @GET
    @Path("/disableTracing")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @Override
    public VoidResponse disableTracing(@QueryParam("queryRegex") String queryRegex, @QueryParam("user") String user) {
        VoidResponse response = new VoidResponse();
        if (queryRegex == null && user == null) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.QUERY_REGEX_OR_USER_REQUIRED);
            response.addException(qe);
            throw new BadRequestException(qe, response);
        } else if (queryRegex == null) {
            traceInfos.removeAll(user);
            response.addMessage("All query tracing for " + user + " is disabled.  Per-query tracing is still possible.");
        } else {
            traceInfos.remove(user, PatternWrapper.wrap(queryRegex));
            response.addMessage("Queries for user " + user + " matching " + queryRegex + " have been disabled. Per-query tracing is still possible.");
        }
        
        // Put updated map back in the cache
        queryTraceCache.put("traceInfos", traceInfos);
        return response;
    }
    
    /**
     * <strong>JBossAdministrator or Administrator credentials required.</strong> Disables all tracing that was enabled using the
     * {@link #enableTracing(String, String)} method. Note that this does not prevent individual queries that are created with the trace parameter specified.
     *
     * @return datawave.webservice.result.VoidResponse
     *
     * @HTTP 200 success
     * @HTTP 401 if the user does not have Administrative credentials
     */
    @GET
    @Path("/disableAllTracing")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @Override
    public VoidResponse disableAllTracing() {
        VoidResponse response = new VoidResponse();
        traceInfos.clear();
        // Put updated map back in the cache
        queryTraceCache.put("traceInfos", traceInfos);
        response.addMessage("All user/regex traces cleared. Per-query tracing is still possible.");
        return response;
    }
    
    private boolean shouldTraceQuery(String queryString, String user, boolean traceRequested) {
        boolean shouldTrace = traceRequested;
        if (!shouldTrace) {
            // Check user-specific regexes (null regex means trace all queries for a user)
            for (PatternWrapper regex : traceInfos.get(user)) {
                if (regex == null || regex.matches(queryString)) {
                    shouldTrace = true;
                    break;
                }
            }
        }
        if (!shouldTrace) {
            // Check user-agnostic regexes (regex can't be null)
            for (PatternWrapper regex : traceInfos.get(null)) {
                if (regex.matches(queryString)) {
                    shouldTrace = true;
                    break;
                }
            }
        }
        return shouldTrace;
    }
    
    protected QueryMetricsBean getMetrics() {
        return metrics;
    }
    
    protected QueryLogicFactory getQueryFactory() {
        return queryLogicFactory;
    }
    
    protected Persister getPersister() {
        return persister;
    }
    
    protected QueryCache getQueryCache() {
        return queryCache;
    }
    
    private void addQueryToTrackingMap(Map<String,String> trackingMap, Query q) {
        
        if (trackingMap == null || q == null) {
            return;
        }
        
        if (q.getOwner() != null) {
            trackingMap.put("query.user", q.getOwner());
        }
        if (q.getId() != null) {
            trackingMap.put("query.id", q.getId().toString());
        }
        if (q.getId() != null) {
            trackingMap.put("query.query", q.getQuery());
        }
    }
    
    /**
     * @param logicName
     * @param queryParameters
     *
     * @return {@code datawave.webservice.result.GenericResponse<String>}
     * @RequestHeader X-ProxiedEntitiesChain use when proxying request for user, by specifying a chain of DNs of the identities to proxy
     * @RequestHeader X-ProxiedIssuersChain required when using X-ProxiedEntitiesChain, specify one issuer DN per subject DN listed in X-ProxiedEntitiesChain
     * @ResponseHeader query-session-id this header and value will be in the Set-Cookie header, subsequent calls for this session will need to supply the
     *                 query-session-id header in the request in a Cookie header or as a query parameter
     * @ResponseHeader X-OperationTimeInMS time spent on the server performing the operation, does not account for network or result serialization
     * @ResponseHeader X-Partial-Results true if the page contains less than the requested number of results
     *
     * @HTTP 200 success
     * @HTTP 204 success and no results
     * @HTTP 400 invalid or missing parameter
     * @HTTP 500 internal server error
     */
    @POST
    @Produces("*/*")
    @Path("/{logicName}/execute")
    @GZIP
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    @TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
    @Override
    @Timed(name = "dw.query.executeQuery", absolute = true)
    public StreamingOutput execute(@PathParam("logicName") String logicName, MultivaluedMap<String,String> queryParameters, @Context HttpHeaders httpHeaders) {
        
        /**
         * This method captures the metrics on the query instead of doing it in the QueryMetricsEnrichmentInterceptor. The ExecuteStreamingOutputResponse class
         * is returned from this method and executed in the JAX-RS layer. It updates the metrics which are then updated on each call to the _next method.
         */
        Collection<String> proxyServers = null;
        Principal p = ctx.getCallerPrincipal();
        DatawavePrincipal dp;
        if (p instanceof DatawavePrincipal) {
            dp = (DatawavePrincipal) p;
            proxyServers = dp.getProxyServers();
        }
        
        final MediaType PB_MEDIA_TYPE = new MediaType("application", "x-protobuf");
        final MediaType YAML_MEDIA_TYPE = new MediaType("application", "x-yaml");
        final VoidResponse response = new VoidResponse();
        
        // HttpHeaders.getAcceptableMediaTypes returns a priority sorted list of acceptable response types.
        // Find the first one in the list that we support.
        MediaType responseType = null;
        for (MediaType type : httpHeaders.getAcceptableMediaTypes()) {
            if (type.equals(MediaType.APPLICATION_XML_TYPE) || type.equals(MediaType.APPLICATION_JSON_TYPE) || type.equals(PB_MEDIA_TYPE)
                            || type.equals(YAML_MEDIA_TYPE)) {
                responseType = type;
                break;
            }
        }
        if (null == responseType) {
            QueryException qe = new QueryException(DatawaveErrorCode.UNSUPPORTED_MEDIA_TYPE);
            response.addException(qe);
            throw new DatawaveWebApplicationException(qe, response);
        }
        
        // reference query necessary to avoid NPEs in getting the Transformer and BaseResponse
        Query q = new QueryImpl();
        Date now = new Date();
        q.setBeginDate(now);
        q.setEndDate(now);
        q.setExpirationDate(now);
        q.setQuery("test");
        q.setQueryAuthorizations("ALL");
        ResultsPage emptyList = new ResultsPage();
        
        // Find the response class
        Class<?> responseClass;
        try {
            QueryLogic<?> l = queryLogicFactory.getQueryLogic(logicName, p);
            QueryLogicTransformer t = l.getTransformer(q);
            BaseResponse refResponse = t.createResponse(emptyList);
            responseClass = refResponse.getClass();
        } catch (Exception e) {
            QueryException qe = new QueryException(DatawaveErrorCode.QUERY_TRANSFORM_ERROR, e);
            log.error(qe);
            response.addException(qe.getBottomQueryException());
            int statusCode = qe.getBottomQueryException().getStatusCode();
            throw new DatawaveWebApplicationException(qe, response, statusCode);
        }
        
        SerializationType s;
        if (responseType.equals(MediaType.APPLICATION_XML_TYPE)) {
            s = SerializationType.XML;
        } else if (responseType.equals(MediaType.APPLICATION_JSON_TYPE)) {
            s = SerializationType.JSON;
        } else if (responseType.equals(PB_MEDIA_TYPE)) {
            if (!(Message.class.isAssignableFrom(responseClass))) {
                QueryException qe = new QueryException(DatawaveErrorCode.BAD_RESPONSE_CLASS, MessageFormat.format("Response  class: {0}", responseClass));
                response.addException(qe);
                throw new DatawaveWebApplicationException(qe, response);
            }
            s = SerializationType.PB;
        } else if (responseType.equals(YAML_MEDIA_TYPE)) {
            if (!(Message.class.isAssignableFrom(responseClass))) {
                QueryException qe = new QueryException(DatawaveErrorCode.BAD_RESPONSE_CLASS, MessageFormat.format("Response  class: {0}", responseClass));
                response.addException(qe);
                throw new DatawaveWebApplicationException(qe, response);
            }
            s = SerializationType.YAML;
        } else {
            QueryException qe = new QueryException(DatawaveErrorCode.INVALID_FORMAT, MessageFormat.format("format: {0}", responseType.toString()));
            response.addException(qe);
            throw new DatawaveWebApplicationException(qe, response);
        }
        
        long start = System.nanoTime();
        GenericResponse<String> createResponse = null;
        try {
            createResponse = this.createQuery(logicName, queryParameters, httpHeaders);
        } catch (DatawaveWebApplicationException ex) {
            if (ex.getCause() instanceof QueryException) {
                QueryException queryException = (QueryException) ex.getCause();
                return new ErrorResponse(queryException, s);
            }
        }
        long createCallTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        final String queryId = createResponse.getResult();
        
        // We created the query and put into cache, get the RunningQuery object
        final RunningQuery rq = queryCache.get(queryId);
        rq.getMetric().setCreateCallTime(createCallTime);
        
        final Collection<String> proxies = proxyServers;
        final SerializationType serializationType = s;
        final Class<?> queryResponseClass = responseClass;
        
        ExecuteStreamingOutputResponse streamingResponse = new ExecuteStreamingOutputResponse(queryId, queryResponseClass, response, rq, serializationType,
                        proxies);
        return streamingResponse;
    }
    
    /**
     * Asynchronous version of {@link #execute(String, MultivaluedMap, HttpHeaders)}
     * 
     * @see #execute(String, MultivaluedMap, HttpHeaders)
     */
    @POST
    @Produces("*/*")
    @Path("/{logicName}/async/execute")
    @GZIP
    @Interceptors({ResponseInterceptor.class, RequiredInterceptor.class})
    @TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
    @Asynchronous
    @Timed(name = "dw.query.executeQueryAsync", absolute = true)
    public void executeAsync(@PathParam("logicName") String logicName, MultivaluedMap<String,String> queryParameters, @Context HttpHeaders httpHeaders,
                    @Suspended AsyncResponse asyncResponse) {
        try {
            StreamingOutput output = execute(logicName, queryParameters, httpHeaders);
            asyncResponse.resume(output);
        } catch (Throwable t) {
            asyncResponse.resume(t);
        }
    }
    
    @Asynchronous
    public Future<?> executeAsync(String logicName, MultivaluedMap<String,String> queryParameters, Long startTime, Long loginTime,
                    AsyncQueryStatusObserver observer) {
        Collection<String> proxyServers = null;
        Principal p = ctx.getCallerPrincipal();
        DatawavePrincipal dp;
        if (p instanceof DatawavePrincipal) {
            dp = (DatawavePrincipal) p;
            proxyServers = dp.getProxyServers();
        }
        
        long start = (startTime != null) ? startTime : System.nanoTime();
        GenericResponse<String> createResponse;
        try {
            createResponse = createQuery(logicName, queryParameters);
            observer.queryCreated(createResponse);
        } catch (DatawaveWebApplicationException e) {
            observer.queryCreateException(new QueryException(e));
            return new AsyncResult<>(e);
        }
        
        if (sessionContext.wasCancelCalled())
            return new AsyncResult<>("cancelled");
        
        long createCallTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        final String queryId = createResponse.getResult();
        
        // We created the query and put into cache, get the RunningQuery object
        // and update the query metric for call time.
        final RunningQuery rq = queryCache.get(queryId);
        try {
            rq.getMetric().setCreateCallTime(createCallTime);
            if (loginTime != null) {
                rq.getMetric().setLoginTime(loginTime);
            }
            try {
                metrics.updateMetric(rq.getMetric());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            
            boolean done = false;
            Span span = null;
            List<PageMetric> pageMetrics = rq.getMetric().getPageTimes();
            
            // Loop over each page of query results, and notify the observer about each page.
            // If we get any exception, then break out of the loop and notify the observer about the problem.
            do {
                long callStart = System.nanoTime();
                rq.setActiveCall(true);
                try {
                    BaseQueryResponse page = _next(rq, queryId, proxyServers, span);
                    long serializationStart = System.nanoTime();
                    observer.queryResultsAvailable(page);
                    long serializationTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - serializationStart);
                    
                    if (rq.getLogic().getCollectQueryMetrics()) {
                        PageMetric pm = pageMetrics.get(pageMetrics.size() - 1);
                        pm.setSerializationTime(serializationTime);
                        long pageCallTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - callStart);
                        pm.setCallTime(pageCallTime);
                    }
                } catch (Exception e) {
                    if (e instanceof NoResultsException || e.getCause() instanceof NoResultsException) {
                        // No more results, break out of loop
                        done = true;
                    } else if (sessionContext.wasCancelCalled() && e instanceof CancellationException) {
                        // We were cancelled by the originating user, so just break out of the loop.
                        // If we were cancelled due to an admin cancel, we'll report the exception to the user.
                        done = true;
                    } else {
                        // We had a real problem. Update the query metric with the error and then notify the observer.
                        if (rq.getLogic().getCollectQueryMetrics()) {
                            rq.getMetric().setError(e);
                        }
                        QueryException qe = (e instanceof QueryException) ? (QueryException) e : new QueryException(e);
                        observer.queryException(qe);
                        done = true;
                        break;
                    }
                } finally {
                    rq.setActiveCall(false);
                    // Update the query metrics for the completion of this page (either successfully or due to error)
                    if (rq.getLogic().getCollectQueryMetrics()) {
                        try {
                            metrics.updateMetric(rq.getMetric());
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }
            } while (!done && !sessionContext.wasCancelCalled());
        } finally {
            // Close the query now that we're done with it.
            try {
                close(rq);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        
        observer.queryFinished(queryId);
        
        return new AsyncResult<>(queryId);
    }
    
    private enum SerializationType {
        JSON, XML, PB, YAML;
    }
    
    public class ExecuteStreamingOutputResponse implements StreamingOutput {
        private String queryId = null;
        private Class<?> queryResponseClass = null;
        private VoidResponse errorResponse = null;
        private RunningQuery rq = null;
        private SerializationType serializationType = SerializationType.XML;
        private Collection<String> proxies = null;
        
        public ExecuteStreamingOutputResponse(String queryId, Class<?> queryResponseClass, VoidResponse errorResponse, RunningQuery rq,
                        SerializationType serializationType, Collection<String> proxies) {
            super();
            this.queryId = queryId;
            this.queryResponseClass = queryResponseClass;
            this.errorResponse = errorResponse;
            this.rq = rq;
            this.serializationType = serializationType;
            this.proxies = proxies;
        }
        
        public String getQueryId() {
            return queryId;
        }
        
        @Override
        public void write(OutputStream out) throws IOException, WebApplicationException {
            
            try {
                LinkedBuffer buffer = LinkedBuffer.allocate(4096);
                Marshaller xmlSerializer;
                try {
                    JAXBContext jaxbContext = JAXBContext.newInstance(queryResponseClass);
                    xmlSerializer = jaxbContext.createMarshaller();
                } catch (JAXBException e1) {
                    QueryException qe = new QueryException(DatawaveErrorCode.JAXB_CONTEXT_ERROR, e1, MessageFormat.format("class: {0}", queryResponseClass));
                    log.error(qe);
                    errorResponse.addException(qe.getBottomQueryException());
                    throw new DatawaveWebApplicationException(qe, errorResponse);
                }
                ObjectMapper jsonSerializer = new ObjectMapper();
                jsonSerializer.enable(MapperFeature.USE_WRAPPER_NAME_AS_PROPERTY_NAME);
                jsonSerializer.setAnnotationIntrospector(AnnotationIntrospector.pair(new JacksonAnnotationIntrospector(), new JaxbAnnotationIntrospector(
                                jsonSerializer.getTypeFactory())));
                // Don't close the output stream
                jsonSerializer.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
                JsonGenerator jsonGenerator = jsonSerializer.getFactory().createGenerator(out, JsonEncoding.UTF8);
                jsonGenerator.enable(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM);
                
                boolean sentResults = false;
                boolean done = false;
                Span span = null;
                List<PageMetric> pageMetrics = rq.getMetric().getPageTimes();
                
                do {
                    try {
                        long callStart = System.nanoTime();
                        BaseQueryResponse page = _next(rq, queryId, proxies, span);
                        PageMetric pm = pageMetrics.get(pageMetrics.size() - 1);
                        
                        // Wrap the output stream so that we can get a byte count
                        CountingOutputStream countingStream = new CountingOutputStream(out);
                        
                        long serializationStart = System.nanoTime();
                        switch (serializationType) {
                            case XML:
                                xmlSerializer.marshal(page, countingStream);
                                break;
                            case JSON:
                                // First page!
                                if (!sentResults) {
                                    jsonGenerator.writeStartObject();
                                    jsonGenerator.writeArrayFieldStart("Pages");
                                    jsonGenerator.flush();
                                } else {
                                    // Delimiter for subsequent pages...
                                    countingStream.write(',');
                                }
                                jsonSerializer.writeValue(countingStream, page);
                                break;
                            case PB:
                                @SuppressWarnings("unchecked")
                                Message<Object> pb = (Message<Object>) page;
                                Schema<Object> pbSchema = pb.cachedSchema();
                                ProtobufIOUtil.writeTo(countingStream, page, pbSchema, buffer);
                                buffer.clear();
                                break;
                            case YAML:
                                @SuppressWarnings("unchecked")
                                Message<Object> yaml = (Message<Object>) page;
                                Schema<Object> yamlSchema = yaml.cachedSchema();
                                YamlIOUtil.writeTo(countingStream, page, yamlSchema, buffer);
                                buffer.clear();
                                break;
                        }
                        countingStream.flush();
                        long serializationTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - serializationStart);
                        pm.setSerializationTime(serializationTime);
                        long pageCallTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - callStart);
                        pm.setCallTime(pageCallTime);
                        pm.setBytesWritten(countingStream.getCount());
                        sentResults = true;
                    } catch (Exception e) {
                        if (e instanceof NoResultsException || e.getCause() instanceof NoResultsException) {
                            // No more results, break out of loop
                            done = true;
                            break; // probably redundant
                        } else {
                            throw e;
                        }
                    }
                } while (!done);
                
                if (!sentResults)
                    throw new NoResultsQueryException(DatawaveErrorCode.RESULTS_NOT_SENT);
                else if (serializationType == SerializationType.JSON) {
                    jsonGenerator.writeEndArray();
                    jsonGenerator.writeEndObject();
                    jsonGenerator.flush();
                }
            } catch (DatawaveWebApplicationException e) {
                throw e;
            } catch (Exception e) {
                log.error("ExecuteStreamingOutputResponse write Failed", e);
                QueryException qe = new QueryException(DatawaveErrorCode.QUERY_NEXT_ERROR, e, MessageFormat.format("query_id: {0}", rq.getSettings().getId()));
                log.error(qe);
                errorResponse.addException(qe.getBottomQueryException());
                int statusCode = qe.getBottomQueryException().getStatusCode();
                throw new DatawaveWebApplicationException(qe, errorResponse, statusCode);
            } finally {
                try {
                    close(rq);
                } catch (Exception e) {
                    log.error("Error returning connection on failed create", e);
                    QueryException qe = new QueryException(DatawaveErrorCode.CONNECTION_RETURN_ERROR, e);
                    log.error(qe);
                    errorResponse.addException(qe.getBottomQueryException());
                }
            }
        }
        
    }
    
    public class ErrorResponse implements StreamingOutput {
        private GenericResponse<String> errorResponse = new GenericResponse<String>();
        private SerializationType serializationType = SerializationType.XML;
        
        public ErrorResponse(QueryException queryException, SerializationType serializationType) {
            this.serializationType = serializationType;
            this.errorResponse.addException(queryException);
        }
        
        @Override
        public void write(OutputStream out) throws IOException, WebApplicationException {
            
            try {
                LinkedBuffer buffer = LinkedBuffer.allocate(4096);
                Marshaller xmlSerializer;
                try {
                    JAXBContext jaxbContext = JAXBContext.newInstance(errorResponse.getClass());
                    xmlSerializer = jaxbContext.createMarshaller();
                } catch (JAXBException e1) {
                    QueryException qe = new QueryException(DatawaveErrorCode.JAXB_CONTEXT_ERROR, e1, MessageFormat.format("class: {0}",
                                    errorResponse.getClass()));
                    log.error(qe);
                    errorResponse.addException(qe.getBottomQueryException());
                    throw new DatawaveWebApplicationException(qe, errorResponse);
                }
                ObjectMapper jsonSerializer = new ObjectMapper();
                jsonSerializer.enable(MapperFeature.USE_WRAPPER_NAME_AS_PROPERTY_NAME);
                jsonSerializer.setAnnotationIntrospector(AnnotationIntrospector.pair(new JacksonAnnotationIntrospector(), new JaxbAnnotationIntrospector(
                                jsonSerializer.getTypeFactory())));
                // Don't close the output stream
                jsonSerializer.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
                JsonGenerator jsonGenerator = jsonSerializer.getFactory().createGenerator(out, JsonEncoding.UTF8);
                jsonGenerator.enable(JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM);
                
                boolean sentResults = false;
                
                try {
                    BaseResponse page = this.errorResponse;
                    
                    // Wrap the output stream so that we can get a byte count
                    CountingOutputStream countingStream = new CountingOutputStream(out);
                    
                    switch (serializationType) {
                        case XML:
                            xmlSerializer.marshal(page, countingStream);
                            break;
                        case JSON:
                            jsonGenerator.writeStartObject();
                            jsonGenerator.flush();
                            jsonSerializer.writeValue(countingStream, page);
                            break;
                        case PB:
                            @SuppressWarnings("unchecked")
                            Message<Object> pb = (Message<Object>) page;
                            Schema<Object> pbSchema = pb.cachedSchema();
                            ProtobufIOUtil.writeTo(countingStream, page, pbSchema, buffer);
                            buffer.clear();
                            break;
                        case YAML:
                            @SuppressWarnings("unchecked")
                            Message<Object> yaml = (Message<Object>) page;
                            Schema<Object> yamlSchema = yaml.cachedSchema();
                            YamlIOUtil.writeTo(countingStream, page, yamlSchema, buffer);
                            buffer.clear();
                            break;
                    }
                    countingStream.flush();
                    sentResults = true;
                } catch (Exception e) {
                    if (e instanceof NoResultsException == false && e.getCause() instanceof NoResultsException == false) {
                        throw e;
                    }
                }
                
                if (!sentResults)
                    throw new NoResultsQueryException(DatawaveErrorCode.RESULTS_NOT_SENT);
                else if (serializationType == SerializationType.JSON) {
                    jsonGenerator.writeEndArray();
                    jsonGenerator.writeEndObject();
                    jsonGenerator.flush();
                }
            } catch (DatawaveWebApplicationException e) {
                throw e;
            } catch (Exception e) {
                log.error("ErrorResponse write Failed", e);
                QueryException qe = new QueryException(DatawaveErrorCode.QUERY_NEXT_ERROR, e, "foo");
                log.error(qe);
                errorResponse.addException(qe.getBottomQueryException());
                int statusCode = qe.getBottomQueryException().getStatusCode();
                throw new DatawaveWebApplicationException(qe, errorResponse, statusCode);
            }
        }
        
    }
    
    private void testForUncaughtException(Query settings, ResultsPage resultList) throws QueryException {
        QueryUncaughtExceptionHandler handler = settings.getUncaughtExceptionHandler();
        if (handler != null) {
            if (handler.getThrowable() != null) {
                if (resultList.getResults() != null && resultList.getResults().size() > 0) {
                    log.warn("Exception with Partial Results: resultList.getResults().size() is " + resultList.getResults().size()
                                    + ", and there was an UncaughtException:" + handler.getThrowable() + " in thread " + handler.getThread());
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Throwing:" + handler.getThrowable() + " for query with no results");
                    }
                }
                if (handler.getThrowable() instanceof QueryException) {
                    throw ((QueryException) handler.getThrowable());
                }
                throw new QueryException(handler.getThrowable());
            }
        }
    }
}
