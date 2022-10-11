package datawave.webservice.query.runner;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.microservice.querymetric.QueryMetric;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.query.data.UUIDType;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.webservice.common.audit.AuditBean;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import datawave.webservice.common.audit.PrivateAuditConstants;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import datawave.webservice.common.exception.BadRequestException;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.common.exception.NoResultsException;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.QueryParameters;
import datawave.webservice.query.QueryParametersImpl;
import datawave.webservice.query.QueryPersistence;
import datawave.webservice.query.cache.ClosedQueryCache;
import datawave.webservice.query.cache.CreatedQueryLogicCacheBean;
import datawave.webservice.query.cache.QueryCache;
import datawave.webservice.query.cache.QueryExpirationConfiguration;
import datawave.webservice.query.cache.QueryTraceCache;
import datawave.webservice.query.cache.QueryTraceCache.PatternWrapper;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.configuration.LookupUUIDConfiguration;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NoResultsQueryException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.factory.Persister;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.DatawaveRoleManager;
import datawave.webservice.query.logic.EasyRoleManager;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.logic.QueryLogicFactory;
import datawave.webservice.query.logic.QueryLogicFactoryImpl;
import datawave.webservice.query.logic.QueryLogicTransformer;
import datawave.webservice.query.logic.RoleManager;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.util.GetUUIDCriteria;
import datawave.webservice.query.util.LookupUUIDUtil;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.QueryImplListResponse;
import datawave.webservice.result.QueryLogicResponse;
import datawave.webservice.result.VoidResponse;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import javax.ejb.EJBContext;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.Status;
import javax.transaction.UserTransaction;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ExtendedQueryExecutorBeanTest {
    private static final Throwable ILLEGAL_STATE_EXCEPTION = new IllegalStateException("INTENTIONALLY THROWN TEST EXCEPTION");
    @Mock
    AuditBean auditor;
    
    @Mock
    BaseQueryResponse baseResponse;
    
    @Mock
    QueryCache cache;
    
    @Mock
    ClosedQueryCache closedCache;
    
    @Mock
    AccumuloConnectionFactory connectionFactory;
    
    @Mock
    Connector connector;
    
    @Mock
    EJBContext context;
    
    @Mock
    GenericQueryConfiguration genericConfiguration;
    
    @Mock
    LookupUUIDConfiguration lookupUUIDConfiguration;
    
    @Mock
    LookupUUIDUtil lookupUUIDUtil;
    
    @Mock
    QueryMetricsBean metrics;
    
    @Mock
    Persister persister;
    
    @Mock
    DatawavePrincipal principal;
    
    @Mock
    DatawaveUser dwUser;
    
    @Mock
    CreatedQueryLogicCacheBean qlCache;
    
    @Mock
    Query query;
    
    @Mock
    QueryUncaughtExceptionHandler exceptionHandler;
    
    @Mock
    QueryLogicFactoryImpl queryLogicFactory;
    
    @Mock
    QueryLogic<?> queryLogic1;
    
    @Mock
    QuerySyntaxParserQueryLogic<?> queryLogic2;
    
    @Mock
    QueryMetric queryMetric;
    
    @Mock
    ResultsPage resultsPage;
    
    @Mock
    RunningQuery runningQuery;
    
    @Mock
    Span span;
    
    @Mock
    QueryTraceCache traceCache;
    
    @Mock
    TInfo traceInfo;
    
    @Mock
    Multimap<String,PatternWrapper> traceInfos;
    
    @Mock
    Tracer tracer;
    
    @Mock
    QueryLogicTransformer transformer;
    
    @Mock
    TransformIterator transformIterator;
    
    @Mock
    UserTransaction transaction;
    
    @Mock
    Pair<QueryLogic<?>,Connector> tuple;
    
    @Mock
    HttpHeaders httpHeaders;
    
    @Mock
    ResponseObjectFactory responseObjectFactory;
    
    @Mock
    AccumuloConnectionRequestBean connectionRequestBean;
    
    @Mock
    UriInfo uriInfo;
    
    QueryExpirationConfiguration queryExpirationConf;
    
    @BeforeEach
    public void setupBefore() throws Exception {
        queryExpirationConf = new QueryExpirationConfiguration();
        queryExpirationConf.setPageSizeShortCircuitCheckTime(45);
        queryExpirationConf.setPageShortCircuitTimeout(58);
        queryExpirationConf.setCallTime(60);
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testAdminCancel_HappyPath() throws Exception {
        // Set local test input
        UUID queryId = UUID.randomUUID();
        // Set expectations of the create logic
        when(this.connectionRequestBean.adminCancelConnectionRequest(queryId.toString())).thenReturn(false);
        when(this.qlCache.poll(queryId.toString())).thenReturn(this.tuple);
        when(this.tuple.getFirst()).thenReturn((QueryLogic) this.queryLogic1);
        this.queryLogic1.close();
        when(this.tuple.getSecond()).thenReturn(this.connector);
        this.connectionFactory.returnConnection(this.connector);
        
        // Run the test
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        VoidResponse result1 = subject.adminCancel(queryId.toString());
        
        // Verify results
        assertNotNull(result1, "Expected a non-null response");
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testAdminCancel_NullTupleReturnedAndQueryExceptionThrown() throws Exception {
        // Set local test input
        UUID queryId = UUID.randomUUID();
        
        // Set expectations of the create logic
        when(this.connectionRequestBean.adminCancelConnectionRequest(queryId.toString())).thenReturn(false);
        when(this.qlCache.poll(queryId.toString())).thenReturn(null);
        when(this.cache.get(queryId.toString())).thenReturn(null);
        when(this.persister.adminFindById(queryId.toString())).thenReturn(Arrays.asList(this.query, this.query));
        
        // Run the test
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        
        assertThrows(DatawaveWebApplicationException.class, () -> subject.adminCancel(queryId.toString()));
        
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testAdminCancel_RunningQueryFoundInCache() throws Exception {
        // Set local test input
        UUID queryId = UUID.randomUUID();
        
        // Set expectations of the create logic
        try (MockedStatic<Trace> traceMock = Mockito.mockStatic(Trace.class)) {
            traceMock.when(() -> Trace.trace(this.traceInfo, "query:close")).thenReturn(this.span);
            
            when(this.connectionRequestBean.adminCancelConnectionRequest(queryId.toString())).thenReturn(false);
            when(this.qlCache.poll(queryId.toString())).thenReturn(null);
            when(this.cache.get(queryId.toString())).thenReturn(this.runningQuery);
            when(this.runningQuery.getSettings()).thenReturn(this.query);
            this.runningQuery.cancel();
            this.runningQuery.closeConnection(this.connectionFactory);
            when(this.query.getId()).thenReturn(queryId);
            cache.remove(queryId.toString());
            when(this.runningQuery.getTraceInfo()).thenReturn(this.traceInfo);
            this.span.data(eq("closedAt"), isA(String.class));
            this.span.stop();
            
            // Run the test
            QueryExecutorBean subject = new QueryExecutorBean();
            ReflectionTestUtils.setField(subject, "ctx", context);
            ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
            ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
            ReflectionTestUtils.setField(subject, "qlCache", qlCache);
            ReflectionTestUtils.setField(subject, "queryCache", cache);
            ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
            ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
            ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
            ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
            
            VoidResponse result1 = subject.adminCancel(queryId.toString());
            
            // Verify results
            assertNotNull(result1, "Expected a non-null response");
        }
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testAdminCancel_LookupAccumuloQuery() throws Exception {
        // Set local test input
        UUID queryId = UUID.randomUUID();
        List<String> dnList = Collections.singletonList("qwe");
        
        // Set expectations of the create logic
        when(this.connectionRequestBean.adminCancelConnectionRequest(queryId.toString())).thenReturn(false);
        when(this.qlCache.poll(queryId.toString())).thenReturn(null);
        when(this.cache.get(queryId.toString())).thenReturn(null);
        when(this.persister.adminFindById(queryId.toString())).thenReturn(Lists.newArrayList(query));
        when(this.query.getQueryAuthorizations()).thenReturn("AUTH_1,AUTH_2");
        when(this.query.getQueryLogicName()).thenReturn("ql1");
        when(this.query.getOwner()).thenReturn("qwe");
        when(this.query.getUserDN()).thenReturn("qwe");
        when(this.query.getDnList()).thenReturn(dnList);
        when(this.query.getId()).thenReturn(queryId);
        when(this.query.getQuery()).thenReturn("qwe");
        when(this.query.getBeginDate()).thenReturn(null);
        when(this.query.getEndDate()).thenReturn(null);
        when(this.query.getColumnVisibility()).thenReturn(null);
        when(this.query.getQueryName()).thenReturn(null);
        when(this.query.getParameters()).thenReturn((Set) Collections.emptySet());
        when(context.getCallerPrincipal()).thenReturn(principal);
        when(this.queryLogicFactory.getQueryLogic("ql1", principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getConnectionPriority()).thenReturn(Priority.NORMAL);
        when(this.queryLogic1.getCollectQueryMetrics()).thenReturn(false);
        when(this.queryLogic1.isLongRunningQuery()).thenReturn(false);
        when(this.queryLogic1.getResultLimit(dnList)).thenReturn(-1L);
        when(this.queryLogic1.getMaxResults()).thenReturn(-1L);
        cache.put(eq(queryId.toString()), isA(RunningQuery.class));
        cache.remove(queryId.toString());
        this.queryLogic1.close();
        
        // Run the test
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        
        VoidResponse result1 = subject.adminCancel(queryId.toString());
        
        // Verify results
        assertNotNull(result1, "Expected a non-null response");
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testAdminClose_NonNullTupleReturned() throws Exception {
        // Set local test input
        UUID queryId = UUID.randomUUID();
        
        // Set expectations
        when(this.connectionRequestBean.adminCancelConnectionRequest(queryId.toString())).thenReturn(false);
        when(this.qlCache.poll(queryId.toString())).thenReturn(this.tuple);
        when(this.tuple.getFirst()).thenReturn((QueryLogic) this.queryLogic1);
        this.queryLogic1.close();
        when(this.tuple.getSecond()).thenReturn(this.connector);
        this.connectionFactory.returnConnection(this.connector);
        
        // Run the test
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        VoidResponse result1 = subject.adminClose(queryId.toString());
        
        // Verify results
        assertNotNull(result1, "Expected a non-null response performing an admin close");
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testAdminClose_NullTupleReturnedAndQueryExceptionThrown() throws Exception {
        // Set local test input
        UUID queryId = UUID.randomUUID();
        
        // Set expectations
        when(this.connectionRequestBean.adminCancelConnectionRequest(queryId.toString())).thenReturn(false);
        when(this.qlCache.poll(queryId.toString())).thenReturn(null);
        when(this.cache.get(queryId.toString())).thenReturn(null);
        when(this.persister.adminFindById(queryId.toString())).thenReturn(null);
        
        // Run the test
        EasyMock.replay();
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        assertThrows(DatawaveWebApplicationException.class, () -> subject.adminClose(queryId.toString()));
        
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testCancel_HappyPath() throws Exception {
        // Set local test input
        String userName = "userName";
        UUID queryId = UUID.randomUUID();
        
        // Set expectations of the create logic
        when(this.connectionRequestBean.cancelConnectionRequest(queryId.toString())).thenReturn(false);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.qlCache.pollIfOwnedBy(queryId.toString(), userName)).thenReturn(this.tuple);
        this.closedCache.remove(queryId.toString());
        when(this.tuple.getFirst()).thenReturn((QueryLogic) this.queryLogic1);
        this.queryLogic1.close();
        when(this.tuple.getSecond()).thenReturn(this.connector);
        this.connectionFactory.returnConnection(this.connector);
        
        // Run the test
        EasyMock.replay();
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        VoidResponse result1 = subject.cancel(queryId.toString());
        
        // Verify results
        assertNotNull(result1, "Expected a non-null response");
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testCancel_NullTupleReturnedAndQueryExceptionThrown() throws Exception {
        // Set local test input
        String userName = "userName";
        UUID queryId = UUID.randomUUID();
        String userSid = "userSid";
        String queryAuthorizations = "AUTH_1";
        
        // Set expectations of the create logic
        when(this.connectionRequestBean.cancelConnectionRequest(queryId.toString())).thenReturn(false);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.qlCache.pollIfOwnedBy(queryId.toString(), userName)).thenReturn(null);
        when(this.closedCache.exists(queryId.toString())).thenReturn(false);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        when(this.cache.get(queryId.toString())).thenReturn(null);
        when(this.persister.findById(queryId.toString())).thenReturn(Arrays.asList((Query) this.query, this.query));
        
        // Run the test
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        
        assertThrows(DatawaveWebApplicationException.class, () -> subject.cancel(queryId.toString()));
        
    }
    
    @Disabled
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testCancel_RunningQueryFoundInCache() throws Exception {
        try (MockedStatic<Trace> traceMock = Mockito.mockStatic(Trace.class)) {
            traceMock.when(() -> Trace.trace(this.traceInfo, "query:close")).thenReturn(this.span);
            // Set local test input
            String userName = "userName";
            UUID queryId = UUID.randomUUID();
            String userSid = "userSid";
            String queryAuthorizations = "AUTH_1";
            
            // Set expectations of the create logic
            when(this.connectionRequestBean.cancelConnectionRequest(queryId.toString())).thenReturn(false);
            when(this.context.getCallerPrincipal()).thenReturn(this.principal);
            when(this.principal.getName()).thenReturn(userName);
            when(this.qlCache.pollIfOwnedBy(queryId.toString(), userName)).thenReturn(null);
            when(this.principal.getName()).thenReturn(userName);
            when(this.principal.getShortName()).thenReturn(userSid);
            when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
            when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
            this.closedCache.remove(queryId.toString());
            when(this.runningQuery.getSettings()).thenReturn(this.query);
            when(this.query.getOwner()).thenReturn(userSid);
            this.runningQuery.cancel();
            this.runningQuery.closeConnection(this.connectionFactory);
            when(this.query.getId()).thenReturn(queryId);
            cache.remove(queryId.toString());
            when(this.runningQuery.getTraceInfo()).thenReturn(this.traceInfo);
            this.span.data(eq("closedAt"), isA(String.class));
            this.span.stop();
            
            // Run the test
            QueryExecutorBean subject = new QueryExecutorBean();
            ReflectionTestUtils.setField(subject, "ctx", context);
            ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
            ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
            ReflectionTestUtils.setField(subject, "qlCache", qlCache);
            ReflectionTestUtils.setField(subject, "queryCache", cache);
            ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
            ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
            ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
            ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
            VoidResponse result1 = subject.cancel(queryId.toString());
            // Verify results
            assertNotNull(result1, "Expected a non-null response");
        }
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testClose_NullTupleReturnedFromQueryLogicCache() throws Exception {
        // Set local test input
        String userName = "userName";
        String userSid = "userSid";
        UUID queryId = UUID.randomUUID();
        String queryAuthorizations = "AUTH_1";
        
        // Set expectations
        when(this.connectionRequestBean.cancelConnectionRequest(queryId.toString(), this.principal)).thenReturn(false);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        when(this.qlCache.pollIfOwnedBy(queryId.toString(), userSid)).thenReturn(null);
        when(this.closedCache.exists(queryId.toString())).thenReturn(false);
        when(this.cache.get(queryId.toString())).thenReturn(null);
        when(this.persister.findById(queryId.toString())).thenReturn(new ArrayList<>(0));
        
        // Run the te
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        
        assertThrows(DatawaveWebApplicationException.class, () -> subject.close(queryId.toString()));
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testClose_UncheckedException() throws Exception {
        // Set local test input
        String userSid = "userSid";
        UUID queryId = UUID.randomUUID();
        
        // Set expectations
        when(this.connectionRequestBean.cancelConnectionRequest(queryId.toString(), this.principal)).thenReturn(false);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.qlCache.pollIfOwnedBy(queryId.toString(), userSid)).thenReturn(this.tuple);
        when(this.tuple.getFirst()).thenReturn((QueryLogic) this.queryLogic1);
        this.queryLogic1.close();
        when(this.tuple.getSecond()).thenThrow(ILLEGAL_STATE_EXCEPTION);
        
        // Run the te
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        
        assertThrows(DatawaveWebApplicationException.class, () -> subject.close(queryId.toString()));
        
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testCreateQueryAndNext_HappyPath() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 999999);
        int pagesize = 10;
        int pageTimeout = -1;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        boolean trace = false;
        String userName = "userName";
        String userSid = "userSid";
        String userDN = "userdn";
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);
        List<String> dnList = Collections.singletonList(userDN);
        UUID queryId = UUID.randomUUID();
        long pageNumber = 0L;
        
        HashMap<String,Collection<String>> authsMap = new HashMap<>();
        authsMap.put("userdn", Arrays.asList(queryAuthorizations));
        
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putSingle(QueryParameters.QUERY_STRING, query);
        queryParameters.putSingle(QueryParameters.QUERY_NAME, queryName);
        queryParameters.putSingle(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.putSingle(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        queryParameters.putSingle("valid", "param");
        
        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);
        
        QueryParameters qp = new QueryParametersImpl();
        qp.validate(queryParameters);
        
        MultivaluedMap<String,String> op = new MultivaluedMapImpl<>();
        op.putAll(qp.getUnknownParameters(queryParameters));
        // op.putSingle(PrivateAuditConstants.AUDIT_TYPE, AuditType.NONE.name());
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());
        
        // Set expectations of the create logic
        queryLogic1.validate(queryParameters);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getMaxPageSize()).thenReturn(1000);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getUserDN()).thenReturn(userDNpair);
        when(this.principal.getDNs()).thenReturn(new String[] {userDN});
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).thenReturn(true);
        when(this.queryLogic1.getAuditType(null)).thenReturn(Auditor.AuditType.NONE);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        when(persister.create(eq(userDNpair.subjectDN()), eq(dnList), eq(marking), eq(queryLogicName), eq(qp), eq(op))).thenReturn(this.query);
        when(this.queryLogic1.getAuditType(this.query)).thenReturn(Auditor.AuditType.NONE);
        when(this.queryLogic1.getConnectionPriority()).thenReturn(Priority.NORMAL);
        when(this.queryLogic1.getConnPoolName()).thenReturn("connPool1");
        when(this.queryLogic1.isLongRunningQuery()).thenReturn(false);
        when(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).thenReturn(null);
        this.connectionRequestBean.requestBegin(queryId.toString());
        when(this.connectionFactory.getConnection("connPool1", Priority.NORMAL, null)).thenReturn(this.connector);
        this.connectionRequestBean.requestEnd(queryId.toString());
        when(this.traceInfos.get(userSid)).thenReturn(new ArrayList<>(0));
        when(this.traceInfos.get(null)).thenReturn(Arrays.asList(PatternWrapper.wrap("NONMATCHING_REGEX")));
        when(this.qlCache.add(queryId.toString(), userSid, this.queryLogic1, this.connector)).thenReturn(true);
        when(this.queryLogic1.getCollectQueryMetrics()).thenReturn(true);
        when(this.principal.getPrimaryUser()).thenReturn(dwUser);
        when(this.dwUser.getAuths()).thenReturn(Collections.singleton(queryAuthorizations));
        when(this.principal.getProxiedUsers()).thenReturn(Collections.singletonList(dwUser));
        when(this.query.getOwner()).thenReturn(userSid);
        when(this.query.getId()).thenReturn(queryId);
        when(this.query.getQuery()).thenReturn(queryName);
        when(this.query.getQueryLogicName()).thenReturn(queryLogicName);
        when(this.query.getBeginDate()).thenReturn(null);
        when(this.query.getEndDate()).thenReturn(null);
        when(this.query.getColumnVisibility()).thenReturn(null);
        when(this.query.getQueryAuthorizations()).thenReturn(queryAuthorizations);
        when(this.query.getQueryName()).thenReturn(null);
        when(this.query.getParameters()).thenReturn((Set) Collections.emptySet());
        when(this.query.getUncaughtExceptionHandler()).thenReturn(new QueryUncaughtExceptionHandler());
        this.metrics.updateMetric(isA(QueryMetric.class));
        when(this.query.getUserDN()).thenReturn(userDN);
        when(this.query.getDnList()).thenReturn(dnList);
        when(this.queryLogic1.getResultLimit(dnList)).thenReturn(-1L);
        when(this.queryLogic1.getMaxResults()).thenReturn(-1L);
        when(this.queryLogic1.initialize(eq(this.connector), eq(this.query), isA(Set.class))).thenReturn(this.genericConfiguration);
        this.queryLogic1.setupQuery(this.genericConfiguration);
        when(this.queryLogic1.getTransformIterator(this.query)).thenReturn(this.transformIterator);
        cache.put(eq(queryId.toString()), isA(RunningQuery.class));
        when(this.genericConfiguration.getQueryString()).thenReturn(queryName);
        when(this.qlCache.poll(queryId.toString())).thenReturn(null);
        
        // Set expectations of the next logic
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.context.getUserTransaction()).thenReturn(this.transaction);
        
        this.transaction.begin();
        when(this.cache.get(queryId.toString())).thenReturn(this.runningQuery);
        when(cache.lock(queryId.toString())).thenReturn(true);
        when(this.runningQuery.getSettings()).thenReturn(this.query);
        when(this.runningQuery.getConnection()).thenReturn(this.connector);
        
        this.runningQuery.setActiveCall(true);
        when(this.runningQuery.getTraceInfo()).thenReturn(this.traceInfo);
        when(this.runningQuery.next()).thenReturn(this.resultsPage);
        when(this.runningQuery.getLastPageNumber()).thenReturn(pageNumber);
        when(this.runningQuery.getLogic()).thenReturn((QueryLogic) this.queryLogic1);
        when(this.runningQuery.getSettings()).thenReturn(this.query);
        when(this.queryLogic1.getEnrichedTransformer(this.query)).thenReturn(this.transformer);
        when(this.transformer.createResponse(this.resultsPage)).thenReturn(this.baseResponse);
        when(this.resultsPage.getStatus()).thenReturn(ResultsPage.Status.COMPLETE);
        this.baseResponse.setHasResults(true);
        this.baseResponse.setPageNumber(pageNumber);
        when(this.queryLogic1.getLogicName()).thenReturn(queryLogicName);
        this.baseResponse.setLogicName(queryLogicName);
        this.baseResponse.setQueryId(queryId.toString());
        when(this.runningQuery.getMetric()).thenReturn(this.queryMetric);
        this.runningQuery.setActiveCall(false);
        this.queryMetric.setProxyServers(eq(new ArrayList<>(0)));
        when(this.responseObjectFactory.getEventQueryResponse()).thenReturn(new DefaultEventQueryResponse());
        
        cache.unlock(queryId.toString());
        when(this.transaction.getStatus()).thenReturn(Status.STATUS_ACTIVE);
        this.transaction.commit();
        // Run the test
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "auditor", auditor);
        ReflectionTestUtils.setField(subject, "metrics", metrics);
        ReflectionTestUtils.setField(subject, "traceInfos", traceInfos);
        ReflectionTestUtils.setField(subject, "marking", new ColumnVisibilitySecurityMarking());
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        BaseQueryResponse result1 = subject.createQueryAndNext(queryLogicName, queryParameters);
        
        // Verify results
        assertNotNull(result1, "Expected a non-null response");
    }
    
    @Test
    public void testCreateQueryAndNext_BadID() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 999999);
        int pagesize = 10;
        int pageTimeout = -1;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        boolean trace = false;
        String userName = "userName";
        String userSid = "userSid";
        String userDN = "userdn";
        String errorCode = DatawaveErrorCode.INVALID_QUERY_ID.getErrorCode();
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);
        List<String> dnList = Collections.singletonList(userDN);
        UUID queryId = UUID.randomUUID();
        
        HashMap<String,Collection<String>> authsMap = new HashMap<>();
        authsMap.put("userdn", Arrays.asList(queryAuthorizations));
        
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putSingle(QueryParameters.QUERY_STRING, query);
        queryParameters.putSingle(QueryParameters.QUERY_NAME, queryName);
        queryParameters.putSingle(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.putSingle(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        queryParameters.putSingle("valid", "param");
        
        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);
        
        QueryParameters qp = new QueryParametersImpl();
        qp.validate(queryParameters);
        
        MultivaluedMap<String,String> op = new MultivaluedMapImpl<>();
        op.putAll(qp.getUnknownParameters(queryParameters));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());
        
        // Set expectations of the create logic
        queryLogic1.validate(queryParameters);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getMaxPageSize()).thenReturn(1000);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getUserDN()).thenReturn(userDNpair);
        when(this.principal.getDNs()).thenReturn(new String[] {userDN});
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).thenReturn(true);
        when(this.queryLogic1.getAuditType(null)).thenReturn(Auditor.AuditType.NONE);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        when(persister.create(eq(userDNpair.subjectDN()), eq(dnList), eq(marking), eq(queryLogicName), eq(qp), eq(op))).thenReturn(this.query);
        when(this.queryLogic1.getAuditType(this.query)).thenReturn(Auditor.AuditType.NONE);
        when(this.queryLogic1.getConnectionPriority()).thenReturn(Priority.NORMAL);
        when(this.queryLogic1.getConnPoolName()).thenReturn("connPool1");
        when(this.queryLogic1.isLongRunningQuery()).thenReturn(false);
        when(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).thenReturn(null);
        this.connectionRequestBean.requestBegin(queryId.toString());
        when(this.connectionFactory.getConnection("connPool1", Priority.NORMAL, null)).thenReturn(this.connector);
        this.connectionRequestBean.requestEnd(queryId.toString());
        when(this.traceInfos.get(userSid)).thenReturn(new ArrayList<>(0));
        when(this.traceInfos.get(null)).thenReturn(Arrays.asList(PatternWrapper.wrap("NONMATCHING_REGEX")));
        when(this.qlCache.add(queryId.toString(), userSid, this.queryLogic1, this.connector)).thenReturn(true);
        when(this.queryLogic1.getCollectQueryMetrics()).thenReturn(true);
        when(this.principal.getPrimaryUser()).thenReturn(dwUser);
        when(this.dwUser.getAuths()).thenReturn(Collections.singleton(queryAuthorizations));
        when(this.principal.getProxiedUsers()).thenReturn(Collections.singletonList(dwUser));
        when(this.query.getOwner()).thenReturn(userSid);
        when(this.query.getId()).thenReturn(queryId);
        when(this.query.getQuery()).thenReturn(queryName);
        when(this.query.getQueryLogicName()).thenReturn(queryLogicName);
        when(this.query.getBeginDate()).thenReturn(null);
        when(this.query.getEndDate()).thenReturn(null);
        when(this.query.getColumnVisibility()).thenReturn(null);
        when(this.query.getQueryAuthorizations()).thenReturn(queryAuthorizations);
        when(this.query.getQueryName()).thenReturn(null);
        this.metrics.updateMetric(isA(QueryMetric.class));
        when(this.query.getUserDN()).thenReturn(userDN);
        when(this.query.getDnList()).thenReturn(dnList);
        when(this.queryLogic1.getResultLimit(dnList)).thenReturn(-1L);
        when(this.queryLogic1.getMaxResults()).thenReturn(-1L);
        when(this.queryLogic1.initialize(eq(this.connector), eq(this.query), isA(Set.class))).thenReturn(this.genericConfiguration);
        this.queryLogic1.setupQuery(this.genericConfiguration);
        when(this.queryLogic1.getTransformIterator(this.query)).thenReturn(this.transformIterator);
        cache.put(eq(queryId.toString()), isA(RunningQuery.class));
        when(this.genericConfiguration.getQueryString()).thenReturn(queryName);
        when(this.qlCache.poll(queryId.toString())).thenReturn(null);
        
        // Set expectations of the next logic
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.context.getUserTransaction()).thenReturn(this.transaction);
        when(this.transaction.getStatus()).thenReturn(Status.STATUS_NO_TRANSACTION);
        cache.unlock("{id}");
        this.transaction.begin();
        this.transaction.setRollbackOnly();
        
        when(this.responseObjectFactory.getEventQueryResponse()).thenReturn(new DefaultEventQueryResponse());
        this.transaction.commit();
        QueryExecutorBean subject = new QueryExecutorBean();
        // Run the test
        
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "auditor", auditor);
        ReflectionTestUtils.setField(subject, "metrics", metrics);
        ReflectionTestUtils.setField(subject, "traceInfos", traceInfos);
        ReflectionTestUtils.setField(subject, "marking", new ColumnVisibilitySecurityMarking());
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        subject.createQuery(queryLogicName, queryParameters);
        
        Throwable result1 = null;
        try {
            subject.next("{id}");
        } catch (Exception e) {
            result1 = e.getCause();
            assertTrue(e.getCause().toString().contains("BadRequestQueryException: Invalid query id."));
            assertEquals(e.getMessage(), "HTTP 400 Bad Request");
            assertTrue(((BadRequestQueryException) result1).getErrorCode().equals(errorCode));
        }
        
        assertNotNull(result1, "Expected a non-null response");
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testCreateQueryAndNext_DoubleAuditValues() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 999999);
        int pagesize = 10;
        int pageTimeout = -1;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        boolean trace = false;
        String userName = "userName";
        String userSid = "userSid";
        String userDN = "userDN";
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);
        List<String> dnList = Collections.singletonList(userDN);
        UUID queryId = UUID.randomUUID();
        long pageNumber = 0L;
        
        HashMap<String,Collection<String>> authsMap = new HashMap<>();
        authsMap.put("userdn", Arrays.asList(queryAuthorizations));
        
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putSingle(QueryParameters.QUERY_STRING, query);
        queryParameters.putSingle(QueryParameters.QUERY_NAME, queryName);
        queryParameters.putSingle(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.putSingle(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        
        queryParameters.putSingle(QueryParameters.QUERY_PARAMS, "auditType:NONE;auditColumnVisibility:A&B&C&D;auditUserDN:" + userDN);
        
        queryParameters.putSingle("valid", "param");
        
        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);
        
        QueryParameters qp = new QueryParametersImpl();
        qp.validate(queryParameters);
        
        MultivaluedMap<String,String> op = new MultivaluedMapImpl<>();
        op.putAll(qp.getUnknownParameters(queryParameters));
        // op.putSingle(PrivateAuditConstants.AUDIT_TYPE, AuditType.NONE.name());
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());
        
        // Set expectations of the create logic
        queryLogic1.validate(queryParameters);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getMaxPageSize()).thenReturn(1000);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getUserDN()).thenReturn(userDNpair);
        when(this.principal.getDNs()).thenReturn(new String[] {userDN});
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).thenReturn(true);
        when(this.queryLogic1.getAuditType(null)).thenReturn(Auditor.AuditType.NONE);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        when(this.principal.getPrimaryUser()).thenReturn(dwUser);
        when(this.dwUser.getAuths()).thenReturn(Collections.singleton(queryAuthorizations));
        when(this.principal.getProxiedUsers()).thenReturn(Collections.singletonList(dwUser));
        when(persister.create(eq(userDNpair.subjectDN()), eq(dnList), eq(marking), eq(queryLogicName), eq(qp), eq(op))).thenReturn(this.query);
        when(this.queryLogic1.getAuditType(this.query)).thenReturn(Auditor.AuditType.NONE);
        when(this.queryLogic1.getConnectionPriority()).thenReturn(Priority.NORMAL);
        when(this.queryLogic1.getConnPoolName()).thenReturn("connPool1");
        when(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).thenReturn(null);
        when(this.connectionFactory.getConnection("connPool1", Priority.NORMAL, null)).thenReturn(this.connector);
        when(this.traceInfos.get(userSid)).thenReturn(new ArrayList<>(0));
        when(this.traceInfos.get(null)).thenReturn(Arrays.asList(PatternWrapper.wrap("NONMATCHING_REGEX")));
        when(this.qlCache.add(queryId.toString(), userSid, this.queryLogic1, this.connector)).thenReturn(true);
        when(this.queryLogic1.getCollectQueryMetrics()).thenReturn(true);
        when(this.query.getOwner()).thenReturn(userSid);
        when(this.query.getId()).thenReturn(queryId);
        when(this.query.getQuery()).thenReturn(queryName);
        when(this.query.getQueryLogicName()).thenReturn(queryLogicName);
        when(this.query.getBeginDate()).thenReturn(null);
        when(this.query.getEndDate()).thenReturn(null);
        when(this.query.getColumnVisibility()).thenReturn(null);
        when(this.query.getQueryAuthorizations()).thenReturn(queryAuthorizations);
        when(this.query.getQueryName()).thenReturn(null);
        this.metrics.updateMetric(isA(QueryMetric.class));
        when(this.query.getUserDN()).thenReturn(userDN);
        when(this.query.getDnList()).thenReturn(dnList);
        when(this.queryLogic1.isLongRunningQuery()).thenReturn(false);
        when(this.queryLogic1.getResultLimit(dnList)).thenReturn(-1L);
        when(this.queryLogic1.getMaxResults()).thenReturn(-1L);
        when(this.queryLogic1.initialize(eq(this.connector), eq(this.query), isA(Set.class))).thenReturn(this.genericConfiguration);
        this.queryLogic1.setupQuery(this.genericConfiguration);
        when(this.queryLogic1.getTransformIterator(this.query)).thenReturn(this.transformIterator);
        when(this.genericConfiguration.getQueryString()).thenReturn(queryName);
        cache.put(eq(queryId.toString()), isA(RunningQuery.class));
        when(this.qlCache.poll(queryId.toString())).thenReturn(null);
        
        // Set expectations of the next logic
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.context.getUserTransaction()).thenReturn(this.transaction);
        
        this.transaction.begin();
        when(this.cache.get(queryId.toString())).thenReturn(this.runningQuery);
        when(cache.lock(queryId.toString())).thenReturn(true);
        when(this.runningQuery.getSettings()).thenReturn(this.query);
        this.connectionRequestBean.requestBegin(queryId.toString());
        when(this.runningQuery.getConnection()).thenReturn(this.connector);
        this.connectionRequestBean.requestEnd(queryId.toString());
        
        this.runningQuery.setActiveCall(true);
        when(this.runningQuery.getTraceInfo()).thenReturn(this.traceInfo);
        when(this.runningQuery.next()).thenReturn(this.resultsPage);
        when(this.runningQuery.getLastPageNumber()).thenReturn(pageNumber);
        when(this.runningQuery.getLogic()).thenReturn((QueryLogic) this.queryLogic1);
        when(this.runningQuery.getSettings()).thenReturn(this.query);
        when(this.queryLogic1.getEnrichedTransformer(this.query)).thenReturn(this.transformer);
        when(this.transformer.createResponse(this.resultsPage)).thenReturn(this.baseResponse);
        when(this.resultsPage.getStatus()).thenReturn(ResultsPage.Status.COMPLETE);
        this.baseResponse.setHasResults(true);
        this.baseResponse.setPageNumber(pageNumber);
        when(this.queryLogic1.getLogicName()).thenReturn(queryLogicName);
        this.baseResponse.setLogicName(queryLogicName);
        this.baseResponse.setQueryId(queryId.toString());
        when(this.runningQuery.getMetric()).thenReturn(this.queryMetric);
        this.runningQuery.setActiveCall(false);
        this.queryMetric.setProxyServers(eq(new ArrayList<>(0)));
        when(this.responseObjectFactory.getEventQueryResponse()).thenReturn(new DefaultEventQueryResponse());
        
        cache.unlock(queryId.toString());
        when(this.transaction.getStatus()).thenReturn(Status.STATUS_ACTIVE);
        this.transaction.commit();
        
        // Run the te
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "auditor", auditor);
        ReflectionTestUtils.setField(subject, "metrics", metrics);
        ReflectionTestUtils.setField(subject, "traceInfos", traceInfos);
        ReflectionTestUtils.setField(subject, "marking", new ColumnVisibilitySecurityMarking());
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        BaseQueryResponse result1 = subject.createQueryAndNext(queryLogicName, queryParameters);
        
        // Verify results
        assertNotNull(result1, "Expected a non-null response");
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testCreateQueryAndNext_AddToCacheException() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 9999);
        int pagesize = 10;
        int pageTimeout = -1;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        boolean trace = false;
        String userName = "userName";
        String userSid = "userSid";
        String userDN = "userDN";
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);
        List<String> dnList = Collections.singletonList(userDN);
        UUID queryId = UUID.randomUUID();
        
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putSingle(QueryParameters.QUERY_STRING, query);
        queryParameters.putSingle(QueryParameters.QUERY_NAME, queryName);
        queryParameters.putSingle(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.putSingle(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.putSingle("valid", "param");
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        
        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);
        
        QueryParameters qp = new QueryParametersImpl();
        qp.validate(queryParameters);
        
        MultivaluedMap<String,String> op = new MultivaluedMapImpl<>();
        op.putAll(qp.getUnknownParameters(queryParameters));
        // op.putSingle(PrivateAuditConstants.AUDIT_TYPE, AuditType.ACTIVE.name());
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());
        
        // Set expectations
        when(context.getCallerPrincipal()).thenReturn(principal);
        queryLogic1.validate(queryParameters);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getMaxPageSize()).thenReturn(1000);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getUserDN()).thenReturn(userDNpair);
        when(this.principal.getDNs()).thenReturn(new String[] {userDN});
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).thenReturn(true);
        when(this.queryLogic1.getAuditType(null)).thenReturn(Auditor.AuditType.ACTIVE);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        when(persister.create(eq(userDNpair.subjectDN()), eq(dnList), eq(marking), eq(queryLogicName), eq(qp), eq(op))).thenReturn(this.query);
        when(this.queryLogic1.getAuditType(this.query)).thenReturn(Auditor.AuditType.ACTIVE);
        when(this.queryLogic1.getSelectors(this.query)).thenReturn(null);
        when(auditor.audit(eq(queryParameters))).thenReturn(null);
        when(this.queryLogic1.getConnectionPriority()).thenReturn(Priority.NORMAL);
        when(this.queryLogic1.getConnPoolName()).thenReturn("connPool1");
        when(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).thenReturn(null);
        this.connectionRequestBean.requestBegin(queryId.toString());
        when(this.connectionFactory.getConnection("connPool1", Priority.NORMAL, null)).thenReturn(this.connector);
        this.connectionRequestBean.requestEnd(queryId.toString());
        when(this.traceInfos.get(userSid)).thenReturn(Arrays.asList(PatternWrapper.wrap(query)));
        when(this.qlCache.add(queryId.toString(), userSid, this.queryLogic1, this.connector)).thenThrow(
                        new IllegalStateException("INTENTIONALLY THROWN TEST EXCEPTION: PROBLEM ADDING QUERY LOGIC TO CACHE"));
        this.queryLogic1.close();
        this.connectionFactory.returnConnection(this.connector);
        this.persister.remove(this.query);
        when(this.query.getId()).thenReturn(queryId);
        when(this.qlCache.poll(queryId.toString())).thenReturn(null);
        
        // Run the te
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "auditor", auditor);
        ReflectionTestUtils.setField(subject, "traceInfos", traceInfos);
        ReflectionTestUtils.setField(subject, "marking", new ColumnVisibilitySecurityMarking());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        Throwable result1 = null;
        try {
            subject.createQueryAndNext(queryLogicName, queryParameters);
        } catch (DatawaveWebApplicationException e) {
            result1 = e.getCause();
        }
        
        // Verify results
        assertTrue(result1 instanceof QueryException, "QueryException expected to have been thrown");
        assertEquals("500-7", ((QueryException) result1).getErrorCode(), "Exception expected to have been caused by problem adding query logic to cache");
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testCreateQueryAndNext_ButNoResults() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 999999);
        int pagesize = 10;
        int pageTimeout = -1;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        boolean trace = false;
        String userName = "userName";
        String userSid = "userSid";
        String userDN = "userdn";
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);
        List<String> dnList = Collections.singletonList(userDN);
        UUID queryId = UUID.randomUUID();
        long pageNumber = 0L;
        
        HashMap<String,Collection<String>> authsMap = new HashMap<>();
        authsMap.put("userdn", Arrays.asList(queryAuthorizations));
        
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putSingle(QueryParameters.QUERY_STRING, query);
        queryParameters.putSingle(QueryParameters.QUERY_NAME, queryName);
        queryParameters.putSingle(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.putSingle(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        queryParameters.putSingle("valid", "param");
        
        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);
        
        QueryParameters qp = new QueryParametersImpl();
        qp.validate(queryParameters);
        
        MultivaluedMap<String,String> op = new MultivaluedMapImpl<>();
        op.putAll(qp.getUnknownParameters(queryParameters));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());
        
        // Set expectations of the create logic
        queryLogic1.validate(queryParameters);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getMaxPageSize()).thenReturn(1000);
        when(this.queryLogic1.getConnPoolName()).thenReturn("connPool1");
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getUserDN()).thenReturn(userDNpair);
        when(this.principal.getDNs()).thenReturn(new String[] {userDN});
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).thenReturn(true);
        when(this.queryLogic1.getAuditType(null)).thenReturn(Auditor.AuditType.NONE);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        when(this.principal.getPrimaryUser()).thenReturn(dwUser);
        when(this.dwUser.getAuths()).thenReturn(Collections.singleton(queryAuthorizations));
        when(this.principal.getProxiedUsers()).thenReturn(Collections.singletonList(dwUser));
        when(persister.create(eq(userDNpair.subjectDN()), eq(dnList), eq(marking), eq(queryLogicName), eq(qp), eq(op))).thenReturn(this.query);
        when(this.queryLogic1.getAuditType(this.query)).thenReturn(Auditor.AuditType.NONE);
        when(this.queryLogic1.getConnectionPriority()).thenReturn(Priority.NORMAL);
        when(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).thenReturn(null);
        when(this.traceInfos.get(userSid)).thenReturn(new ArrayList<>(0));
        when(this.traceInfos.get(null)).thenReturn(Arrays.asList(PatternWrapper.wrap("NONMATCHING_REGEX")));
        when(this.qlCache.add(queryId.toString(), userSid, this.queryLogic1, this.connector)).thenReturn(true);
        when(this.queryLogic1.getCollectQueryMetrics()).thenReturn(true);
        when(this.query.getOwner()).thenReturn(userSid);
        when(this.query.getId()).thenReturn(queryId);
        when(this.query.getQuery()).thenReturn(queryName);
        when(this.query.getQueryLogicName()).thenReturn(queryLogicName);
        when(this.query.getBeginDate()).thenReturn(null);
        when(this.query.getEndDate()).thenReturn(null);
        when(this.query.getQueryAuthorizations()).thenReturn(queryAuthorizations);
        when(this.query.getQueryName()).thenReturn(null);
        when(this.query.getColumnVisibility()).thenReturn(null);
        this.metrics.updateMetric(isA(QueryMetric.class));
        when(this.query.getUserDN()).thenReturn(userDN);
        when(this.query.getDnList()).thenReturn(dnList);
        when(this.queryLogic1.isLongRunningQuery()).thenReturn(false);
        when(this.queryLogic1.getResultLimit(dnList)).thenReturn(-1L);
        when(this.queryLogic1.getMaxResults()).thenReturn(-1L);
        when(this.queryLogic1.initialize(eq(this.connector), eq(this.query), isA(Set.class))).thenReturn(this.genericConfiguration);
        this.queryLogic1.setupQuery(this.genericConfiguration);
        when(this.queryLogic1.getTransformIterator(this.query)).thenReturn(this.transformIterator);
        when(this.genericConfiguration.getQueryString()).thenReturn(queryName);
        this.cache.put(eq(queryId.toString()), isA(RunningQuery.class));
        when(this.qlCache.poll(queryId.toString())).thenReturn(null);
        
        // Set expectations of the next logic
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.context.getUserTransaction()).thenReturn(this.transaction);
        this.transaction.begin();
        when(this.cache.get(queryId.toString())).thenReturn(this.runningQuery);
        when(cache.lock(queryId.toString())).thenReturn(true);
        when(this.runningQuery.getSettings()).thenReturn(this.query);
        this.connectionRequestBean.requestBegin(queryId.toString());
        when(this.runningQuery.getConnection()).thenReturn(this.connector);
        this.connectionRequestBean.requestEnd(queryId.toString());
        this.runningQuery.setActiveCall(true);
        when(this.runningQuery.getTraceInfo()).thenReturn(this.traceInfo);
        when(this.runningQuery.next()).thenReturn(this.resultsPage);
        when(this.runningQuery.getLastPageNumber()).thenReturn(pageNumber);
        when(this.runningQuery.getLogic()).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getEnrichedTransformer(this.query)).thenReturn(this.transformer);
        when(this.transformer.createResponse(this.resultsPage)).thenReturn(this.baseResponse);
        when(this.resultsPage.getStatus()).thenReturn(ResultsPage.Status.NONE);
        this.baseResponse.setHasResults(false);
        this.baseResponse.setPageNumber(pageNumber);
        when(this.queryLogic1.getLogicName()).thenReturn(queryLogicName);
        this.baseResponse.setLogicName(queryLogicName);
        this.baseResponse.setQueryId(queryId.toString());
        when(this.runningQuery.getMetric()).thenReturn(this.queryMetric);
        this.runningQuery.setActiveCall(false);
        this.queryMetric.setProxyServers(eq(new ArrayList<>(0)));
        this.baseResponse.addException(isA(NoResultsQueryException.class));
        
        when(this.runningQuery.getLogic()).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getCollectQueryMetrics()).thenReturn(true);
        
        this.metrics.updateMetric(this.queryMetric);
        cache.unlock(queryId.toString());
        this.transaction.setRollbackOnly();
        when(this.transaction.getStatus()).thenReturn(Status.STATUS_ACTIVE);
        this.transaction.commit();
        
        // Set expectations
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        when(this.connectionRequestBean.cancelConnectionRequest(queryId.toString(), this.principal)).thenReturn(false);
        when(this.qlCache.pollIfOwnedBy(queryId.toString(), userSid)).thenReturn(null);
        when(this.cache.get(queryId.toString())).thenReturn(this.runningQuery);
        when(this.connectionFactory.getConnection("connPool1", Priority.NORMAL, null)).thenReturn(this.connector);
        this.runningQuery.closeConnection(this.connectionFactory);
        this.cache.remove(queryId.toString());
        this.closedCache.add(queryId.toString());
        this.closedCache.remove(queryId.toString());
        when(this.runningQuery.getTraceInfo()).thenReturn(null);
        when(this.responseObjectFactory.getEventQueryResponse()).thenReturn(new DefaultEventQueryResponse());
        
        // Run the te
        QueryExecutorBean subject = new QueryExecutorBean();
        
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "auditor", auditor);
        ReflectionTestUtils.setField(subject, "metrics", metrics);
        ReflectionTestUtils.setField(subject, "traceInfos", traceInfos);
        ReflectionTestUtils.setField(subject, "marking", new ColumnVisibilitySecurityMarking());
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        
        assertThrows(NoResultsException.class, () -> subject.createQueryAndNext(queryLogicName, queryParameters));
    }
    
    @Test
    public void testCreateQueryAndNext_InvalidExpirationDate() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime - 500);
        int pagesize = 1;
        int pageTimeout = -1;
        Long maxResultsOverride = null;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        String parameters = null;
        boolean trace = false;
        
        MultivaluedMap<String,String> p = new MultivaluedMapImpl<>();
        p.putAll(QueryParametersImpl.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations, expirationDate,
                        pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters, trace));
        
        // Run the te
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        Throwable result1 = assertThrows(DatawaveWebApplicationException.class, () -> subject.createQueryAndNext(queryLogicName, p));
        // Verify results
        assertTrue(result1 instanceof BadRequestException, "BadRequestException expected to have been thrown");
        assertEquals("400-3", ((QueryException) result1.getCause()).getErrorCode(), "Thrown exception expected to have been due to invalid expiration date");
    }
    
    @Test
    public void testCreateQueryAndNext_InvalidPageSize() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime);
        int pagesize = 0;
        int pageTimeout = -1;
        Long maxResultsOverride = null;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        String parameters = null;
        boolean trace = false;
        
        MultivaluedMap<String,String> p = new MultivaluedMapImpl<>();
        p.putAll(QueryParametersImpl.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations, expirationDate,
                        pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters, trace));
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        Throwable result1 = null;
        try {
            subject.createQueryAndNext(queryLogicName, p);
            
        } catch (DatawaveWebApplicationException e) {
            result1 = e;
        }
        
        // Verify results
        assertTrue(result1 instanceof BadRequestException, "BadRequestException expected to have been thrown");
        assertEquals("400-2", ((QueryException) result1.getCause()).getErrorCode(), "Thrown exception expected to have been due to invalid page size");
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testCreateQueryAndNext_PageSizeExceedsConfiguredMax() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 9999);
        int pagesize = 1000;
        int pageTimeout = -1;
        Long maxResultsOverride = null;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        String parameters = null;
        boolean trace = false;
        // Set expectations
        
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putAll(QueryParametersImpl.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                        expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters, trace));
        
        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);
        
        QueryParameters qp = new QueryParametersImpl();
        qp.validate(queryParameters);
        
        MultivaluedMap<String,String> op = new MultivaluedMapImpl<>();
        op.putAll(qp.getUnknownParameters(queryParameters));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        
        queryLogic1.validate(queryParameters);
        when(this.principal.getName()).thenReturn("userName Full");
        when(this.principal.getShortName()).thenReturn("userName");
        when(this.principal.getUserDN()).thenReturn(SubjectIssuerDNPair.of("userDN"));
        when(this.principal.getDNs()).thenReturn(new String[] {"userDN"});
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.queryLogic1.containsDNWithAccess(Collections.singletonList("userDN"))).thenReturn(true);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        when(this.queryLogic1.getMaxPageSize()).thenReturn(10);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "marking", new ColumnVisibilitySecurityMarking());
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        Throwable result1 = null;
        try {
            subject.createQueryAndNext(queryLogicName, queryParameters);
        } catch (DatawaveWebApplicationException e) {
            result1 = e.getCause();
        }
        
        // Verify results
        assertTrue(result1 instanceof QueryException, "QueryException expected to have been thrown");
        assertEquals("400-6", ((QueryException) result1).getErrorCode(), "Thrown exception expected to have been due to undefined query logic");
    }
    
    @Test
    public void testCreateQueryAndNext_UndefinedQueryLogic() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 999999);
        int pagesize = 1;
        int pageTimeout = -1;
        Long maxResultsOverride = null;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        String parameters = null;
        boolean trace = false;
        
        // Set expectations
        when(context.getCallerPrincipal()).thenReturn(principal);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, principal)).thenThrow(
                        new IllegalArgumentException("INTENTIONALLY THROWN TEST EXCEPTION: UNDEFINED QUERY LOGIC"));
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        Throwable result1 = null;
        try {
            MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
            queryParameters.putAll(QueryParametersImpl.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                            expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters, trace));
            
            subject.createQueryAndNext(queryLogicName, queryParameters);
            
        } catch (DatawaveWebApplicationException e) {
            result1 = e.getCause();
        }
        
        // Verify results
        assertTrue(result1 instanceof QueryException, "QueryException expected to have been thrown");
        assertTrue(result1.getCause().getMessage().toLowerCase().contains("undefined query logic"),
                        "Thrown exception expected to have been due to undefined query logic");
    }
    
    @Test
    public void testDefineQuery_InvalidExpirationDate() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime - 500);
        int pagesize = 1;
        int pageTimeout = -1;
        Long maxResultsOverride = null;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        String parameters = null;
        boolean trace = false;
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        Throwable result1 = null;
        try {
            MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
            queryParameters.putAll(QueryParametersImpl.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                            expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters, trace));
            
            subject.defineQuery(queryLogicName, queryParameters);
            
        } catch (DatawaveWebApplicationException e) {
            result1 = e;
        }
        
        // Verify results
        assertTrue(result1 instanceof BadRequestException, "BadRequestException expected to have been thrown");
        assertEquals("400-3", ((QueryException) result1.getCause()).getErrorCode(), "Thrown exception expected to have been due to invalid expiration date");
    }
    
    @Test
    public void testDefineQuery_InvalidPageSize() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime);
        int pagesize = 0;
        int pageTimeout = -1;
        Long maxResultsOverride = null;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        String parameters = null;
        
        boolean trace = false;
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        Throwable result1 = null;
        try {
            MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
            queryParameters.putAll(QueryParametersImpl.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                            expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters, trace));
            
            subject.defineQuery(queryLogicName, queryParameters);
        } catch (DatawaveWebApplicationException e) {
            result1 = e;
        }
        
        // Verify results
        assertTrue(result1 instanceof BadRequestException, "BadRequestException expected to have been thrown");
        assertEquals("400-2", ((QueryException) result1.getCause()).getErrorCode(), "Thrown exception expected to have been due to invalid page size");
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testDefineQuery_UncheckedException() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 10000);
        int pagesize = 10;
        int pageTimeout = -1;
        Long maxResultsOverride = null;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        String userName = "userName";
        String userSid = "userSid";
        String userDN = "userdn";
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);
        boolean trace = false;
        List<String> dnList = Collections.singletonList(userDN);
        UUID queryId = UUID.randomUUID();
        
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putAll(QueryParametersImpl.paramsToMap(null, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                        expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, null, trace));
        queryParameters.putSingle("valid", "param");
        
        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);
        
        QueryParameters qp = new QueryParametersImpl();
        assertThrows(IllegalArgumentException.class, () -> qp.validate(queryParameters));
        
    }
    
    @Test
    public void testDisableAllTracing_HappyPath() throws Exception {
        // Set expectations
        this.traceInfos.clear();
        this.traceCache.put("traceInfos", this.traceInfos);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "traceInfos", traceInfos);
        ReflectionTestUtils.setField(subject, "queryTraceCache", traceCache);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        VoidResponse result1 = subject.disableAllTracing();
        
        // Verify results
        assertNotNull(result1, "Expected a non-null response");
    }
    
    @Test
    public void testDisableTracing_NullChecksAndHappyPath() throws Exception {
        // Set local test input
        String user = "user";
        String queryRegex = "queryRegex";
        
        // Set expectations
        when(this.traceInfos.removeAll(user)).thenReturn(new ArrayList<>(0));
        this.traceCache.put("traceInfos", this.traceInfos);
        // when(this.traceInfos.remove(eq(user), notNull())).thenReturn(true);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "traceInfos", traceInfos);
        ReflectionTestUtils.setField(subject, "queryTraceCache", traceCache);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        Exception result1 = null;
        try {
            subject.disableTracing(null, null);
        } catch (BadRequestException e) {
            result1 = e;
        }
        VoidResponse result2 = subject.disableTracing(null, user);
        VoidResponse result3 = subject.disableTracing(queryRegex, user);
        
        // Verify results
        assertNotNull(result1, "Expected a BadRequestException due to null regex and user values");
        
        assertNotNull(result2, "Expected a non-null response");
        
        assertNotNull(result3, "Expected a non-null response");
    }
    
    @Disabled
    // TODO: Fix Query duplicate method
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testDuplicateQuery_HappyPath() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String newQueryName = "newQueryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 9999);
        int pagesize = 10;
        int pageTimeout = -1;
        Long maxResultsOverride = null;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        String parameters = "invalidparam; valid:param";
        boolean trace = true;
        String userName = "userName";
        String userSid = "userSid";
        String userDN = "userDN";
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);
        List<String> dnList = Collections.singletonList(userDN);
        UUID queryId = UUID.randomUUID();
        
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putSingle(QueryParameters.QUERY_STRING, query);
        queryParameters.putSingle(QueryParameters.QUERY_NAME, newQueryName);
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.putSingle(QueryParameters.QUERY_PARAMS, parameters);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.putSingle(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        
        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);
        
        QueryParameters qp = new QueryParametersImpl();
        qp.validate(queryParameters);
        
        MultivaluedMap<String,String> op = new MultivaluedMapImpl<>();
        op.putAll(qp.getUnknownParameters(queryParameters));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogic1.getClass().getSimpleName());
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDN);
        
        // Set expectations of the create logic
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getUserDN()).thenReturn(userDNpair);
        when(this.principal.getDNs()).thenReturn(new String[] {userDN});
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        queryLogic1.validate(queryParameters);
        when(this.queryLogic1.getAuditType(null)).thenReturn(Auditor.AuditType.NONE);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        when(this.cache.get(queryId.toString())).thenReturn(this.runningQuery);
        when(this.runningQuery.getSettings()).thenReturn(this.query);
        when(this.query.getOwner()).thenReturn(userSid);
        QueryImpl newQuery1 = new QueryImpl();
        newQuery1.setId(UUID.randomUUID());
        newQuery1.setQuery(query);
        newQuery1.setQueryName(newQueryName);
        newQuery1.setBeginDate(beginDate);
        newQuery1.setEndDate(endDate);
        newQuery1.setExpirationDate(expirationDate);
        newQuery1.setDnList(Collections.singletonList(userDN));
        when(this.query.duplicate(newQueryName)).thenReturn(newQuery1);
        when(context.getCallerPrincipal()).thenReturn(principal);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getLogicName()).thenReturn(queryLogicName);
        when(this.queryLogic1.getMaxPageSize()).thenReturn(100);
        QueryImpl newQuery2 = new TestQuery(newQuery1);
        when(persister.create(eq(userDNpair.subjectDN()), eq(dnList), eq(marking), eq(queryLogicName), eq(qp), eq(op))).thenReturn(newQuery2);
        when(this.queryLogic1.getAuditType(newQuery2)).thenReturn(Auditor.AuditType.NONE);
        when(this.queryLogic1.getConnectionPriority()).thenReturn(Priority.NORMAL);
        when(this.queryLogic1.getConnPoolName()).thenReturn("connPool1");
        when(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).thenReturn(null);
        when(this.connectionFactory.getConnection("connPool1", Priority.NORMAL, null)).thenReturn(this.connector);
        when(this.qlCache.add(newQuery1.getId().toString(), userSid, this.queryLogic1, this.connector)).thenReturn(true);
        when(this.queryLogic1.getCollectQueryMetrics()).thenReturn(false);
        when(this.queryLogic1.initialize(eq(this.connector), isA(Query.class), isA(Set.class))).thenReturn(this.genericConfiguration);
        this.queryLogic1.setupQuery(this.genericConfiguration);
        when(this.queryLogic1.getTransformIterator(eq(newQuery2))).thenReturn(this.transformIterator);
        when(this.genericConfiguration.getQueryString()).thenReturn(query);
        this.cache.put(eq(newQuery2.getId().toString()), isA(RunningQuery.class));
        when(this.qlCache.poll(newQuery1.getId().toString())).thenReturn(null);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "traceInfos", traceInfos);
        ReflectionTestUtils.setField(subject, "queryTraceCache", traceCache);
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        GenericResponse<String> result1 = subject.duplicateQuery(queryId.toString(), newQueryName, queryLogicName, query, queryVisibility, beginDate, endDate,
                        queryAuthorizations, expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters, trace);
        
        // Verify results
        assertNotNull(result1, "Expected a non-null response");
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testDuplicateQuery_FindByIDReturnsNull() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 9999);
        int pagesize = 10;
        int pageTimeout = -1;
        Long maxResultsOverride = null;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        String parameters = "invalidparam; valid:param";
        boolean trace = false;
        String userName = "userName";
        String userSid = "userSid";
        UUID queryId = UUID.randomUUID();
        
        // Set expectations of the create logic
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        when(this.cache.get(queryId.toString())).thenReturn(null);
        when(this.persister.findById(queryId.toString())).thenReturn(null);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        
        assertThrows(DatawaveWebApplicationException.class, () -> subject.duplicateQuery(queryId.toString(), queryName, queryLogicName, query, queryVisibility,
                        beginDate, endDate, queryAuthorizations, expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters, trace));
    }
    
    @Disabled
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testDuplicateQuery_UncheckedExceptionThrownDuringCreateQuery() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String newQueryName = "newQueryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 9999);
        int pagesize = 10;
        int pageTimeout = -1;
        Long maxResultsOverride = null;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        String parameters = "invalidparam; valid:param";
        boolean trace = true;
        String userName = "userName";
        String userSid = "userSid";
        String userDN = "userDN";
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);
        UUID queryId = UUID.randomUUID();
        
        // Set expectations of the create logic
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getUserDN()).thenReturn(userDNpair);
        when(this.principal.getDNs()).thenReturn(new String[] {userDN});
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.queryLogic1.getAuditType(null)).thenReturn(Auditor.AuditType.NONE);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        when(this.cache.get(queryId.toString())).thenReturn(this.runningQuery);
        when(this.runningQuery.getSettings()).thenReturn(this.query);
        when(this.query.getOwner()).thenReturn(userSid);
        QueryImpl newQuery1 = new QueryImpl();
        newQuery1.setId(UUID.randomUUID());
        when(this.query.duplicate(newQueryName)).thenReturn(newQuery1);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getLogicName()).thenReturn(queryLogicName);
        when(this.queryLogic1.getMaxPageSize()).thenReturn(100);
        QueryImpl newQuery2 = new TestQuery(newQuery1);
        
        when(this.queryLogic1.getAuditType(newQuery2)).thenReturn(Auditor.AuditType.NONE);
        Exception uncheckedException = new IllegalStateException("INTENTIONALLY THROWN UNCHECKED TEST EXCEPTION");
        when(this.queryLogic1.getConnectionPriority()).thenThrow(uncheckedException);
        this.queryLogic1.close();
        this.persister.remove(newQuery2);
        when(this.qlCache.poll(newQuery1.getId().toString())).thenReturn(null);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "metrics", metrics);
        ReflectionTestUtils.setField(subject, "traceInfos", traceInfos);
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        
        assertThrows(DatawaveWebApplicationException.class, () -> subject.duplicateQuery(queryId.toString(), newQueryName, queryLogicName, query,
                        queryVisibility, beginDate, endDate, queryAuthorizations, expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode,
                        parameters, trace));
    }
    
    @Test
    public void testDuplicateQuery_UncheckedExceptionThrownDuringQueryLookup() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String newQueryName = "newQueryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 9999);
        int pagesize = 10;
        int pageTimeout = -1;
        Long maxResultsOverride = null;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        String parameters = "invalidparam; valid:param";
        boolean trace = true;
        UUID queryId = UUID.randomUUID();
        
        // Set expectations of the create logic
        Exception uncheckedException = new IllegalStateException("INTENTIONALLY THROWN UNCHECKED TEST EXCEPTION", new Exception());
        when(this.context.getCallerPrincipal()).thenThrow(uncheckedException);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        
        assertThrows(DatawaveWebApplicationException.class, () -> subject.duplicateQuery(queryId.toString(), newQueryName, queryLogicName, query,
                        queryVisibility, beginDate, endDate, queryAuthorizations, expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode,
                        parameters, trace));
    }
    
    @Test
    public void testDuplicateQuery_BadRequestException() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String newQueryName = "";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 9999);
        int pagesize = 10;
        int pageTimeout = -1;
        Long maxResultsOverride = null;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        String parameters = "invalidparam; valid:param";
        boolean trace = true;
        UUID queryId = UUID.randomUUID();
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        
        Exception result1 = null;
        try {
            subject.duplicateQuery(queryId.toString(), newQueryName, queryLogicName, query, queryVisibility, beginDate, endDate, queryAuthorizations,
                            expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters, trace);
        } catch (DatawaveWebApplicationException e) {
            result1 = e;
        }
        
        assertNotNull(result1, "Expected a DatawaveWebApplicationException.");
        assertEquals(400, ((DatawaveWebApplicationException) result1).getResponse().getStatus(), "Expected a Bad Request status code.");
    }
    
    @Test
    public void testEnableTracing_NullChecksAndHappyPath() throws Exception {
        // Set local test input
        String user = "user";
        String queryRegex = "queryRegex";
        
        // Set expectations
        when(traceInfos.containsEntry(user, PatternWrapper.wrap(queryRegex))).thenReturn(false);
        when(traceInfos.put(user, PatternWrapper.wrap(queryRegex))).thenReturn(true);
        traceCache.put("traceInfos", traceInfos);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "traceInfos", traceInfos);
        ReflectionTestUtils.setField(subject, "queryTraceCache", traceCache);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        Exception result1 = null;
        try {
            subject.enableTracing(null, null);
        } catch (BadRequestException e) {
            result1 = e;
        }
        VoidResponse result2 = subject.enableTracing(queryRegex, user);
        
        // Verify results
        assertNotNull(result1, "Expected a BadRequestException due to null regex and user values");
        assertNotNull(result2, "Expected a non-null response");
        
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testGet_HappyPath() throws Exception {
        // Set local test input
        String userName = "userName";
        String sid = "sid";
        UUID queryId = UUID.randomUUID();
        
        // Set expectations
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(sid);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList("AUTH_1")));
        when(this.cache.get(queryId.toString())).thenReturn(this.runningQuery);
        when(this.runningQuery.getSettings()).thenReturn(this.query);
        when(this.query.getOwner()).thenReturn(sid);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        QueryImplListResponse result1 = subject.get(queryId.toString());
        
        // Verify results
        assertNotNull(result1, "Expected non-null response");
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testGet_UncheckedException() throws Exception {
        // Set local test input
        String userName = "userName";
        String sid = "sid";
        UUID queryId = UUID.randomUUID();
        
        // Set expectations
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(sid);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList((Collection) Arrays.asList("AUTH_1")));
        when(this.cache.get(queryId.toString())).thenReturn(this.runningQuery);
        when(this.runningQuery.getSettings()).thenReturn(this.query);
        when(this.runningQuery.getSettings()).thenThrow(new IllegalStateException("INTENTIONALLY THROWN UNCHECKED TEST EXCEPTION"));
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        
        assertThrows(DatawaveWebApplicationException.class, () -> subject.get(queryId.toString()));
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testGet_QueryExceptionDueToNonMatchingSids() throws Exception {
        // Set local test input
        String userName = "userName";
        String sid = "sid";
        UUID queryId = UUID.randomUUID();
        
        // Set expectations
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(sid);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList("AUTH_1")));
        when(this.cache.get(queryId.toString())).thenReturn(this.runningQuery);
        when(this.runningQuery.getSettings()).thenReturn(this.query);
        when(this.query.getOwner()).thenReturn("nonmatching_sid");
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        
        assertThrows(DatawaveWebApplicationException.class, () -> subject.get(queryId.toString()));
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testInit() throws Exception {
        // Set expectations
        // when(this.traceCache.putIfAbsent(isA(String.class), (Multimap) notNull())).thenReturn(null);
        this.traceCache.addListener(isA(QueryTraceCache.CacheListener.class));
        when(this.lookupUUIDConfiguration.getUuidTypes()).thenReturn(null);
        when(this.lookupUUIDConfiguration.getBeginDate()).thenReturn("not a date");
        when(this.lookupUUIDConfiguration.getBatchLookupUpperLimit()).thenReturn(0);
        // when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        // LookupUUIDConfiguration tmpCfg = new LookupUUIDConfiguration();
        // tmpCfg.setColumnVisibility("PUBLIC");
        // when(this.lookupUUIDConfiguration.optionalParamsToMap()).andDelegateTo(tmpCfg);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "queryTraceCache", traceCache);
        ReflectionTestUtils.setField(subject, "lookupUUIDConfiguration", lookupUUIDConfiguration);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        
        subject.init();
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testList_HappyPath() throws Exception {
        // Set local test input
        String queryName = "queryName";
        String queryLogicName = "queryLogicName";
        String userName = "userName";
        String sid = "sid";
        UUID queryId = UUID.randomUUID();
        List<String> dnList = Collections.singletonList("userDn");
        HashMap<String,Collection<String>> authsMap = new HashMap<>();
        authsMap.put("USERDN", Arrays.asList("AUTH_1"));
        
        // Set expectations
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(sid);
        when(this.principal.getPrimaryUser()).thenReturn(dwUser);
        when(this.dwUser.getAuths()).thenReturn(Collections.singletonList("AUTH_1"));
        when(this.principal.getProxiedUsers()).thenReturn(Collections.singletonList(dwUser));
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList("AUTH_1")));
        when(this.persister.findByName(queryName)).thenReturn(Arrays.asList((Query) this.query));
        when(this.query.getOwner()).thenReturn(sid);
        when(this.query.getUserDN()).thenReturn(sid);
        when(this.query.getQueryLogicName()).thenReturn(queryLogicName);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getConnectionPriority()).thenReturn(Priority.HIGH);
        when(this.query.getQueryAuthorizations()).thenReturn(null);
        when(this.queryLogic1.getCollectQueryMetrics()).thenReturn(false);
        when(this.query.getId()).thenReturn(queryId);
        when(this.query.getQuery()).thenReturn(queryName);
        when(this.query.getBeginDate()).thenReturn(null);
        when(this.query.getEndDate()).thenReturn(null);
        when(this.query.getColumnVisibility()).thenReturn(null);
        when(this.cache.containsKey(queryId.toString())).thenReturn(false);
        when(this.query.getQueryName()).thenReturn(null);
        when(this.queryLogic1.isLongRunningQuery()).thenReturn(false);
        when(this.queryLogic1.getMaxResults()).thenReturn(-1L);
        this.cache.put(eq(queryId.toString()), isA(RunningQuery.class));
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        QueryImplListResponse result1 = subject.list(queryName);
        
        // Verify results
        assertNotNull(result1, "QueryLogicResponse should not be returned null");
    }
    
    @Test
    public void testListQueriesForUser_HappyPath() throws Exception {
        // Set local test input
        String userSid = "userSid";
        
        // Set expectations
        when(this.persister.findByUser(userSid)).thenReturn(Arrays.asList(this.query));
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        QueryImplListResponse result1 = subject.listQueriesForUser(userSid);
        
        // Verify results
        assertNotNull(result1, "Expected a non-null response");
    }
    
    @Test
    public void testListQueriesForUser_NoResultsException() throws Exception {
        // Set local test input
        String userSid = "userSid";
        
        // Set expectations
        when(this.persister.findByUser(userSid)).thenReturn(null);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        
        assertThrows(NoResultsException.class, () -> subject.listQueriesForUser(userSid));
        
    }
    
    @Test
    public void testListQueriesForUser_UncheckedException() throws Exception {
        // Set local test input
        String userSid = "userSid";
        
        // Set expectations
        when(this.persister.findByUser(userSid)).thenThrow(ILLEGAL_STATE_EXCEPTION);
        
        // Run the test
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        
        assertThrows(DatawaveWebApplicationException.class, () -> subject.listQueriesForUser(userSid));
        
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testListQueryLogic() throws Exception {
        // Set expectations
        RoleManager roleManager = new EasyRoleManager();
        when(this.queryLogicFactory.getQueryLogicList()).thenReturn(Arrays.asList(this.queryLogic1, this.queryLogic2));
        when(this.queryLogic1.getLogicName()).thenReturn("logic1"); // Begin 1st loop
        when(this.queryLogic1.getAuditType(null)).thenReturn(Auditor.AuditType.LOCALONLY);
        when(this.queryLogic1.getLogicDescription()).thenReturn("description1");
        when(this.queryLogic1.getOptionalQueryParameters()).thenReturn(new TreeSet<>());
        when(this.queryLogic1.getRequiredQueryParameters()).thenReturn(new TreeSet<>());
        when(this.queryLogic1.getExampleQueries()).thenReturn(new TreeSet<>());
        when(this.queryLogic1.getRoleManager()).thenReturn(roleManager);
        when(this.queryLogic1.getResponseClass(EasyMock.anyObject(Query.class))).thenThrow(ILLEGAL_STATE_EXCEPTION);
        when(this.queryLogic2.getLogicName()).thenReturn("logic2"); // Begin 1st loop
        when(this.queryLogic2.getAuditType(null)).thenReturn(Auditor.AuditType.LOCALONLY);
        when(this.queryLogic2.getLogicDescription()).thenReturn("description2");
        when(this.queryLogic2.getOptionalQueryParameters()).thenReturn(new TreeSet<>());
        when(this.queryLogic2.getRequiredQueryParameters()).thenReturn(new TreeSet<>());
        when(this.queryLogic2.getExampleQueries()).thenReturn(new TreeSet<>());
        RoleManager roleManager2 = new DatawaveRoleManager(Arrays.asList("ROLE_1", "ROLE_2"));
        when(this.queryLogic2.getRoleManager()).thenReturn(roleManager2);
        when(this.queryLogic2.getResponseClass(EasyMock.anyObject(Query.class))).thenReturn(this.baseResponse.getClass().getCanonicalName());
        Map<String,String> parsers = new HashMap<>();
        parsers.put("PARSER1", null);
        when(this.queryLogic2.getQuerySyntaxParsers()).thenReturn((Map) parsers);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        QueryLogicResponse result1 = subject.listQueryLogic();
        subject.close();
        
        // Verify results
        assertNotNull(result1, "QueryLogicResponse should not be returned null");
    }
    
    @Test
    public void testListUserQueries_HappyPath() throws Exception {
        // Set expectations
        when(this.persister.findByUser()).thenReturn(Arrays.asList(this.query));
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        QueryImplListResponse result1 = subject.listUserQueries();
        subject.close();
        
        // Verify results
        assertNotNull(result1, "Query response should not be returned null");
    }
    
    @Test
    public void testListUserQueries_NoResultsException() throws Exception {
        // Set expectations
        when(this.persister.findByUser()).thenReturn(null);
        
        // Run the test
        
        Exception result1 = null;
        try {
            QueryExecutorBean subject = new QueryExecutorBean();
            ReflectionTestUtils.setField(subject, "persister", persister);
            ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
            subject.listUserQueries();
        } catch (Exception e) {
            result1 = e;
        }
        
        assertNotNull(result1, "Expected a DatawaveWebApplicationException.");
        assertEquals(204, ((DatawaveWebApplicationException) result1).getResponse().getStatus(), "Expected a No Results status code.");
    }
    
    @Test
    public void testListUserQueries_UncheckedException() throws Exception {
        // Set expectations
        Exception uncheckedException = new IllegalStateException("INTENTIONALLY THROWN UNCHECKED TEST EXCEPTION");
        when(this.persister.findByUser()).thenThrow(uncheckedException);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        Exception result1 = null;
        try {
            subject.listUserQueries();
        } catch (DatawaveWebApplicationException e) {
            result1 = e;
        }
        
        // Verify results
        assertNotNull(result1, "Expected an exception to be thrown");
        assertTrue(result1.getCause() instanceof QueryException, "Expected an QueryException to be wrapped by a DatawaveWebApplicationException");
        assertSame(result1.getCause().getCause(), uncheckedException, "Expected an unchecked exception to be wrapped by a QueryException");
    }
    
    @Test
    public void testNext_QueryExceptionDueToCacheLock() throws Exception {
        // Set local test input
        String userName = "userName";
        String userSid = "userSid";
        UUID queryId = UUID.randomUUID();
        
        // Set expectations
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.context.getUserTransaction()).thenReturn(this.transaction);
        this.transaction.begin();
        when(this.cache.get(queryId.toString())).thenReturn(this.runningQuery);
        when(this.cache.lock(queryId.toString())).thenReturn(false);
        when(this.responseObjectFactory.getEventQueryResponse()).thenReturn(new DefaultEventQueryResponse());
        this.runningQuery.setActiveCall(false);
        when(this.runningQuery.getLogic()).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getCollectQueryMetrics()).thenReturn(true);
        when(this.runningQuery.getMetric()).thenReturn(this.queryMetric);
        this.queryMetric.setError(isA(Throwable.class));
        this.metrics.updateMetric(this.queryMetric);
        cache.unlock(queryId.toString());
        this.transaction.setRollbackOnly();
        when(this.transaction.getStatus()).thenThrow(ILLEGAL_STATE_EXCEPTION);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "metrics", metrics);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        Exception result1 = null;
        try {
            subject.next(queryId.toString());
        } catch (DatawaveWebApplicationException e) {
            result1 = e;
        }
        
        // Verify results
        assertNotNull(result1, "Expected a DatawaveWebApplicationException to be thrown");
        assertEquals("500-9", ((QueryException) result1.getCause().getCause()).getErrorCode(),
                        "Expected DatawaveWebApplicationException to have been caused by a locked cache entry");
    }
    
    @Test
    public void testNext_UncheckedException() throws Exception {
        // Set local test input
        String userName = "userName";
        String userSid = "userSid";
        UUID queryId = UUID.randomUUID();
        
        // Set expectations
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.context.getUserTransaction()).thenReturn(this.transaction);
        this.transaction.begin();
        when(this.cache.get(queryId.toString())).thenReturn(this.runningQuery);
        when(this.cache.lock(queryId.toString())).thenThrow(new IllegalStateException("INTENTIONALLY THROWN UNCHECKED TEST EXCEPTION"));
        when(this.responseObjectFactory.getEventQueryResponse()).thenReturn(new DefaultEventQueryResponse());
        this.runningQuery.setActiveCall(false);
        when(this.runningQuery.getLogic()).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getCollectQueryMetrics()).thenReturn(true);
        when(this.runningQuery.getMetric()).thenReturn(this.queryMetric);
        this.queryMetric.setError(isA(Throwable.class));
        cache.unlock(queryId.toString());
        this.transaction.setRollbackOnly();
        when(this.transaction.getStatus()).thenReturn(Status.STATUS_PREPARING);
        doThrow(new HeuristicMixedException("INTENTIONALLY THROWN TEST EXCEPTION")).when(this.transaction).commit();
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        
        assertThrows(DatawaveWebApplicationException.class, () -> subject.next(queryId.toString()));
    }
    
    @Test
    public void testNext_UserNotOwner() throws Exception {
        // Set local test input
        String userName = "userName";
        String userSid = "userSid";
        String otherSid = "otherSid";
        UUID queryId = UUID.randomUUID();
        
        // Set expectations
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(otherSid);
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.context.getUserTransaction()).thenReturn(this.transaction);
        this.transaction.begin();
        when(this.cache.get(queryId.toString())).thenReturn(this.runningQuery);
        when(this.cache.lock(queryId.toString())).thenReturn(true);
        when(this.responseObjectFactory.getEventQueryResponse()).thenReturn(new DefaultEventQueryResponse());
        when(this.runningQuery.getConnection()).thenReturn(this.connector);
        when(this.runningQuery.getSettings()).thenReturn(this.query);
        when(this.query.getOwner()).thenReturn(userSid);
        when(this.runningQuery.getSettings()).thenReturn(this.query);
        when(this.query.getOwner()).thenReturn(userSid);
        cache.unlock(queryId.toString());
        when(this.transaction.getStatus()).thenReturn(Status.STATUS_PREPARING);
        this.transaction.setRollbackOnly();
        this.transaction.commit();
        this.runningQuery.setActiveCall(false);
        when(this.runningQuery.getLogic()).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getCollectQueryMetrics()).thenReturn(true);
        when(this.runningQuery.getMetric()).thenReturn(this.queryMetric);
        this.queryMetric.setError(isA(Throwable.class));
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        Exception result1 = null;
        try {
            subject.next(queryId.toString());
        } catch (DatawaveWebApplicationException e) {
            result1 = e;
            assertTrue(e.getCause() instanceof QueryException);
            assertEquals("401-1", ((QueryException) e.getCause().getCause()).getErrorCode());
        }
        
        // Verify results
        assertNotNull(result1, "Expected a DatawaveWebApplicationException to be thrown due an unchecked exception");
    }
    
    @Test
    public void testNext_NullQueryReturnedFromCache() throws Exception {
        // Set local test input
        String userName = "userName";
        String userSid = "userSid";
        UUID queryId = UUID.randomUUID();
        
        // Set expectations
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.context.getUserTransaction()).thenReturn(this.transaction);
        this.transaction.begin();
        when(this.cache.get(queryId.toString())).thenReturn(null);
        when(this.cache.lock(queryId.toString())).thenReturn(true);
        when(this.responseObjectFactory.getEventQueryResponse()).thenReturn(new DefaultEventQueryResponse());
        when(this.persister.findById(queryId.toString())).thenReturn(new ArrayList<>(0));
        cache.unlock(queryId.toString());
        doThrow(ILLEGAL_STATE_EXCEPTION).when(this.transaction).setRollbackOnly();
        when(this.transaction.getStatus()).thenReturn(Status.STATUS_UNKNOWN);
        doThrow(new HeuristicRollbackException("INTENTIONALLY THROWN TEST EXCEPTION")).when(this.transaction).commit();
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "metrics", metrics);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        
        Exception result1 = null;
        try {
            subject.next(queryId.toString());
        } catch (DatawaveWebApplicationException e) {
            result1 = e;
        }
        
        assertNotNull(result1, "Expected a DatawaveWebApplicationException.");
        assertEquals(404, ((DatawaveWebApplicationException) result1).getResponse().getStatus(), "Expected a Not Found status code.");
    }
    
    @Test
    public void testPurgeQueryCache_UncheckedException() throws Exception {
        // Set expectations
        doThrow(new IllegalStateException("INTENTIONALLY THROWN UNCHECKED TEST EXCEPTION")).when(this.cache).clear();
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        
        assertThrows(DatawaveWebApplicationException.class, () -> subject.purgeQueryCache());
    }
    
    @Test
    public void testPurgeQueryCacheAndMiscAccessors_HappyPath() throws Exception {
        // Set expectations
        this.cache.clear();
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        VoidResponse result1 = subject.purgeQueryCache();
        QueryMetricsBean result5 = subject.getMetrics();
        QueryLogicFactory result6 = subject.getQueryFactory();
        Persister result7 = subject.getPersister();
        QueryCache result8 = subject.getQueryCache();
        
        // Verify results
        assertNotNull(result1, "Expected a non-null response");
        assertNull(result5, "Expected a null metrics instance");
        assertNull(result6, "Expected a null query logic factory");
        assertNull(result7, "Expected a null persister");
        assertNotNull(result8, "Expected a NON-null cache");
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testReset_NoPreexistingRunningQuery() throws Exception {
        // Set local test input
        String authorization = "AUTH_1";
        String queryName = "queryName";
        String queryLogicName = "queryLogicName";
        String userName = "userName";
        String userDN = "userDN";
        String sid = "sid";
        UUID queryId = UUID.randomUUID();
        
        HashMap<String,Collection<String>> authsMap = new HashMap<>();
        authsMap.put("userdn", Arrays.asList(authorization));
        
        QueryImpl qp = new QueryImpl();
        qp.setQuery("someQuery");
        qp.setQueryAuthorizations(authorization);
        qp.setBeginDate(new Date());
        qp.setEndDate(new Date());
        qp.setExpirationDate(new Date());
        qp.setQueryName(queryName);
        qp.setUserDN(userDN);
        List<String> dnList = Collections.singletonList(userDN);
        qp.setDnList(dnList);
        
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>(qp.toMap());
        map.set(PrivateAuditConstants.AUDIT_TYPE, Auditor.AuditType.PASSIVE.name());
        map.set(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        map.set(PrivateAuditConstants.COLUMN_VISIBILITY, authorization);
        map.set(PrivateAuditConstants.USER_DN, userDN);
        map.set(AuditParameters.AUDIT_ID, queryName);
        MultivaluedMap auditMap = new MultivaluedMapImpl();
        auditMap.putAll(map);
        
        // Set expectations
        when(this.context.getUserTransaction()).thenReturn(this.transaction);
        this.transaction.begin();
        when(this.transaction.getStatus()).thenReturn(Status.STATUS_ACTIVE);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(sid);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(authorization)));
        when(this.principal.getPrimaryUser()).thenReturn(dwUser);
        when(this.dwUser.getAuths()).thenReturn(Collections.singleton(authorization));
        when(this.principal.getProxiedUsers()).thenReturn(Collections.singletonList(dwUser));
        when(this.cache.get(queryName)).thenReturn(null);
        when(this.persister.findById(queryName)).thenReturn(Arrays.asList((Query) this.query));
        when(this.query.getQueryLogicName()).thenReturn(queryLogicName);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getConnectionPriority()).thenReturn(Priority.NORMAL);
        when(this.query.getQueryAuthorizations()).thenReturn(authorization);
        when(this.queryLogic1.getCollectQueryMetrics()).thenReturn(false);
        when(this.query.getUncaughtExceptionHandler()).thenReturn(exceptionHandler);
        when(this.exceptionHandler.getThrowable()).thenReturn(null);
        when(this.query.getOwner()).thenReturn(sid);
        when(this.query.getId()).thenReturn(queryId);
        when(this.query.getQuery()).thenReturn(queryName);
        this.cache.put(eq(queryId.toString()), isA(RunningQuery.class));
        when(this.cache.lock(queryName)).thenReturn(true);
        when(this.queryLogic1.getAuditType(this.query)).thenReturn(Auditor.AuditType.PASSIVE);
        when(this.query.getUserDN()).thenReturn(userDN);
        when(this.query.getDnList()).thenReturn(dnList);
        when(this.queryLogic1.isLongRunningQuery()).thenReturn(false);
        when(this.queryLogic1.getResultLimit(dnList)).thenReturn(-1L);
        when(this.queryLogic1.getMaxResults()).thenReturn(-1L);
        when(this.query.toMap()).thenReturn(map);
        when(this.query.getColumnVisibility()).thenReturn(authorization);
        when(this.query.getBeginDate()).thenReturn(null);
        when(this.query.getEndDate()).thenReturn(null);
        when(this.query.getQueryName()).thenReturn(queryName);
        when(this.query.getParameters()).thenReturn((Set) Collections.emptySet());
        when(this.query.getColumnVisibility()).thenReturn(authorization);
        when(this.queryLogic1.getSelectors(this.query)).thenReturn(null);
        when(this.auditor.audit(auditMap)).thenReturn(null);
        //
        // Advice from a test-driven development perspective...
        //
        // In order to reduce the quantity and complexity of the multitude of previous steps, try
        // consolidating them into one or more helper classes. Such helpers could be injected via Whitebox
        // and invoked via many fewer steps and simpler input.
        //
        // For example, the auditing steps could be combined into a single "audit(RunningQuery);" method
        // of a "RunningQueryAuditor" helper class. Likewise, the multiple steps involved in the direct
        // construction of a RunningQuery instance could be simplified into a "newRunningQuery(..);" method
        // of a "RunningQueryFactory" helper class. Both helpers (or even a single helper designed to
        // consolidate both RunningQuery-related chunks of logic) could be injected using Whitebox as
        // PowerMock instances into new QueryExecutorBean instance variables, as demonstrated below with
        // previously existing instance variables.
        //
        // After creating unit tests for the helpers to ensure sufficient testing coverage, you'd end up
        // with smaller, more compartmentalized bundles of logic, including QueryExecutorBean, which
        // currently has over 1,600 lines of code.
        //
        //
        when(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).thenReturn(new HashMap<>());
        when(this.queryLogic1.getConnPoolName()).thenReturn("connPool1");
        when(this.queryLogic1.getLogicName()).thenReturn(queryLogicName);
        connectionRequestBean.requestBegin(queryName);
        when(this.connectionFactory.getConnection(eq("connPool1"), eq(Priority.NORMAL), isA(Map.class))).thenReturn(this.connector);
        connectionRequestBean.requestEnd(queryName);
        when(this.queryLogic1.initialize(eq(this.connector), eq(this.query), isA(Set.class))).thenReturn(this.genericConfiguration);
        this.queryLogic1.setupQuery(this.genericConfiguration);
        when(this.queryLogic1.getTransformIterator(this.query)).thenReturn(this.transformIterator);
        when(this.genericConfiguration.getQueryString()).thenReturn(queryName);
        this.connectionFactory.returnConnection(null); // These 2 lines prevent the bean's exception-handling logic (in combination
        cache.unlock(queryName);
        this.transaction.commit();
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "auditor", auditor);
        ReflectionTestUtils.setField(subject, "metrics", metrics);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        VoidResponse result1 = subject.reset(queryName);
        
        // Verify results
        assertNotNull(result1, "VoidResponse should not be returned null");
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testReset_PreexistingRunningQueryWithCloseConnectionException() throws Exception {
        // Set local test input
        String authorization = "AUTH_1";
        String queryName = "queryName";
        String userName = "userName";
        String userSid = "sid";
        
        // Set expectations
        when(this.context.getUserTransaction()).thenReturn(this.transaction);
        this.transaction.begin();
        when(this.transaction.getStatus()).thenReturn(Status.STATUS_ACTIVE);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(authorization)));
        when(this.cache.get(queryName)).thenReturn(this.runningQuery);
        when(this.runningQuery.getSettings()).thenReturn(this.query);
        when(this.query.getOwner()).thenReturn(userSid);
        when(this.runningQuery.getTraceInfo()).thenReturn(this.traceInfo);
        when(this.cache.lock(queryName)).thenReturn(true);
        when(this.runningQuery.getConnection()).thenReturn(this.connector);
        doThrow(new IOException("INTENTIONALLY THROWN 1ST-LEVEL TEST EXCEPTION")).when(this.runningQuery).closeConnection(this.connectionFactory);
        cache.unlock(queryName);
        doThrow(new IllegalStateException("INTENTIONALLY THROWN 3RD-LEVEL TEST EXCEPTION")).when(this.transaction).commit();
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "auditor", auditor);
        ReflectionTestUtils.setField(subject, "metrics", metrics);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        Throwable result1 = null;
        try {
            subject.reset(queryName);
        } catch (DatawaveWebApplicationException e) {
            result1 = e.getCause();
        }
        
        // Verify results
        assertTrue(result1 instanceof QueryException, "Query exception expected to have been thrown due to locking problem");
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testReset_PreexistingRunningQueryWithLockException() throws Exception {
        // Set local test input
        String authorization = "AUTH_1";
        String queryName = "queryName";
        String userName = "userName";
        String userSid = "sid";
        
        // Set expectations
        when(this.context.getUserTransaction()).thenReturn(this.transaction);
        this.transaction.begin();
        when(this.transaction.getStatus()).thenReturn(Status.STATUS_ACTIVE);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(authorization)));
        when(this.cache.get(queryName)).thenReturn(this.runningQuery);
        when(this.runningQuery.getSettings()).thenReturn(this.query);
        when(this.query.getOwner()).thenReturn(userSid);
        when(this.runningQuery.getTraceInfo()).thenReturn(this.traceInfo);
        when(this.cache.lock(queryName)).thenReturn(false);
        when(this.transaction.getStatus()).thenReturn(Status.STATUS_NO_TRANSACTION);
        this.connectionFactory.returnConnection(null); // These 2 lines prevent the bean's exception-handling logic (in combination
        cache.unlock(queryName);
        this.transaction.commit();
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "auditor", auditor);
        ReflectionTestUtils.setField(subject, "metrics", metrics);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        Throwable result1 = null;
        try {
            subject.reset(queryName);
        } catch (DatawaveWebApplicationException e) {
            result1 = e.getCause();
        }
        
        // Verify results
        assertTrue(result1 instanceof QueryException, "Query exception expected to have been thrown");
        assertEquals("500-9", ((QueryException) result1.getCause()).getErrorCode(), "Thrown exception expected to have been due to locking problem");
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testUpdateQuery_PersistentMode() throws Exception {
        // Set local test input
        UUID queryId = UUID.randomUUID();
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 9999);
        int pagesize = 10;
        int pageTimeout = -1;
        Long maxResultsOverride = null;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        String parameters = "invalidparam; valid:param";
        String userName = "userName";
        String userSid = "userSid";
        String userDN = "userDN";
        Query duplicateQuery = mock(Query.class);
        
        MultiValueMap<String,String> p = new LinkedMultiValueMap<>();
        p.set(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        p.set(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        p.set(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        p.set(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expirationDate));
        p.set(QueryParameters.QUERY_NAME, queryName);
        p.set(QueryParameters.QUERY_PAGESIZE, Integer.toString(pagesize));
        p.set(QueryParameters.QUERY_PAGETIMEOUT, Integer.toString(pageTimeout));
        p.set(QueryParameters.QUERY_STRING, query);
        p.set(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        p.set(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        p.set(QueryParameters.QUERY_PARAMS, parameters);
        
        p.set(PrivateAuditConstants.AUDIT_TYPE, Auditor.AuditType.LOCALONLY.name());
        p.set(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        p.set(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        p.set(PrivateAuditConstants.USER_DN, userDN);
        MultivaluedMap auditMap = new MultivaluedMapImpl();
        auditMap.putAll(p);
        auditMap.putSingle(AuditParameters.AUDIT_ID, queryId.toString());
        
        // Set expectations
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        when(this.cache.get(queryId.toString())).thenReturn(this.runningQuery);
        when(this.runningQuery.getSettings()).thenReturn(this.query);
        when(this.query.getOwner()).thenReturn(userSid);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getLogicName()).thenReturn(queryLogicName);
        when(this.query.duplicate(queryName)).thenReturn(duplicateQuery);
        duplicateQuery.setId(queryId);
        duplicateQuery.setQueryLogicName(queryLogicName);
        duplicateQuery.setQuery(query);
        duplicateQuery.setBeginDate(beginDate);
        duplicateQuery.setEndDate(endDate);
        duplicateQuery.setQueryAuthorizations(queryAuthorizations);
        duplicateQuery.setExpirationDate(expirationDate);
        duplicateQuery.setPagesize(pagesize);
        duplicateQuery.setPageTimeout(pageTimeout);
        duplicateQuery.setParameters(isA(Set.class));
        when(duplicateQuery.toMap()).thenReturn(p);
        when(this.auditor.audit(eq(auditMap))).thenReturn(null);
        this.query.setQueryLogicName(queryLogicName);
        this.query.setQuery(query);
        this.query.setBeginDate(beginDate);
        this.query.setEndDate(endDate);
        this.query.setQueryAuthorizations(queryAuthorizations);
        this.query.setExpirationDate(expirationDate);
        this.query.setPagesize(pagesize);
        this.query.setPageTimeout(pageTimeout);
        this.query.setParameters(isA(Set.class));
        when(this.query.getQueryName()).thenReturn(queryName);
        this.persister.update(this.query);
        when(this.query.getId()).thenReturn(queryId);
        this.cache.put(queryId.toString(), this.runningQuery);
        when(this.runningQuery.getLogic()).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getAuditType(this.query)).thenReturn(Auditor.AuditType.LOCALONLY);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "auditor", auditor);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        GenericResponse<String> result1 = subject.updateQuery(queryId.toString(), queryLogicName, query, queryVisibility, beginDate, endDate,
                        queryAuthorizations, expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters);
        
        // Verify results
        assertNotNull(result1, "Expected a non-null response performing an admin close");
    }
    
    @Disabled
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testExecute_HappyPath() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 9999);
        int pagesize = 10;
        int pageTimeout = -1;
        Long maxResultsOverride = null;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        String parameters = "invalidparam; valid:param";
        boolean trace = false;
        UUID queryId = UUID.randomUUID();
        List<MediaType> mediaTypes = new ArrayList<>();
        mediaTypes.add(MediaType.APPLICATION_XML_TYPE);
        GenericResponse<String> createResponse = new GenericResponse<>();
        createResponse.setResult(queryId.toString());
        
        QueryImpl qp = new QueryImpl();
        qp.setQuery(query);
        qp.setQueryName(queryName);
        qp.setDnList(Collections.singletonList("someDN"));
        qp.setQueryAuthorizations(queryAuthorizations);
        qp.setColumnVisibility(queryVisibility);
        qp.setBeginDate(beginDate);
        qp.setEndDate(endDate);
        qp.setExpirationDate(expirationDate);
        qp.setPagesize(pagesize);
        qp.setPageTimeout(pageTimeout);
        qp.setColumnVisibility(queryAuthorizations);
        
        MultivaluedMap<String,String> params = new MultivaluedMapImpl<>();
        params.putAll(qp.toMap());
        params.putSingle(QueryParameters.QUERY_TRACE, Boolean.toString(trace));
        params.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        params.putSingle(QueryParameters.QUERY_PARAMS, parameters);
        
        QueryExecutorBean subject = spy(QueryExecutorBean.class);
        
        // Set expectations of the create logic
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.httpHeaders.getAcceptableMediaTypes()).thenReturn(mediaTypes);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getEnrichedTransformer(isA(Query.class))).thenReturn(this.transformer);
        when(this.transformer.createResponse(isA(ResultsPage.class))).thenReturn(this.baseResponse);
        when(subject.createQuery(queryLogicName, params, httpHeaders)).thenReturn(createResponse);
        when(this.cache.get(eq(queryId.toString()))).thenReturn(this.runningQuery);
        when(this.runningQuery.getMetric()).thenReturn(this.queryMetric);
        this.queryMetric.setCreateCallTime(EasyMock.geq(0L));
        // return streaming response
        
        // Run the test
        
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "auditor", auditor);
        ReflectionTestUtils.setField(subject, "metrics", metrics);
        ReflectionTestUtils.setField(subject, "traceInfos", traceInfos);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        StreamingOutput result1 = subject.execute(queryLogicName, params, httpHeaders);
        
        // Verify results
        assertNotNull(result1, "Expected a non-null response");
    }
    
    @Test
    public void testExecute_InvalidMediaType() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 9999);
        int pagesize = 10;
        int pageTimeout = -1;
        Long maxResultsOverride = null;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        String parameters = "invalidparam; valid:param";
        boolean trace = false;
        String systemFrom = "systemFrom";
        String dataSetType = "dataSetType";
        String purpose = "purpose";
        String parentAuditId = "parentAuditId";
        UUID queryId = UUID.randomUUID();
        List<MediaType> mediaTypes = new ArrayList<>();
        mediaTypes.add(MediaType.APPLICATION_OCTET_STREAM_TYPE);
        GenericResponse<String> createResponse = new GenericResponse<>();
        createResponse.setResult(queryId.toString());
        
        QueryExecutorBean subject = spy(QueryExecutorBean.class);
        
        // Set expectations of the create logic
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.httpHeaders.getAcceptableMediaTypes()).thenReturn(mediaTypes);
        
        // Run the test
        
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "auditor", auditor);
        ReflectionTestUtils.setField(subject, "metrics", metrics);
        ReflectionTestUtils.setField(subject, "traceInfos", traceInfos);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putAll(QueryParametersImpl.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                        expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters, trace));
        
        assertThrows(DatawaveWebApplicationException.class, () -> {
            StreamingOutput result1 = subject.execute(queryLogicName, queryParameters, httpHeaders);
            // Verify results
                        assertNull(result1, "Expected a non-null response");
                    });
    }
    
    @Test
    public void testLookupUUID_happyPath() {
        UUIDType uuidType = mock(UUIDType.class);
        BaseQueryResponse response = mock(BaseQueryResponse.class);
        ManagedExecutorService executor = mock(ManagedExecutorService.class);
        
        when(uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap<>());
        when(lookupUUIDUtil.getUUIDType("uuidType")).thenReturn(uuidType);
        when(uuidType.getDefinedView()).thenReturn("abc");
        when(lookupUUIDUtil.createUUIDQueryAndNext(isA(GetUUIDCriteria.class))).thenReturn(response);
        when(response.getQueryId()).thenReturn("11111");
        when(context.getCallerPrincipal()).thenReturn(principal);
        when(executor.submit(isA(Runnable.class))).thenReturn(null);
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "auditor", auditor);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(subject, "lookupUUIDUtil", lookupUUIDUtil);
        ReflectionTestUtils.setField(subject, "executor", executor);
        
        subject.lookupUUID("uuidType", "1234567890", uriInfo, httpHeaders);
        
    }
    
    @Test
    public void testLookupUUID_closeFail() {
        QueryExecutorBean subject = mock(QueryExecutorBean.class);
        ManagedExecutorService executor = mock(ManagedExecutorService.class);
        
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "auditor", auditor);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(subject, "lookupUUIDUtil", lookupUUIDUtil);
        ReflectionTestUtils.setField(subject, "executor", executor);
        ReflectionTestUtils.setField(subject, "log", Logger.getLogger(QueryExecutorBean.class));
        
        subject.lookupUUID("uuidType", "1234567890", uriInfo, httpHeaders);
        
    }
    
    @Disabled
    @Test
    public void testPlanQuery() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 999999);
        int pagesize = 10;
        int pageTimeout = -1;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        boolean trace = false;
        String userName = "userName";
        String userSid = "userSid";
        String userDN = "userdn";
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);
        List<String> dnList = Collections.singletonList(userDN);
        UUID queryId = UUID.randomUUID();
        
        HashMap<String,Collection<String>> authsMap = new HashMap<>();
        authsMap.put("userdn", Arrays.asList(queryAuthorizations));
        
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putSingle(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        queryParameters.putSingle(QueryParameters.QUERY_STRING, query);
        queryParameters.putSingle(QueryParameters.QUERY_NAME, queryName);
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.putSingle(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        queryParameters.putSingle("valid", "param");
        
        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);
        
        QueryParameters qp = new QueryParametersImpl();
        qp.validate(queryParameters);
        
        MultivaluedMap<String,String> op = new MultivaluedMapImpl<>();
        op.putAll(qp.getUnknownParameters(queryParameters));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());
        
        // Set expectations of the create logic
        queryLogic1.validate(queryParameters);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getMaxPageSize()).thenReturn(1000);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getUserDN()).thenReturn(userDNpair);
        when(this.principal.getDNs()).thenReturn(new String[] {userDN});
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).thenReturn(true);
        when(this.queryLogic1.getAuditType(null)).thenReturn(Auditor.AuditType.PASSIVE);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        when(persister.create(eq(userDNpair.subjectDN()), eq(dnList), eq(marking), eq(queryLogicName), eq(qp), eq(op))).thenReturn(this.query);
        when(this.queryLogic1.getAuditType(this.query)).thenReturn(Auditor.AuditType.PASSIVE);
        when(this.queryLogic1.getConnectionPriority()).thenReturn(Priority.NORMAL);
        when(this.queryLogic1.getConnPoolName()).thenReturn("connPool1");
        when(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).thenReturn(null);
        this.connectionRequestBean.requestBegin(queryId.toString());
        when(this.connectionFactory.getConnection("connPool1", Priority.NORMAL, null)).thenReturn(this.connector);
        this.connectionRequestBean.requestEnd(queryId.toString());
        when(this.principal.getPrimaryUser()).thenReturn(dwUser);
        when(this.dwUser.getAuths()).thenReturn(Collections.singleton(queryAuthorizations));
        when(this.principal.getProxiedUsers()).thenReturn(Collections.singletonList(dwUser));
        // when(this.query.getOwner()).thenReturn(userSid);
        // when(this.query.getId()).thenReturn(queryId);
        // when(this.query.getQuery()).thenReturn(queryName);
        // when(this.query.getQueryLogicName()).thenReturn(queryLogicName);
        // when(this.query.getBeginDate()).thenReturn(null);
        // when(this.query.getEndDate()).thenReturn(null);
        // when(this.query.getColumnVisibility()).thenReturn(null);
        // when(this.query.getQueryAuthorizations()).thenReturn(queryAuthorizations);
        // when(this.query.getQueryName()).thenReturn(null);
        // when(this.query.getPagesize()).thenReturn(0);
        // when(this.query.getExpirationDate()).thenReturn(null);
        // when(this.query.getParameters()).thenReturn((Set) Collections.emptySet());
        // when(this.query.getUncaughtExceptionHandler()).thenReturn(new QueryUncaughtExceptionHandler());
        // when(this.query.getUserDN()).thenReturn(userDN);
        
        // Set expectations of the plan
        Authorizations queryAuths = new Authorizations(queryAuthorizations);
        when(this.queryLogic1.getPlan(this.connector, this.query, Collections.singleton(queryAuths), true, false)).thenReturn("a query plan");
        
        // Set expectations of the cleanup
        this.connectionFactory.returnConnection(this.connector);
        queryLogic1.close();
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "auditor", auditor);
        ReflectionTestUtils.setField(subject, "metrics", metrics);
        ReflectionTestUtils.setField(subject, "traceInfos", traceInfos);
        ReflectionTestUtils.setField(subject, "marking", new ColumnVisibilitySecurityMarking());
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        GenericResponse<String> result1 = subject.planQuery(queryLogicName, queryParameters);
        
        // Verify results
        assertNotNull(result1, "Expected a non-null response");
        assertEquals("a query plan", result1.getResult());
    }
    
    @Test
    public void testPlanQueryWithValues() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 999999);
        int pagesize = 10;
        int pageTimeout = -1;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        boolean trace = false;
        String userName = "userName";
        String userSid = "userSid";
        String userDN = "userdn";
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);
        List<String> dnList = Collections.singletonList(userDN);
        UUID queryId = UUID.randomUUID();
        
        HashMap<String,Collection<String>> authsMap = new HashMap<>();
        authsMap.put("userdn", Arrays.asList(queryAuthorizations));
        
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putSingle(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        queryParameters.putSingle(QueryParameters.QUERY_STRING, query);
        queryParameters.putSingle(QueryParameters.QUERY_NAME, queryName);
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.putSingle(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        queryParameters.putSingle("valid", "param");
        queryParameters.putSingle(QueryExecutorBean.EXPAND_VALUES, "true");
        
        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);
        
        QueryParameters qp = new QueryParametersImpl();
        qp.validate(queryParameters);
        
        MultivaluedMap<String,String> op = new MultivaluedMapImpl<>();
        op.putAll(qp.getUnknownParameters(queryParameters));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());
        
        // Set expectations of the create logic
        queryLogic1.validate(queryParameters);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getMaxPageSize()).thenReturn(1000);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getUserDN()).thenReturn(userDNpair);
        when(this.principal.getDNs()).thenReturn(new String[] {userDN});
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).thenReturn(true);
        when(this.queryLogic1.getAuditType(null)).thenReturn(Auditor.AuditType.PASSIVE);
        when(this.queryLogic1.getSelectors(this.query)).thenReturn(null);
        when(auditor.audit(eq(queryParameters))).thenReturn(null);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        when(persister.create(eq(userDNpair.subjectDN()), eq(dnList), eq(marking), eq(queryLogicName), eq(qp), eq(op))).thenReturn(this.query);
        when(this.queryLogic1.getAuditType(this.query)).thenReturn(Auditor.AuditType.PASSIVE);
        when(this.queryLogic1.getConnectionPriority()).thenReturn(Priority.NORMAL);
        when(this.queryLogic1.getConnPoolName()).thenReturn("connPool1");
        when(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).thenReturn(null);
        this.connectionRequestBean.requestBegin(queryId.toString());
        when(this.connectionFactory.getConnection("connPool1", Priority.NORMAL, null)).thenReturn(this.connector);
        this.connectionRequestBean.requestEnd(queryId.toString());
        when(this.principal.getPrimaryUser()).thenReturn(dwUser);
        when(this.dwUser.getAuths()).thenReturn(Collections.singleton(queryAuthorizations));
        when(this.principal.getProxiedUsers()).thenReturn(Collections.singletonList(dwUser));
        when(this.query.getId()).thenReturn(queryId);
        
        // Set expectations of the plan
        Authorizations queryAuths = new Authorizations(queryAuthorizations);
        when(this.queryLogic1.getPlan(this.connector, this.query, Collections.singleton(queryAuths), true, true)).thenReturn("a query plan");
        
        // Set expectations of the cleanup
        this.connectionFactory.returnConnection(this.connector);
        queryLogic1.close();
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "auditor", auditor);
        ReflectionTestUtils.setField(subject, "metrics", metrics);
        ReflectionTestUtils.setField(subject, "traceInfos", traceInfos);
        ReflectionTestUtils.setField(subject, "marking", new ColumnVisibilitySecurityMarking());
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        GenericResponse<String> result1 = subject.planQuery(queryLogicName, queryParameters);
        
        // Verify results
        assertNotNull(result1, "Expected a non-null response");
        assertEquals("a query plan", result1.getResult());
    }
    
    @Test
    public void testCreateQuery_auditException() throws Exception {
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 999999);
        int pagesize = 10;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        boolean trace = false;
        String userName = "userName";
        String userSid = "userSid";
        String userDN = "userDN";
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);
        List<String> dnList = Collections.singletonList(userDN);
        UUID queryId = UUID.randomUUID();
        long pageNumber = 0L;
        
        HashMap<String,Collection<String>> authsMap = new HashMap<>();
        authsMap.put("USERDN", Arrays.asList(queryAuthorizations));
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putSingle(QueryParameters.QUERY_STRING, query);
        queryParameters.putSingle(QueryParameters.QUERY_NAME, queryName);
        queryParameters.putSingle(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        
        when(context.getCallerPrincipal()).thenReturn(principal);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).thenReturn((QueryLogic) this.queryLogic1);
        queryLogic1.validate(queryParameters);
        when(principal.getName()).thenReturn(userName);
        when(principal.getShortName()).thenReturn(userSid);
        when(principal.getUserDN()).thenReturn(userDNpair);
        when(principal.getDNs()).thenReturn(new String[] {userDN});
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).thenReturn(true);
        when(this.queryLogic1.getAuditType(null)).thenReturn(Auditor.AuditType.ACTIVE);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        when(this.queryLogic1.getMaxPageSize()).thenReturn(10);
        when(queryLogic1.getSelectors(null)).thenReturn(null);
        
        QueryExecutorBean executor = new QueryExecutorBean();
        ReflectionTestUtils.setField(executor, "ctx", context);
        ReflectionTestUtils.setField(executor, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(executor, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(executor, "qlCache", qlCache);
        ReflectionTestUtils.setField(executor, "queryCache", cache);
        ReflectionTestUtils.setField(executor, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(executor, "persister", persister);
        ReflectionTestUtils.setField(executor, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(executor, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(executor, "auditor", auditor);
        ReflectionTestUtils.setField(executor, "metrics", metrics);
        ReflectionTestUtils.setField(executor, "traceInfos", traceInfos);
        ReflectionTestUtils.setField(executor, "marking", new ColumnVisibilitySecurityMarking());
        ReflectionTestUtils.setField(executor, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(executor, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(executor, "accumuloConnectionRequestBean", connectionRequestBean);
        
        assertThrows(DatawaveWebApplicationException.class, () -> executor.createQuery(queryLogicName, queryParameters));
        
    }
    
    @Test
    public void testReset_auditException() throws Exception {
        // Set local test input
        String authorization = "AUTH_1";
        String queryName = "queryName";
        String queryLogicName = "queryLogicName";
        String userName = "userName";
        String userDN = "userDN";
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);
        String sid = "sid";
        UUID queryId = UUID.randomUUID();
        
        HashMap<String,Collection<String>> authsMap = new HashMap<>();
        authsMap.put("USERDN", Arrays.asList(authorization));
        
        QueryImpl qp = new QueryImpl();
        qp.setQuery("someQuery");
        qp.setQueryAuthorizations(authorization);
        qp.setBeginDate(new Date());
        qp.setEndDate(new Date());
        qp.setExpirationDate(new Date());
        qp.setQueryName(queryName);
        qp.setUserDN(userDN);
        qp.setDnList(Collections.singletonList(userDN));
        
        MultiValueMap<String,String> map = new LinkedMultiValueMap<>();
        map.set(PrivateAuditConstants.AUDIT_TYPE, Auditor.AuditType.PASSIVE.name());
        map.set(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        map.set(PrivateAuditConstants.COLUMN_VISIBILITY, authorization);
        map.set(PrivateAuditConstants.USER_DN, userDN);
        map.set(AuditParameters.AUDIT_ID, queryName);
        MultivaluedMap auditMap = new MultivaluedMapImpl();
        auditMap.putAll(map);
        
        // Set expectations
        when(this.context.getUserTransaction()).thenReturn(this.transaction);
        this.transaction.begin();
        when(this.transaction.getStatus()).thenReturn(Status.STATUS_ACTIVE);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(sid);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(authorization)));
        when(this.cache.get(queryName)).thenReturn(null);
        when(this.persister.findById(queryName)).thenReturn(Arrays.asList((Query) this.query));
        when(this.query.getQueryLogicName()).thenReturn(queryLogicName);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getConnectionPriority()).thenReturn(Priority.NORMAL);
        when(this.query.getQueryAuthorizations()).thenReturn(authorization);
        this.cache.put(eq(queryId.toString()), isA(RunningQuery.class));
        // expectLastCall().thenThrow(new Exception("EXPECTED EXCEPTION IN AUDIT"));
        cache.unlock(queryName);
        transaction.commit();
        
        // Run the test
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ;
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "auditor", auditor);
        ReflectionTestUtils.setField(subject, "metrics", metrics);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        
        assertThrows(DatawaveWebApplicationException.class, () -> subject.reset(queryName));
    }
    
    @Test
    public void testUpdateQuery_auditException() throws Exception {
        // Set local test input
        UUID queryId = UUID.randomUUID();
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 9999);
        int pageTimeout = 60;
        int pagesize = 10;
        Long maxResultsOverride = null;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        String parameters = "invalidparam; valid:param";
        String userName = "userName";
        String userSid = "userSid";
        String userDN = "userDN";
        Query duplicateQuery = mock(Query.class);
        
        MultiValueMap<String,String> p = new LinkedMultiValueMap<>();
        p.set(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        p.set(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        p.set(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        p.set(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expirationDate));
        p.set(QueryParameters.QUERY_NAME, queryName);
        p.set(QueryParameters.QUERY_PAGESIZE, Integer.toString(pagesize));
        p.set(QueryParameters.QUERY_PAGETIMEOUT, Integer.toString(pageTimeout));
        p.set(QueryParameters.QUERY_STRING, query);
        p.set(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        p.set(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        p.set(QueryParameters.QUERY_PARAMS, parameters);
        
        p.set(PrivateAuditConstants.AUDIT_TYPE, Auditor.AuditType.LOCALONLY.name());
        p.set(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        p.set(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        p.set(PrivateAuditConstants.USER_DN, userDN);
        p.set(AuditParameters.AUDIT_ID, queryId.toString());
        MultivaluedMap auditMap = new MultivaluedMapImpl();
        auditMap.putAll(p);
        
        // Set expectations
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getAuthorizations()).thenReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        when(this.cache.get(queryId.toString())).thenReturn(this.runningQuery);
        when(this.runningQuery.getSettings()).thenReturn(this.query);
        when(this.query.getOwner()).thenReturn(userSid);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getLogicName()).thenReturn(queryLogicName);
        when(this.query.duplicate(queryName)).thenReturn(duplicateQuery);
        duplicateQuery.setId(queryId);
        duplicateQuery.setQueryLogicName(queryLogicName);
        duplicateQuery.setQuery(query);
        duplicateQuery.setBeginDate(beginDate);
        duplicateQuery.setEndDate(endDate);
        duplicateQuery.setQueryAuthorizations(queryAuthorizations);
        duplicateQuery.setExpirationDate(expirationDate);
        duplicateQuery.setPagesize(pagesize);
        duplicateQuery.setPageTimeout(pageTimeout);
        duplicateQuery.setParameters(isA(Set.class));
        when(duplicateQuery.toMap()).thenReturn(p);
        when(this.query.getQueryName()).thenReturn(queryName);
        when(this.query.getId()).thenReturn(queryId);
        when(this.runningQuery.getLogic()).thenReturn((QueryLogic) this.queryLogic1);
        when(this.queryLogic1.getAuditType(this.query)).thenReturn(Auditor.AuditType.LOCALONLY);
        
        when(this.auditor.audit(eq(auditMap))).thenThrow(new Exception("INTENTIONALLY THROWN EXCEPTION"));
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "auditor", auditor);
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        
        assertThrows(DatawaveWebApplicationException.class, () -> subject.updateQuery(queryId.toString(), queryLogicName, query, queryVisibility, beginDate,
                        endDate, queryAuthorizations, expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters));
    }
    
    @Test
    public void testDefineQuery_userNotInAllowedDNs() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 9999);
        int pagesize = 1000;
        int pageTimeout = -1;
        Long maxResultsOverride = null;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        String parameters = null;
        boolean trace = false;
        // Set expectations
        
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putAll(QueryParametersImpl.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                        expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters, trace));
        
        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);
        
        QueryParameters qp = new QueryParametersImpl();
        qp.validate(queryParameters);
        
        MultivaluedMap<String,String> op = new MultivaluedMapImpl<>();
        op.putAll(qp.getUnknownParameters(queryParameters));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        
        queryLogic1.validate(queryParameters);
        when(this.principal.getName()).thenReturn("userName Full");
        when(this.principal.getShortName()).thenReturn("userName");
        when(this.principal.getUserDN()).thenReturn(SubjectIssuerDNPair.of("userDN"));
        when(this.principal.getDNs()).thenReturn(new String[] {"userDN"});
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.queryLogic1.containsDNWithAccess(Collections.singletonList("userDN"))).thenReturn(false);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "marking", new ColumnVisibilitySecurityMarking());
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        Throwable result1 = assertThrows(DatawaveWebApplicationException.class, () -> subject.defineQuery(queryLogicName, queryParameters)).getCause();
        
        // Verify results
        assertTrue(result1 instanceof QueryException, "QueryException expected to have been thrown");
        assertEquals("401", ((QueryException) result1).getErrorCode(), "Thrown exception expected to have been due to access denied");
        assertEquals("None of the DNs used have access to this query logic: [userDN]", result1.getMessage(),
                        "Thrown exception expected to detail reason for access denial");
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testCreateQuery_userNotInAllowedDNs() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 9999);
        int pagesize = 1000;
        int pageTimeout = -1;
        Long maxResultsOverride = null;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        String parameters = null;
        boolean trace = false;
        // Set expectations
        
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putAll(QueryParametersImpl.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                        expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters, trace));
        
        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);
        
        QueryParameters qp = new QueryParametersImpl();
        qp.validate(queryParameters);
        
        MultivaluedMap<String,String> op = new MultivaluedMapImpl<>();
        op.putAll(qp.getUnknownParameters(queryParameters));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        
        queryLogic1.validate(queryParameters);
        when(this.principal.getName()).thenReturn("userName Full");
        when(this.principal.getShortName()).thenReturn("userName");
        when(this.principal.getUserDN()).thenReturn(SubjectIssuerDNPair.of("userDN"));
        when(this.principal.getDNs()).thenReturn(new String[] {"userDN"});
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.queryLogic1.containsDNWithAccess(Collections.singletonList("userDN"))).thenReturn(false);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "marking", new ColumnVisibilitySecurityMarking());
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        Throwable result1 = null;
        try {
            subject.createQuery(queryLogicName, queryParameters);
        } catch (DatawaveWebApplicationException e) {
            result1 = e.getCause();
        }
        
        // Verify results
        assertTrue(result1 instanceof QueryException, "QueryException expected to have been thrown");
        assertEquals("401", ((QueryException) result1).getErrorCode(), "Thrown exception expected to have been due to access denied");
        assertEquals("None of the DNs used have access to this query logic: [userDN]", result1.getMessage(),
                        "Thrown exception expected to detail reason for access denial");
    }
    
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testCreateQueryAndNext_userNotInAllowedDNs() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 9999);
        int pagesize = 1000;
        int pageTimeout = -1;
        Long maxResultsOverride = null;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        String parameters = null;
        boolean trace = false;
        // Set expectations
        
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putAll(QueryParametersImpl.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                        expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters, trace));
        
        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);
        
        QueryParameters qp = new QueryParametersImpl();
        qp.validate(queryParameters);
        
        MultivaluedMap<String,String> op = new MultivaluedMapImpl<>();
        op.putAll(qp.getUnknownParameters(queryParameters));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        
        queryLogic1.validate(queryParameters);
        when(this.principal.getName()).thenReturn("userName Full");
        when(this.principal.getShortName()).thenReturn("userName");
        when(this.principal.getUserDN()).thenReturn(SubjectIssuerDNPair.of("userDN"));
        when(this.principal.getDNs()).thenReturn(new String[] {"userDN"});
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.queryLogic1.containsDNWithAccess(Collections.singletonList("userDN"))).thenReturn(false);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "marking", new ColumnVisibilitySecurityMarking());
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        Throwable result1 = assertThrows(Exception.class, () -> subject.createQueryAndNext(queryLogicName, queryParameters)).getCause();
        // Verify results
        assertTrue(result1 instanceof QueryException, "QueryException expected to have been thrown");
        assertEquals("401", ((QueryException) result1).getErrorCode(), "Thrown exception expected to have been due to access denied");
        assertEquals("None of the DNs used have access to this query logic: [userDN]", result1.getMessage(),
                        "Thrown exception expected to detail reason for access denial");
    }
    
    @Test
    public void testPlanQuery_userNotInAllowedDNs() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 999999);
        int pagesize = 10;
        int pageTimeout = -1;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        boolean trace = false;
        String userName = "userName";
        String userSid = "userSid";
        String userDN = "userdn";
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);
        
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putSingle(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        queryParameters.putSingle(QueryParameters.QUERY_STRING, query);
        queryParameters.putSingle(QueryParameters.QUERY_NAME, queryName);
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.putSingle(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        queryParameters.putSingle("valid", "param");
        
        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);
        
        QueryParameters qp = new QueryParametersImpl();
        qp.validate(queryParameters);
        
        MultivaluedMap<String,String> op = new MultivaluedMapImpl<>();
        op.putAll(qp.getUnknownParameters(queryParameters));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());
        
        // Set expectations of the create logic
        queryLogic1.validate(queryParameters);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getUserDN()).thenReturn(userDNpair);
        when(this.principal.getDNs()).thenReturn(new String[] {userDN});
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).thenReturn(false);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "auditor", auditor);
        ReflectionTestUtils.setField(subject, "metrics", metrics);
        ReflectionTestUtils.setField(subject, "traceInfos", traceInfos);
        ReflectionTestUtils.setField(subject, "marking", new ColumnVisibilitySecurityMarking());
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        Throwable result1 = assertThrows(Exception.class, () -> subject.planQuery(queryLogicName, queryParameters)).getCause();
        // Verify results
        assertTrue(result1 instanceof QueryException, "QueryException expected to have been thrown");
        assertEquals("401", ((QueryException) result1).getErrorCode(), "Thrown exception expected to have been due to access denied");
        assertEquals("None of the DNs used have access to this query logic: [userdn]", result1.getMessage(),
                        "Thrown exception expected to detail reason for access denial");
    }
    
    @Test
    public void testPredictQuery_userNotInAllowedDNs() throws Exception {
        // Set local test input
        String queryLogicName = "queryLogicName";
        String query = "query";
        String queryName = "queryName";
        String queryVisibility = "A&B";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String queryAuthorizations = "AUTH_1";
        Date expirationDate = new Date(currentTime + 999999);
        int pagesize = 10;
        int pageTimeout = -1;
        QueryPersistence persistenceMode = QueryPersistence.PERSISTENT;
        boolean trace = false;
        String userName = "userName";
        String userSid = "userSid";
        String userDN = "userdn";
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);
        
        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putSingle(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        queryParameters.putSingle(QueryParameters.QUERY_STRING, query);
        queryParameters.putSingle(QueryParameters.QUERY_NAME, queryName);
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.putSingle(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        queryParameters.putSingle("valid", "param");
        
        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);
        
        QueryParameters qp = new QueryParametersImpl();
        qp.validate(queryParameters);
        
        MultivaluedMap<String,String> op = new MultivaluedMapImpl<>();
        op.putAll(qp.getUnknownParameters(queryParameters));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());
        
        // Set expectations of the create logic
        queryLogic1.validate(queryParameters);
        when(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).thenReturn((QueryLogic) this.queryLogic1);
        when(this.context.getCallerPrincipal()).thenReturn(this.principal);
        when(this.principal.getName()).thenReturn(userName);
        when(this.principal.getShortName()).thenReturn(userSid);
        when(this.principal.getUserDN()).thenReturn(userDNpair);
        when(this.principal.getDNs()).thenReturn(new String[] {userDN});
        when(this.principal.getProxyServers()).thenReturn(new ArrayList<>(0));
        when(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).thenReturn(false);
        
        // Run the test
        
        QueryExecutorBean subject = new QueryExecutorBean();
        ReflectionTestUtils.setField(subject, "ctx", context);
        ReflectionTestUtils.setField(subject, "connectionFactory", connectionFactory);
        ReflectionTestUtils.setField(subject, "responseObjectFactory", responseObjectFactory);
        ReflectionTestUtils.setField(subject, "qlCache", qlCache);
        ReflectionTestUtils.setField(subject, "queryCache", cache);
        ReflectionTestUtils.setField(subject, "closedQueryCache", closedCache);
        ReflectionTestUtils.setField(subject, "persister", persister);
        ReflectionTestUtils.setField(subject, "queryLogicFactory", queryLogicFactory);
        ReflectionTestUtils.setField(subject, "queryExpirationConf", queryExpirationConf);
        ReflectionTestUtils.setField(subject, "auditor", auditor);
        ReflectionTestUtils.setField(subject, "metrics", metrics);
        ReflectionTestUtils.setField(subject, "traceInfos", traceInfos);
        ReflectionTestUtils.setField(subject, "marking", new ColumnVisibilitySecurityMarking());
        ReflectionTestUtils.setField(subject, "qp", new QueryParametersImpl());
        ReflectionTestUtils.setField(subject, "metricFactory", new QueryMetricFactoryImpl());
        ReflectionTestUtils.setField(connectionRequestBean, "ctx", context);
        ReflectionTestUtils.setField(subject, "accumuloConnectionRequestBean", connectionRequestBean);
        Throwable result1 = assertThrows(Exception.class, () -> subject.predictQuery(queryLogicName, queryParameters)).getCause();
        // Verify results
        assertTrue(result1 instanceof QueryException, "QueryException expected to have been thrown");
        assertEquals("401", ((QueryException) result1).getErrorCode(), "Thrown exception expected to have been due to access denied");
        assertEquals("None of the DNs used have access to this query logic: [userdn]", result1.getMessage(),
                        "Thrown exception expected to detail reason for access denial");
    }
    
    public class TestQuery extends QueryImpl {
        private static final long serialVersionUID = -1514300746858409155L;
        
        public TestQuery(Query query) {
            this.setBeginDate(query.getBeginDate());
            this.setColumnVisibility(query.getColumnVisibility());
            this.setEndDate(query.getEndDate());
            this.setExpirationDate(query.getExpirationDate());
            this.setId(query.getId());
            this.setPagesize(query.getPagesize());
            this.setPageTimeout(query.getPageTimeout());
            this.setParameters(query.getParameters());
            this.setQuery(query.getQuery());
            this.setQueryAuthorizations(query.getQueryAuthorizations());
            this.setQueryLogicName(query.getQueryLogicName());
            this.setQueryName(query.getQueryName());
            this.setOwner(query.getOwner());
            this.setUserDN(query.getUserDN());
        }
        
    }
    
    public static class QuerySyntaxParserQueryLogic<T> extends BaseQueryLogic<T> {
        public Map<?,?> getQuerySyntaxParsers() {
            return null;
        }
        
        @Override
        public GenericQueryConfiguration initialize(Connector connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
            return null;
        }
        
        @Override
        public void setupQuery(GenericQueryConfiguration configuration) throws Exception {
            // No op
        }
        
        @Override
        public String getPlan(Connector connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields, boolean expandValues)
                        throws Exception {
            return "";
        }
        
        @Override
        public Priority getConnectionPriority() {
            return null;
        }
        
        @Override
        public QueryLogicTransformer getTransformer(Query settings) {
            return null;
        }
        
        @Override
        public Object clone() throws CloneNotSupportedException {
            return null;
        }
        
        @Override
        public Set<String> getOptionalQueryParameters() {
            return Collections.emptySet();
        }
        
        @Override
        public Set<String> getRequiredQueryParameters() {
            return Collections.emptySet();
        }
        
        @Override
        public Set<String> getExampleQueries() {
            return Collections.emptySet();
        }
    }
}
