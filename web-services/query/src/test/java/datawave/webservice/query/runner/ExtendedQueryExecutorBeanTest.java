package datawave.webservice.query.runner;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.notNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.powermock.reflect.Whitebox.setInternalState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;

import javax.ejb.EJBContext;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.jms.JMSRuntimeException;
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

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.core.common.audit.PrivateAuditConstants;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.connection.AccumuloConnectionFactory.Priority;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.core.query.logic.QueryLogicTransformer;
import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.marking.SecurityMarking;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.microservice.query.QueryParameters;
import datawave.microservice.query.QueryPersistence;
import datawave.microservice.query.config.QueryExpirationProperties;
import datawave.microservice.querymetric.QueryMetric;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.query.data.UUIDType;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.authorization.remote.RemoteUserOperationsImpl;
import datawave.security.user.UserOperationsBean;
import datawave.security.util.WSAuthorizationsUtil;
import datawave.webservice.common.audit.AuditBean;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor.AuditType;
import datawave.webservice.common.exception.BadRequestException;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.common.exception.NoResultsException;
import datawave.webservice.query.cache.ClosedQueryCache;
import datawave.webservice.query.cache.CreatedQueryLogicCacheBean;
import datawave.webservice.query.cache.QueryCache;
import datawave.webservice.query.cache.QueryTraceCache;
import datawave.webservice.query.cache.QueryTraceCache.CacheListener;
import datawave.webservice.query.cache.QueryTraceCache.PatternWrapper;
import datawave.webservice.query.configuration.LookupUUIDConfiguration;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NoResultsQueryException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.factory.Persister;
import datawave.webservice.query.logic.QueryLogicFactoryImpl;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.query.util.GetUUIDCriteria;
import datawave.webservice.query.util.LookupUUIDUtil;
import datawave.webservice.query.util.MapUtils;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.QueryImplListResponse;
import datawave.webservice.result.QueryLogicResponse;
import datawave.webservice.result.VoidResponse;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.security.auth.Subject")
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
    AccumuloClient client;

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

    QuerySyntaxParserQueryLogic<?> queryLogic2;

    @Mock
    QueryMetric queryMetric;

    @Mock
    ResultsPage resultsPage;

    @Mock
    RunningQuery runningQuery;

    @Mock
    QueryTraceCache traceCache;

    @Mock
    Multimap<String,PatternWrapper> traceInfos;

    @Mock
    QueryLogicTransformer transformer;

    @Mock
    TransformIterator transformIterator;

    @Mock
    UserTransaction transaction;

    @Mock
    Pair<QueryLogic<?>,AccumuloClient> tuple;

    @Mock
    HttpHeaders httpHeaders;

    @Mock
    ResponseObjectFactory responseObjectFactory;

    @Mock
    AccumuloConnectionRequestBean connectionRequestBean;

    @Mock
    UriInfo uriInfo;

    @Mock
    UserOperationsBean userOperations;

    QueryExpirationProperties queryExpirationConf;

    @BeforeClass
    public static void setup() throws Exception {}

    @Before
    public void setupBefore() throws Exception {

        queryLogic2 = PowerMock.createMock(QuerySyntaxParserQueryLogic.class);

        queryExpirationConf = new QueryExpirationProperties();
        queryExpirationConf.setShortCircuitCheckTime(45);
        queryExpirationConf.setShortCircuitTimeout(58);
        queryExpirationConf.setIdleTimeout(60);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testAdminCancel_HappyPath() throws Exception {
        // Set local test input
        UUID queryId = UUID.randomUUID();
        // Set expectations of the create logic
        expect(this.connectionRequestBean.adminCancelConnectionRequest(queryId.toString())).andReturn(false);
        expect(this.qlCache.poll(queryId.toString())).andReturn(this.tuple);
        expect(this.tuple.getFirst()).andReturn((QueryLogic) this.queryLogic1);
        this.queryLogic1.close();
        expect(this.tuple.getSecond()).andReturn(this.client);
        this.connectionFactory.returnClient(this.client);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);
        VoidResponse result1 = subject.adminCancel(queryId.toString());
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a non-null response", result1);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test(expected = DatawaveWebApplicationException.class)
    public void testAdminCancel_NullTupleReturnedAndQueryExceptionThrown() throws Exception {
        // Set local test input
        UUID queryId = UUID.randomUUID();

        // Set expectations of the create logic
        expect(this.connectionRequestBean.adminCancelConnectionRequest(queryId.toString())).andReturn(false);
        expect(this.qlCache.poll(queryId.toString())).andReturn(null);
        expect(this.cache.get(queryId.toString())).andReturn(null);
        expect(this.persister.adminFindById(queryId.toString())).andReturn(Arrays.asList(this.query, this.query));

        // Run the test
        PowerMock.replayAll();
        try {
            QueryExecutorBean subject = new QueryExecutorBean();
            setInternalState(subject, EJBContext.class, context);
            setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
            setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
            setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
            setInternalState(subject, QueryCache.class, cache);
            setInternalState(subject, ClosedQueryCache.class, closedCache);
            setInternalState(subject, Persister.class, persister);
            setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
            setInternalState(connectionRequestBean, EJBContext.class, context);
            setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);

            subject.adminCancel(queryId.toString());
        } finally {
            PowerMock.verifyAll();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testAdminCancel_RunningQueryFoundInCache() throws Exception {
        // Set local test input
        UUID queryId = UUID.randomUUID();

        // Set expectations of the create logic
        expect(this.connectionRequestBean.adminCancelConnectionRequest(queryId.toString())).andReturn(false);
        expect(this.qlCache.poll(queryId.toString())).andReturn(null);
        expect(this.cache.get(queryId.toString())).andReturn(this.runningQuery);
        expect(this.runningQuery.getSettings()).andReturn(this.query);
        this.runningQuery.cancel();
        this.runningQuery.closeConnection(this.connectionFactory);
        expect(this.query.getId()).andReturn(queryId);
        cache.remove(queryId.toString());

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);

        VoidResponse result1 = subject.adminCancel(queryId.toString());
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a non-null response", result1);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testAdminCancel_LookupAccumuloQuery() throws Exception {
        // Set local test input
        UUID queryId = UUID.randomUUID();
        List<String> dnList = Collections.singletonList("qwe");

        // Set expectations of the create logic
        expect(this.connectionRequestBean.adminCancelConnectionRequest(queryId.toString())).andReturn(false);
        expect(this.qlCache.poll(queryId.toString())).andReturn(null);
        expect(this.cache.get(queryId.toString())).andReturn(null);
        expect(this.persister.adminFindById(queryId.toString())).andReturn(Lists.newArrayList(query));
        expect(this.query.getQueryAuthorizations()).andReturn("AUTH_1,AUTH_2").anyTimes();
        expect(this.query.getQueryLogicName()).andReturn("ql1").anyTimes();
        expect(this.query.getOwner()).andReturn("qwe").anyTimes();
        expect(this.query.getUserDN()).andReturn("qwe").anyTimes();
        expect(this.query.getDnList()).andReturn(dnList).anyTimes();
        expect(this.query.getId()).andReturn(queryId).anyTimes();
        expect(this.query.getQuery()).andReturn("qwe").anyTimes();
        expect(this.query.getBeginDate()).andReturn(null).anyTimes();
        expect(this.query.getEndDate()).andReturn(null).anyTimes();
        expect(this.query.getColumnVisibility()).andReturn(null).anyTimes();
        expect(this.query.getQueryName()).andReturn(null).anyTimes();
        expect(this.query.getPagesize()).andReturn(0).anyTimes();
        expect(this.query.getPageTimeout()).andReturn(-1).anyTimes();
        expect(this.query.getExpirationDate()).andReturn(null).anyTimes();
        expect(this.query.getParameters()).andReturn((Set) Collections.emptySet()).anyTimes();
        expect(this.query.findParameter(RemoteUserOperationsImpl.INCLUDE_REMOTE_SERVICES))
                        .andReturn(new QueryImpl.Parameter(RemoteUserOperationsImpl.INCLUDE_REMOTE_SERVICES, "true")).anyTimes();
        expect(context.getCallerPrincipal()).andReturn(principal);
        expect(this.queryLogicFactory.getQueryLogic("ql1", principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getConnectionPriority()).andReturn(Priority.NORMAL);
        expect(this.queryLogic1.getCollectQueryMetrics()).andReturn(false);
        expect(this.queryLogic1.getResultLimit(this.query)).andReturn(-1L);
        expect(this.queryLogic1.getMaxResults()).andReturn(-1L);
        this.queryLogic1.preInitialize(this.query, WSAuthorizationsUtil.buildAuthorizations(Collections.singleton(Sets.newHashSet("AUTH_2", "AUTH_1"))));
        expect(this.queryLogic1.getUserOperations()).andReturn(null);
        cache.put(eq(queryId.toString()), isA(RunningQuery.class));
        cache.remove(queryId.toString());
        this.queryLogic1.close();

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);

        VoidResponse result1 = subject.adminCancel(queryId.toString());
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a non-null response", result1);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testAdminClose_NonNullTupleReturned() throws Exception {
        // Set local test input
        UUID queryId = UUID.randomUUID();

        // Set expectations
        expect(this.connectionRequestBean.adminCancelConnectionRequest(queryId.toString())).andReturn(false);
        expect(this.qlCache.poll(queryId.toString())).andReturn(this.tuple);
        expect(this.tuple.getFirst()).andReturn((QueryLogic) this.queryLogic1);
        this.queryLogic1.close();
        expect(this.tuple.getSecond()).andReturn(this.client);
        this.connectionFactory.returnClient(this.client);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);
        setInternalState(connectionRequestBean, EJBContext.class, context);
        VoidResponse result1 = subject.adminClose(queryId.toString());
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a non-null response performing an admin close", result1);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test(expected = DatawaveWebApplicationException.class)
    public void testAdminClose_NullTupleReturnedAndQueryExceptionThrown() throws Exception {
        // Set local test input
        UUID queryId = UUID.randomUUID();

        // Set expectations
        expect(this.connectionRequestBean.adminCancelConnectionRequest(queryId.toString())).andReturn(false);
        expect(this.qlCache.poll(queryId.toString())).andReturn(null);
        expect(this.cache.get(queryId.toString())).andReturn(null);
        expect(this.persister.adminFindById(queryId.toString())).andReturn(null);

        // Run the test
        PowerMock.replayAll();
        try {
            QueryExecutorBean subject = new QueryExecutorBean();
            setInternalState(subject, EJBContext.class, context);
            setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
            setInternalState(subject, QueryCache.class, cache);
            setInternalState(subject, ClosedQueryCache.class, closedCache);
            setInternalState(subject, Persister.class, persister);
            setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
            setInternalState(connectionRequestBean, EJBContext.class, context);
            setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);
            subject.adminClose(queryId.toString());
        } finally {
            PowerMock.verifyAll();
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testCancel_HappyPath() throws Exception {
        // Set local test input
        String userName = "userName";
        String userSid = "userSid";
        UUID queryId = UUID.randomUUID();

        // Set expectations of the create logic
        expect(this.connectionRequestBean.cancelConnectionRequest(queryId.toString())).andReturn(false);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getUserDN()).andReturn(SubjectIssuerDNPair.of(userName));
        expect(this.principal.getDNs()).andReturn(new String[] {userName});
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.qlCache.pollIfOwnedBy(queryId.toString(), userSid)).andReturn(this.tuple);
        this.closedCache.remove(queryId.toString());
        expect(this.tuple.getFirst()).andReturn((QueryLogic) this.queryLogic1);
        this.queryLogic1.close();
        expect(this.tuple.getSecond()).andReturn(this.client);
        this.connectionFactory.returnClient(this.client);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        VoidResponse result1 = subject.cancel(queryId.toString());
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a non-null response", result1);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test(expected = DatawaveWebApplicationException.class)
    public void testCancel_NullTupleReturnedAndQueryExceptionThrown() throws Exception {
        // Set local test input
        String userName = "userName";
        UUID queryId = UUID.randomUUID();
        String userSid = "userSid";
        String queryAuthorizations = "AUTH_1";

        // Set expectations of the create logic
        expect(this.connectionRequestBean.cancelConnectionRequest(queryId.toString())).andReturn(false);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).times(2);
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getUserDN()).andReturn(SubjectIssuerDNPair.of(userName));
        expect(this.principal.getDNs()).andReturn(new String[] {userName});
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.qlCache.pollIfOwnedBy(queryId.toString(), userSid)).andReturn(null);
        expect(this.closedCache.exists(queryId.toString())).andReturn(false);
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        expect(this.cache.get(queryId.toString())).andReturn(null);
        expect(this.persister.findById(queryId.toString())).andReturn(Arrays.asList((Query) this.query, this.query));

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);

        try {
            subject.cancel(queryId.toString());
        } finally {
            PowerMock.verifyAll();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testCancel_RunningQueryFoundInCache() throws Exception {
        // Set local test input
        String userName = "userName";
        UUID queryId = UUID.randomUUID();
        String userSid = "userSid";
        String queryAuthorizations = "AUTH_1";

        // Set expectations of the create logic
        expect(this.connectionRequestBean.cancelConnectionRequest(queryId.toString())).andReturn(false);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).times(2);
        expect(this.principal.getName()).andReturn(userName);
        expect(this.qlCache.pollIfOwnedBy(queryId.toString(), userSid)).andReturn(null);
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getUserDN()).andReturn(SubjectIssuerDNPair.of(userName));
        expect(this.principal.getDNs()).andReturn(new String[] {userName});
        expect(this.principal.getShortName()).andReturn(userSid).anyTimes();
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        expect(this.cache.get(queryId.toString())).andReturn(this.runningQuery);
        this.closedCache.remove(queryId.toString());
        expect(this.runningQuery.getSettings()).andReturn(this.query).times(2);
        expect(this.query.getOwner()).andReturn(userSid);
        this.runningQuery.cancel();
        this.runningQuery.closeConnection(this.connectionFactory);
        expect(this.query.getId()).andReturn(queryId);
        cache.remove(queryId.toString());

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);
        VoidResponse result1 = subject.cancel(queryId.toString());
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a non-null response", result1);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test(expected = DatawaveWebApplicationException.class)
    public void testClose_NullTupleReturnedFromQueryLogicCache() throws Exception {
        // Set local test input
        String userName = "userName";
        String userSid = "userSid";
        UUID queryId = UUID.randomUUID();
        String queryAuthorizations = "AUTH_1";

        // Set expectations
        expect(this.connectionRequestBean.cancelConnectionRequest(queryId.toString(), userName.toLowerCase())).andReturn(false);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName).anyTimes();
        expect(this.principal.getShortName()).andReturn(userSid).anyTimes();
        expect(this.principal.getDNs()).andReturn(new String[] {userName});
        expect(this.principal.getUserDN()).andReturn(SubjectIssuerDNPair.of(userName));
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        expect(this.qlCache.pollIfOwnedBy(queryId.toString(), userSid)).andReturn(null);
        expect(this.closedCache.exists(queryId.toString())).andReturn(false);
        expect(this.cache.get(queryId.toString())).andReturn(null);
        expect(this.persister.findById(queryId.toString())).andReturn(new ArrayList<>(0));

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);

        try {
            subject.close(queryId.toString());
        } finally {
            PowerMock.verifyAll();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test(expected = DatawaveWebApplicationException.class)
    public void testClose_UncheckedException() throws Exception {
        // Set local test input
        String userSid = "userSid";
        String userName = "userName";
        UUID queryId = UUID.randomUUID();

        // Set expectations
        expect(this.connectionRequestBean.cancelConnectionRequest(queryId.toString(), userName.toLowerCase())).andReturn(false);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.principal.getDNs()).andReturn(new String[] {userName});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.qlCache.pollIfOwnedBy(queryId.toString(), userSid)).andReturn(this.tuple);
        expect(this.principal.getUserDN()).andReturn(SubjectIssuerDNPair.of(userName));
        expect(this.tuple.getFirst()).andReturn((QueryLogic) this.queryLogic1);
        this.queryLogic1.close();
        PowerMock.expectLastCall().andThrow(ILLEGAL_STATE_EXCEPTION);
        expect(this.tuple.getSecond()).andThrow(ILLEGAL_STATE_EXCEPTION);

        // Run the test
        PowerMock.replayAll();
        try {
            QueryExecutorBean subject = new QueryExecutorBean();
            setInternalState(subject, EJBContext.class, context);
            setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
            setInternalState(subject, QueryCache.class, cache);
            setInternalState(subject, ClosedQueryCache.class, closedCache);
            setInternalState(subject, Persister.class, persister);
            setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
            setInternalState(connectionRequestBean, EJBContext.class, context);
            setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);

            subject.close(queryId.toString());
        } finally {
            PowerMock.verifyAll();
        }
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
        String userDN = "userDN";
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);
        List<String> dnList = Collections.singletonList(userDN);
        UUID queryId = UUID.randomUUID();
        long pageNumber = 0L;

        HashMap<String,Collection<String>> authsMap = new HashMap<>();
        authsMap.put("userdn", Arrays.asList(queryAuthorizations));

        MultiValueMap<String,String> queryParameters = new LinkedMultiValueMap<>();
        queryParameters.add(QueryParameters.QUERY_STRING, query);
        queryParameters.add(QueryParameters.QUERY_NAME, queryName);
        queryParameters.add(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        queryParameters.add(QueryParameters.QUERY_BEGIN, DefaultQueryParameters.formatDate(beginDate));
        queryParameters.add(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        queryParameters.add(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expirationDate));
        queryParameters.add(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.add(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.add(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.add(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.add(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.add(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        queryParameters.add("valid", "param");

        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);

        QueryParameters qp = new DefaultQueryParameters();
        qp.validate(queryParameters);

        MultiValueMap<String,String> op = qp.getUnknownParameters(queryParameters);
        op.add(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.add(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.add(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());

        // Set expectations of the create logic
        queryLogic1.validate(queryParameters);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getMaxPageSize()).andReturn(1000).times(2);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.principal.getUserDN()).andReturn(userDNpair);
        expect(this.principal.getDNs()).andReturn(new String[] {userDN});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).andReturn(true);
        expect(this.queryLogic1.getAuditType(null)).andReturn(AuditType.NONE);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        expect(persister.create(eq(userDNpair.subjectDN()), eq(dnList), eq(marking), eq(queryLogicName), eq(qp), eq(MapUtils.toMultivaluedMap(op))))
                        .andReturn(this.query);
        expect(this.queryLogic1.getAuditType(this.query)).andReturn(AuditType.NONE);
        expect(this.queryLogic1.getConnectionPriority()).andReturn(Priority.NORMAL);
        expect(this.queryLogic1.getConnPoolName()).andReturn("connPool1");
        expect(this.queryLogic1.isLongRunningQuery()).andReturn(false);
        expect(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).andReturn(null);
        this.query.populateTrackingMap(null);
        this.connectionRequestBean.requestBegin(queryId.toString(), userDN.toLowerCase(), null);
        expect(this.connectionFactory.getClient(userDN.toLowerCase(), new ArrayList<>(0), "connPool1", Priority.NORMAL, null)).andReturn(this.client);
        this.connectionRequestBean.requestEnd(queryId.toString());
        expect(this.traceInfos.get(userSid)).andReturn(new ArrayList<>(0));
        expect(this.traceInfos.get(null)).andReturn(Arrays.asList(PatternWrapper.wrap("NONMATCHING_REGEX")));
        expect(this.qlCache.add(queryId.toString(), userSid, this.queryLogic1, this.client)).andReturn(true);
        expect(this.queryLogic1.getCollectQueryMetrics()).andReturn(true);
        expect(this.principal.getPrimaryUser()).andReturn(dwUser).anyTimes();
        expect(this.dwUser.getAuths()).andReturn(Collections.singleton(queryAuthorizations)).anyTimes();
        expect(this.principal.getProxiedUsers()).andReturn(Collections.singletonList(dwUser));
        expect(this.userOperations.getRemoteUser(this.principal)).andReturn(this.principal);
        expect(this.query.getOwner()).andReturn(userSid).anyTimes();
        expect(this.query.getId()).andReturn(queryId).anyTimes();
        expect(this.query.getQuery()).andReturn(queryName).anyTimes();
        expect(this.query.getQueryLogicName()).andReturn(queryLogicName).anyTimes();
        expect(this.query.getBeginDate()).andReturn(null).anyTimes();
        expect(this.query.getEndDate()).andReturn(null).anyTimes();
        expect(this.query.getColumnVisibility()).andReturn(null).anyTimes();
        expect(this.query.getQueryAuthorizations()).andReturn(queryAuthorizations).anyTimes();
        expect(this.query.getQueryName()).andReturn(null).anyTimes();
        expect(this.query.getPagesize()).andReturn(0).anyTimes();
        expect(this.query.getExpirationDate()).andReturn(null).anyTimes();
        expect(this.query.getParameters()).andReturn((Set) Collections.emptySet()).anyTimes();
        expect(this.query.getUncaughtExceptionHandler()).andReturn(new QueryUncaughtExceptionHandler()).anyTimes();
        expect(this.query.findParameter(RemoteUserOperationsImpl.INCLUDE_REMOTE_SERVICES))
                        .andReturn(new QueryImpl.Parameter(RemoteUserOperationsImpl.INCLUDE_REMOTE_SERVICES, "true")).anyTimes();
        this.metrics.updateMetric(isA(QueryMetric.class));
        PowerMock.expectLastCall().times(2);
        expect(this.query.getUserDN()).andReturn(userDN).anyTimes();
        expect(this.query.getDnList()).andReturn(dnList).anyTimes();
        expect(this.queryLogic1.getResultLimit(this.query)).andReturn(-1L);
        expect(this.queryLogic1.getMaxResults()).andReturn(-1L);
        this.queryLogic1.preInitialize(this.query, WSAuthorizationsUtil.buildAuthorizations(Collections.singleton(Sets.newHashSet("AUTH_1"))));
        expect(this.queryLogic1.getUserOperations()).andReturn(null);
        expect(this.queryLogic1.initialize(eq(this.client), eq(this.query), isA(Set.class))).andReturn(this.genericConfiguration);
        this.queryLogic1.setupQuery(this.genericConfiguration);
        expect(this.queryLogic1.getTransformIterator(this.query)).andReturn(this.transformIterator);
        cache.put(eq(queryId.toString()), isA(RunningQuery.class));
        expect(this.genericConfiguration.getQueryString()).andReturn(queryName).once();
        expect(this.qlCache.poll(queryId.toString())).andReturn(null);

        // Set expectations of the next logic
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.context.getUserTransaction()).andReturn(this.transaction).anyTimes();

        this.transaction.begin();
        expect(this.cache.get(queryId.toString())).andReturn(this.runningQuery);
        expect(cache.lock(queryId.toString())).andReturn(true);
        expect(this.runningQuery.getSettings()).andReturn(this.query);
        expect(this.runningQuery.getClient()).andReturn(this.client);

        this.runningQuery.setActiveCall(true);
        expectLastCall();

        // expect(this.runningQuery.getTraceInfo()).andReturn(this.traceInfo);
        expect(this.runningQuery.next()).andReturn(this.resultsPage);
        expect(this.runningQuery.getLastPageNumber()).andReturn(pageNumber);
        expect(this.runningQuery.getLogic()).andReturn((QueryLogic) this.queryLogic1).times(2);
        expect(this.runningQuery.getSettings()).andReturn(this.query).anyTimes();
        expect(this.queryLogic1.getEnrichedTransformer(this.query)).andReturn(this.transformer);
        expect(this.transformer.createResponse(this.resultsPage)).andReturn(this.baseResponse);
        expect(this.resultsPage.getStatus()).andReturn(ResultsPage.Status.COMPLETE).times(2);
        expect(this.responseObjectFactory.getQueryImpl()).andReturn(new QueryImpl());
        expect(queryLogic1.getResultLimit(anyObject(QueryImpl.class))).andReturn(-1L);
        this.baseResponse.setHasResults(true);
        this.baseResponse.setPageNumber(pageNumber);
        expect(this.queryLogic1.getLogicName()).andReturn(queryLogicName);
        this.baseResponse.setLogicName(queryLogicName);
        this.baseResponse.setQueryId(queryId.toString());
        expect(this.runningQuery.getMetric()).andReturn(this.queryMetric);
        this.runningQuery.setActiveCall(false);
        expectLastCall();
        this.queryMetric.setProxyServers(eq(new ArrayList<>(0)));
        expect(this.responseObjectFactory.getEventQueryResponse()).andReturn(new DefaultEventQueryResponse());

        cache.unlock(queryId.toString());
        expect(this.transaction.getStatus()).andReturn(Status.STATUS_ACTIVE).anyTimes();
        this.transaction.commit();
        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, UserOperationsBean.class, userOperations);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactoryImpl.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, QueryMetricsBean.class, metrics);
        setInternalState(subject, Multimap.class, traceInfos);
        setInternalState(subject, SecurityMarking.class, new ColumnVisibilitySecurityMarking());
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);
        BaseQueryResponse result1 = subject.createQueryAndNext(queryLogicName, MapUtils.toMultivaluedMap(queryParameters));
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a non-null response", result1);
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
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, DefaultQueryParameters.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.putSingle(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        queryParameters.putSingle("valid", "param");

        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);

        QueryParameters qp = new DefaultQueryParameters();
        qp.validate(queryParameters);

        MultivaluedMap<String,String> op = new MultivaluedMapImpl<>();
        op.putAll(qp.getUnknownParameters(queryParameters));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());

        // Set expectations of the create logic
        queryLogic1.validate(queryParameters);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getMaxPageSize()).andReturn(1000).times(2);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.principal.getUserDN()).andReturn(userDNpair);
        expect(this.principal.getDNs()).andReturn(new String[] {userDN});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).andReturn(true);
        expect(this.queryLogic1.getAuditType(null)).andReturn(AuditType.NONE);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        expect(persister.create(eq(userDNpair.subjectDN()), eq(dnList), eq(marking), eq(queryLogicName), eq(qp), eq(op))).andReturn(this.query);
        expect(this.queryLogic1.getAuditType(this.query)).andReturn(AuditType.NONE);
        expect(this.queryLogic1.getConnectionPriority()).andReturn(Priority.NORMAL);
        expect(this.queryLogic1.getConnPoolName()).andReturn("connPool1");
        expect(this.queryLogic1.isLongRunningQuery()).andReturn(false);
        expect(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).andReturn(null);
        this.query.populateTrackingMap(null);
        this.connectionRequestBean.requestBegin(queryId.toString(), userDN.toLowerCase(), null);
        expect(this.connectionFactory.getClient(userDN.toLowerCase(), new ArrayList<>(0), "connPool1", Priority.NORMAL, null)).andReturn(this.client);
        this.connectionRequestBean.requestEnd(queryId.toString());
        expect(this.traceInfos.get(userSid)).andReturn(new ArrayList<>(0));
        expect(this.traceInfos.get(null)).andReturn(Arrays.asList(PatternWrapper.wrap("NONMATCHING_REGEX")));
        expect(this.qlCache.add(queryId.toString(), userSid, this.queryLogic1, this.client)).andReturn(true);
        expect(this.queryLogic1.getCollectQueryMetrics()).andReturn(true);
        expect(this.principal.getPrimaryUser()).andReturn(dwUser).anyTimes();
        expect(this.dwUser.getAuths()).andReturn(Collections.singleton(queryAuthorizations)).anyTimes();
        expect(this.principal.getProxiedUsers()).andReturn(Collections.singletonList(dwUser));
        expect(this.userOperations.getRemoteUser(this.principal)).andReturn(this.principal);
        expect(this.query.getOwner()).andReturn(userSid).anyTimes();
        expect(this.query.getId()).andReturn(queryId).anyTimes();
        expect(this.query.getQuery()).andReturn(queryName).anyTimes();
        expect(this.query.getQueryLogicName()).andReturn(queryLogicName).anyTimes();
        expect(this.query.getBeginDate()).andReturn(null).anyTimes();
        expect(this.query.getEndDate()).andReturn(null).anyTimes();
        expect(this.query.getColumnVisibility()).andReturn(null).anyTimes();
        expect(this.query.getQueryAuthorizations()).andReturn(queryAuthorizations).anyTimes();
        expect(this.query.getQueryName()).andReturn(null).anyTimes();
        expect(this.query.getPagesize()).andReturn(0).anyTimes();
        expect(this.query.getExpirationDate()).andReturn(null).anyTimes();
        expect(this.query.getParameters()).andReturn((Set) Collections.emptySet()).anyTimes();
        expect(this.query.getUncaughtExceptionHandler()).andReturn(new QueryUncaughtExceptionHandler()).anyTimes();
        expect(this.query.findParameter(RemoteUserOperationsImpl.INCLUDE_REMOTE_SERVICES))
                        .andReturn(new QueryImpl.Parameter(RemoteUserOperationsImpl.INCLUDE_REMOTE_SERVICES, "true")).anyTimes();
        this.metrics.updateMetric(isA(QueryMetric.class));
        PowerMock.expectLastCall().times(2);
        expect(this.query.getUserDN()).andReturn(userDN).anyTimes();
        expect(this.query.getDnList()).andReturn(dnList).anyTimes();
        expect(this.queryLogic1.getResultLimit(this.query)).andReturn(-1L);
        expect(this.queryLogic1.getMaxResults()).andReturn(-1L);
        this.queryLogic1.preInitialize(this.query, WSAuthorizationsUtil.buildAuthorizations(Collections.singleton(Sets.newHashSet("AUTH_1"))));
        expect(this.queryLogic1.getUserOperations()).andReturn(null);
        expect(this.queryLogic1.initialize(eq(this.client), eq(this.query), isA(Set.class))).andReturn(this.genericConfiguration);
        this.queryLogic1.setupQuery(this.genericConfiguration);
        expect(this.queryLogic1.getTransformIterator(this.query)).andReturn(this.transformIterator);
        cache.put(eq(queryId.toString()), isA(RunningQuery.class));
        expect(this.genericConfiguration.getQueryString()).andReturn(queryName).once();
        expect(this.qlCache.poll(queryId.toString())).andReturn(null);
        expect(this.responseObjectFactory.getQueryImpl()).andReturn(new QueryImpl());
        expect(queryLogic1.getResultLimit(anyObject(QueryImpl.class))).andReturn(-1L);

        // Set expectations of the next logic
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.context.getUserTransaction()).andReturn(this.transaction).anyTimes();
        expect(this.transaction.getStatus()).andReturn(Status.STATUS_NO_TRANSACTION).anyTimes();
        cache.unlock("{id}");
        PowerMock.expectLastCall().anyTimes();
        this.transaction.begin();
        this.transaction.setRollbackOnly();
        PowerMock.expectLastCall().anyTimes();

        expect(this.responseObjectFactory.getEventQueryResponse()).andReturn(new DefaultEventQueryResponse());
        this.transaction.commit();
        QueryExecutorBean subject = new QueryExecutorBean();
        // Run the test
        PowerMock.replayAll();

        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, UserOperationsBean.class, userOperations);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactoryImpl.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, QueryMetricsBean.class, metrics);
        setInternalState(subject, Multimap.class, traceInfos);
        setInternalState(subject, SecurityMarking.class, new ColumnVisibilitySecurityMarking());
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);
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

        assertNotNull("Expected a non-null response", result1);
    }

    @Test
    public void testCreateQueryAndNext_PageSizeParam() throws Exception {
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
        String errorCode = DatawaveErrorCode.INVALID_PAGE_SIZE.getErrorCode();
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);
        List<String> dnList = Collections.singletonList(userDN);
        UUID queryId = UUID.randomUUID();

        HashMap<String,Collection<String>> authsMap = new HashMap<>();
        authsMap.put("userdn", Arrays.asList(queryAuthorizations));

        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putSingle(QueryParameters.QUERY_STRING, query);
        queryParameters.putSingle(QueryParameters.QUERY_NAME, queryName);
        queryParameters.putSingle(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, DefaultQueryParameters.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.putSingle(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        queryParameters.putSingle("valid", "param");
        queryParameters.putSingle(QueryParameters.QUERY_PARAMS, "auditType:NONE;auditColumnVisibility:A&B&C&D;page.size:75");

        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);

        QueryParameters qp = new DefaultQueryParameters();
        qp.validate(queryParameters);

        MultivaluedMap<String,String> op = new MultivaluedMapImpl<>();
        op.putAll(qp.getUnknownParameters(queryParameters));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());

        // Set expectations of the create logic
        queryLogic1.validate(queryParameters);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getMaxPageSize()).andReturn(1000).times(2);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.principal.getUserDN()).andReturn(userDNpair);
        expect(this.principal.getDNs()).andReturn(new String[] {userDN});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).andReturn(true);
        expect(this.queryLogic1.getAuditType(null)).andReturn(AuditType.NONE);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        expect(persister.create(eq(userDNpair.subjectDN()), eq(dnList), eq(marking), eq(queryLogicName), eq(qp), eq(op))).andReturn(this.query);
        expect(this.queryLogic1.getAuditType(this.query)).andReturn(AuditType.NONE);
        expect(this.queryLogic1.getConnectionPriority()).andReturn(Priority.NORMAL);
        expect(this.queryLogic1.getConnPoolName()).andReturn("connPool1");
        expect(this.queryLogic1.isLongRunningQuery()).andReturn(false);
        expect(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).andReturn(null);
        this.connectionRequestBean.requestBegin(queryId.toString(), userDN.toLowerCase(), null);
        expect(this.connectionFactory.getClient("connPool1", new ArrayList<>(), Priority.NORMAL, null)).andReturn(this.client);
        this.connectionRequestBean.requestEnd(queryId.toString());
        expect(this.traceInfos.get(userSid)).andReturn(new ArrayList<>(0));
        expect(this.traceInfos.get(null)).andReturn(Arrays.asList(PatternWrapper.wrap("NONMATCHING_REGEX")));
        expect(this.qlCache.add(queryId.toString(), userSid, this.queryLogic1, this.client)).andReturn(true);
        expect(this.queryLogic1.getCollectQueryMetrics()).andReturn(true);
        expect(this.principal.getPrimaryUser()).andReturn(dwUser);
        expect(this.dwUser.getAuths()).andReturn(Collections.singleton(queryAuthorizations));
        expect(this.principal.getProxiedUsers()).andReturn(Collections.singletonList(dwUser));
        expect(this.query.getOwner()).andReturn(userSid).anyTimes();
        expect(this.query.getId()).andReturn(queryId).anyTimes();
        expect(this.query.getQuery()).andReturn(queryName).anyTimes();
        expect(this.query.getQueryLogicName()).andReturn(queryLogicName).anyTimes();
        expect(this.query.getBeginDate()).andReturn(null).anyTimes();
        expect(this.query.getEndDate()).andReturn(null).anyTimes();
        expect(this.query.getColumnVisibility()).andReturn(null).anyTimes();
        expect(this.query.getQueryAuthorizations()).andReturn(queryAuthorizations).anyTimes();
        expect(this.query.getQueryName()).andReturn(null).anyTimes();
        expect(this.query.getPagesize()).andReturn(0).anyTimes();
        expect(this.query.getExpirationDate()).andReturn(null).anyTimes();
        expect(this.query.getParameters()).andReturn((Set) Collections.emptySet()).anyTimes();
        expect(this.query.getUncaughtExceptionHandler()).andReturn(new QueryUncaughtExceptionHandler()).anyTimes();
        this.metrics.updateMetric(isA(QueryMetric.class));
        PowerMock.expectLastCall().times(2);
        expect(this.query.getUserDN()).andReturn(userDN).anyTimes();
        expect(this.query.getDnList()).andReturn(dnList).anyTimes();
        expect(this.queryLogic1.getResultLimit(this.query)).andReturn(-1L);
        expect(this.queryLogic1.getMaxResults()).andReturn(-1L);
        expect(this.queryLogic1.initialize(eq(this.client), eq(this.query), isA(Set.class))).andReturn(this.genericConfiguration);
        this.queryLogic1.setupQuery(this.genericConfiguration);
        expect(this.queryLogic1.getTransformIterator(this.query)).andReturn(this.transformIterator);
        cache.put(eq(queryId.toString()), isA(RunningQuery.class));
        expect(this.genericConfiguration.getQueryString()).andReturn(queryName).once();
        expect(this.qlCache.poll(queryId.toString())).andReturn(null);

        // Set expectations of the next logic
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.context.getUserTransaction()).andReturn(this.transaction).anyTimes();
        expect(this.transaction.getStatus()).andReturn(Status.STATUS_NO_TRANSACTION).anyTimes();
        cache.unlock("{id}");
        PowerMock.expectLastCall().anyTimes();
        this.transaction.begin();
        this.transaction.setRollbackOnly();
        PowerMock.expectLastCall().anyTimes();

        expect(this.responseObjectFactory.getEventQueryResponse()).andReturn(new DefaultEventQueryResponse());
        this.transaction.commit();
        QueryExecutorBean subject = new QueryExecutorBean();
        // Run the test
        PowerMock.replayAll();

        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactoryImpl.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, QueryMetricsBean.class, metrics);
        setInternalState(subject, Multimap.class, traceInfos);
        setInternalState(subject, SecurityMarking.class, new ColumnVisibilitySecurityMarking());
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);

        Throwable result1 = null;
        try {
            subject.createQuery(queryLogicName, queryParameters);
        } catch (Exception e) {
            result1 = e.getCause();
            assertTrue(e.getCause().toString().contains("datawave.webservice.query.exception.BadRequestQueryException: Invalid page size."));
            assertEquals(e.getMessage(), "HTTP 400 Bad Request");
            assertTrue(((BadRequestQueryException) result1).getErrorCode().equals(errorCode));
        }

        assertNotNull("Expected a non-null response", result1);
    }

    @Test
    public void testCreateQueryAndNext_PageSizeParamTwo() throws Exception {
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
        String errorCode = DatawaveErrorCode.INVALID_PAGE_SIZE.getErrorCode();
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
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, DefaultQueryParameters.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        // If the wrong page size parameter is added here, it should be dropped automatically by the QueryImpl
        queryParameters.putSingle("page.size", String.valueOf(pagesize));
        queryParameters.putSingle(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        queryParameters.putSingle("valid", "param");

        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);

        QueryParameters qp = new DefaultQueryParameters();
        qp.validate(queryParameters);

        MultivaluedMap<String,String> op = new MultivaluedMapImpl<>();
        op.putAll(qp.getUnknownParameters(queryParameters));
        // op.putSingle(PrivateAuditConstants.AUDIT_TYPE, AuditType.NONE.name());
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());

        // Set expectations of the create logic
        queryLogic1.validate(queryParameters);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getMaxPageSize()).andReturn(1000).times(2);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.principal.getUserDN()).andReturn(userDNpair);
        expect(this.principal.getDNs()).andReturn(new String[] {userDN});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).andReturn(true);
        expect(this.queryLogic1.getAuditType(null)).andReturn(AuditType.NONE);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        expect(persister.create(eq(userDNpair.subjectDN()), eq(dnList), eq(marking), eq(queryLogicName), eq(qp), eq(op))).andReturn(this.query);
        expect(this.queryLogic1.getAuditType(this.query)).andReturn(AuditType.NONE);
        expect(this.queryLogic1.getConnectionPriority()).andReturn(Priority.NORMAL);
        expect(this.queryLogic1.getConnPoolName()).andReturn("connPool1");
        expect(this.queryLogic1.isLongRunningQuery()).andReturn(false);
        expect(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).andReturn(null);
        this.connectionRequestBean.requestBegin(queryId.toString(), userDN.toLowerCase(), null);
        expect(this.connectionFactory.getClient("connPool1", new ArrayList<>(), Priority.NORMAL, null)).andReturn(this.client);
        this.connectionRequestBean.requestEnd(queryId.toString());
        expect(this.traceInfos.get(userSid)).andReturn(new ArrayList<>(0));
        expect(this.traceInfos.get(null)).andReturn(Arrays.asList(PatternWrapper.wrap("NONMATCHING_REGEX")));
        expect(this.qlCache.add(queryId.toString(), userSid, this.queryLogic1, this.client)).andReturn(true);
        expect(this.queryLogic1.getCollectQueryMetrics()).andReturn(true);
        expect(this.principal.getPrimaryUser()).andReturn(dwUser);
        expect(this.dwUser.getAuths()).andReturn(Collections.singleton(queryAuthorizations));
        expect(this.principal.getProxiedUsers()).andReturn(Collections.singletonList(dwUser));
        expect(this.query.getOwner()).andReturn(userSid).anyTimes();
        expect(this.query.getId()).andReturn(queryId).anyTimes();
        expect(this.query.getQuery()).andReturn(queryName).anyTimes();
        expect(this.query.getQueryLogicName()).andReturn(queryLogicName).anyTimes();
        expect(this.query.getBeginDate()).andReturn(null).anyTimes();
        expect(this.query.getEndDate()).andReturn(null).anyTimes();
        expect(this.query.getColumnVisibility()).andReturn(null).anyTimes();
        expect(this.query.getQueryAuthorizations()).andReturn(queryAuthorizations).anyTimes();
        expect(this.query.getQueryName()).andReturn(null).anyTimes();
        expect(this.query.getPagesize()).andReturn(0).anyTimes();
        expect(this.query.getExpirationDate()).andReturn(null).anyTimes();
        expect(this.query.getParameters()).andReturn((Set) Collections.emptySet()).anyTimes();
        expect(this.query.getUncaughtExceptionHandler()).andReturn(new QueryUncaughtExceptionHandler()).anyTimes();
        this.metrics.updateMetric(isA(QueryMetric.class));
        PowerMock.expectLastCall().times(2);
        expect(this.query.getUserDN()).andReturn(userDN).anyTimes();
        expect(this.query.getDnList()).andReturn(dnList).anyTimes();
        expect(this.queryLogic1.getResultLimit(this.query)).andReturn(-1L);
        expect(this.queryLogic1.getMaxResults()).andReturn(-1L);
        expect(this.queryLogic1.initialize(eq(this.client), eq(this.query), isA(Set.class))).andReturn(this.genericConfiguration);
        this.queryLogic1.setupQuery(this.genericConfiguration);
        expect(this.queryLogic1.getTransformIterator(this.query)).andReturn(this.transformIterator);
        cache.put(eq(queryId.toString()), isA(RunningQuery.class));
        expect(this.genericConfiguration.getQueryString()).andReturn(queryName).once();
        expect(this.qlCache.poll(queryId.toString())).andReturn(null);

        // Set expectations of the next logic
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.context.getUserTransaction()).andReturn(this.transaction).anyTimes();

        this.transaction.begin();
        expect(this.cache.get(queryId.toString())).andReturn(this.runningQuery);
        expect(cache.lock(queryId.toString())).andReturn(true);
        expect(this.runningQuery.getSettings()).andReturn(this.query);
        expect(this.runningQuery.getClient()).andReturn(this.client);

        this.runningQuery.setActiveCall(true);
        expectLastCall();

        expect(this.runningQuery.next()).andReturn(this.resultsPage);
        expect(this.runningQuery.getLastPageNumber()).andReturn(pageNumber);
        expect(this.runningQuery.getLogic()).andReturn((QueryLogic) this.queryLogic1).times(2);
        expect(this.runningQuery.getSettings()).andReturn(this.query).anyTimes();
        expect(this.queryLogic1.getEnrichedTransformer(this.query)).andReturn(this.transformer);
        expect(this.transformer.createResponse(this.resultsPage)).andReturn(this.baseResponse);
        expect(this.resultsPage.getStatus()).andReturn(ResultsPage.Status.COMPLETE).times(2);
        this.baseResponse.setHasResults(true);
        this.baseResponse.setPageNumber(pageNumber);
        expect(this.queryLogic1.getLogicName()).andReturn(queryLogicName);
        this.baseResponse.setLogicName(queryLogicName);
        this.baseResponse.setQueryId(queryId.toString());
        expect(this.runningQuery.getMetric()).andReturn(this.queryMetric);
        this.runningQuery.setActiveCall(false);
        expectLastCall();
        this.queryMetric.setProxyServers(eq(new ArrayList<>(0)));
        expect(this.responseObjectFactory.getEventQueryResponse()).andReturn(new DefaultEventQueryResponse());

        cache.unlock(queryId.toString());
        expect(this.transaction.getStatus()).andReturn(Status.STATUS_ACTIVE).anyTimes();
        this.transaction.commit();
        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactoryImpl.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, QueryMetricsBean.class, metrics);
        setInternalState(subject, Multimap.class, traceInfos);
        setInternalState(subject, SecurityMarking.class, new ColumnVisibilitySecurityMarking());
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);
        Throwable result1 = null;
        try {
            subject.createQuery(queryLogicName, queryParameters);
        } catch (Exception e) {
            result1 = e.getCause();
            assertTrue(e.getCause().toString().contains("datawave.webservice.query.exception.BadRequestQueryException: Invalid page size."));
            assertEquals(e.getMessage(), "HTTP 400 Bad Request");
            assertTrue(((BadRequestQueryException) result1).getErrorCode().equals(errorCode));
        }

        assertNotNull("Expected a non-null response", result1);

        // Verify results
        assertNotNull("Expected a non-null response", result1);
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
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, DefaultQueryParameters.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expirationDate));
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

        QueryParameters qp = new DefaultQueryParameters();
        qp.validate(queryParameters);

        MultivaluedMap<String,String> op = MapUtils.toMultivaluedMap(qp.getUnknownParameters(MapUtils.toMultiValueMap(queryParameters)));
        // op.putSingle(PrivateAuditConstants.AUDIT_TYPE, AuditType.NONE.name());
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());

        // Set expectations of the create logic
        queryLogic1.validate(queryParameters);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getMaxPageSize()).andReturn(1000).times(2);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.principal.getUserDN()).andReturn(userDNpair);
        expect(this.principal.getDNs()).andReturn(new String[] {userDN});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).andReturn(true);
        expect(this.queryLogic1.getAuditType(null)).andReturn(AuditType.NONE);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        expect(this.principal.getPrimaryUser()).andReturn(dwUser).anyTimes();
        expect(this.dwUser.getAuths()).andReturn(Collections.singleton(queryAuthorizations)).anyTimes();
        expect(this.principal.getProxiedUsers()).andReturn(Collections.singletonList(dwUser));
        expect(this.userOperations.getRemoteUser(this.principal)).andReturn(this.principal);
        expect(persister.create(eq(userDNpair.subjectDN()), eq(dnList), eq(marking), eq(queryLogicName), eq(qp), eq(op))).andReturn(this.query);
        expect(this.queryLogic1.getAuditType(this.query)).andReturn(AuditType.NONE);
        expect(this.queryLogic1.getConnectionPriority()).andReturn(Priority.NORMAL);
        expect(this.queryLogic1.getConnPoolName()).andReturn("connPool1");
        expect(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).andReturn(null);
        this.query.populateTrackingMap(null);
        expect(this.connectionFactory.getClient(userDN.toLowerCase(), new ArrayList<>(0), "connPool1", Priority.NORMAL, null)).andReturn(this.client);
        expect(this.traceInfos.get(userSid)).andReturn(new ArrayList<>(0));
        expect(this.traceInfos.get(null)).andReturn(Arrays.asList(PatternWrapper.wrap("NONMATCHING_REGEX")));
        expect(this.qlCache.add(queryId.toString(), userSid, this.queryLogic1, this.client)).andReturn(true);
        expect(this.queryLogic1.getCollectQueryMetrics()).andReturn(true);
        expect(this.query.getOwner()).andReturn(userSid).anyTimes();
        expect(this.query.getId()).andReturn(queryId).anyTimes();
        expect(this.query.getQuery()).andReturn(queryName).anyTimes();
        expect(this.query.getQueryLogicName()).andReturn(queryLogicName).anyTimes();
        expect(this.query.getBeginDate()).andReturn(null).anyTimes();
        expect(this.query.getEndDate()).andReturn(null).anyTimes();
        expect(this.query.getColumnVisibility()).andReturn(null).anyTimes();
        expect(this.query.getQueryAuthorizations()).andReturn(queryAuthorizations).anyTimes();
        expect(this.query.getQueryName()).andReturn(null).anyTimes();
        expect(this.query.getPagesize()).andReturn(0).anyTimes();
        expect(this.query.getPageTimeout()).andReturn(-1).anyTimes();
        expect(this.query.getExpirationDate()).andReturn(null).anyTimes();
        expect(this.query.getParameters()).andReturn((Set) Collections.emptySet()).anyTimes();
        expect(this.query.getUncaughtExceptionHandler()).andReturn(new QueryUncaughtExceptionHandler()).anyTimes();
        expect(this.query.findParameter(RemoteUserOperationsImpl.INCLUDE_REMOTE_SERVICES))
                        .andReturn(new QueryImpl.Parameter(RemoteUserOperationsImpl.INCLUDE_REMOTE_SERVICES, "true")).anyTimes();
        this.metrics.updateMetric(isA(QueryMetric.class));
        PowerMock.expectLastCall().times(2);
        expect(this.query.getUserDN()).andReturn(userDN).anyTimes();
        expect(this.query.getDnList()).andReturn(dnList).anyTimes();
        expect(this.queryLogic1.isLongRunningQuery()).andReturn(false);
        expect(this.queryLogic1.getResultLimit(this.query)).andReturn(-1L);
        expect(this.queryLogic1.getMaxResults()).andReturn(-1L);
        this.queryLogic1.preInitialize(this.query, WSAuthorizationsUtil.buildAuthorizations(Collections.singleton(Sets.newHashSet("AUTH_1"))));
        expect(this.queryLogic1.getUserOperations()).andReturn(null);
        expect(this.queryLogic1.initialize(eq(this.client), eq(this.query), isA(Set.class))).andReturn(this.genericConfiguration);
        this.queryLogic1.setupQuery(this.genericConfiguration);
        expect(this.queryLogic1.getTransformIterator(this.query)).andReturn(this.transformIterator);
        expect(this.genericConfiguration.getQueryString()).andReturn(queryName).once();
        cache.put(eq(queryId.toString()), isA(RunningQuery.class));
        expect(this.qlCache.poll(queryId.toString())).andReturn(null);

        // Set expectations of the next logic
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.context.getUserTransaction()).andReturn(this.transaction).anyTimes();

        this.transaction.begin();
        expect(this.cache.get(queryId.toString())).andReturn(this.runningQuery);
        expect(cache.lock(queryId.toString())).andReturn(true);
        expect(this.runningQuery.getSettings()).andReturn(this.query);
        this.connectionRequestBean.requestBegin(queryId.toString(), userDN.toLowerCase(), null);
        expect(this.runningQuery.getClient()).andReturn(this.client);
        this.connectionRequestBean.requestEnd(queryId.toString());

        this.runningQuery.setActiveCall(true);
        expectLastCall();

        // expect(this.runningQuery.getTraceInfo()).andReturn(this.traceInfo);
        expect(this.runningQuery.next()).andReturn(this.resultsPage);
        expect(this.runningQuery.getLastPageNumber()).andReturn(pageNumber);
        expect(this.runningQuery.getLogic()).andReturn((QueryLogic) this.queryLogic1).times(2);
        expect(this.runningQuery.getSettings()).andReturn(this.query).anyTimes();
        expect(this.queryLogic1.getEnrichedTransformer(this.query)).andReturn(this.transformer);
        expect(this.transformer.createResponse(this.resultsPage)).andReturn(this.baseResponse);
        expect(this.resultsPage.getStatus()).andReturn(ResultsPage.Status.COMPLETE).times(2);
        this.baseResponse.setHasResults(true);
        this.baseResponse.setPageNumber(pageNumber);
        expect(this.queryLogic1.getLogicName()).andReturn(queryLogicName);
        this.baseResponse.setLogicName(queryLogicName);
        this.baseResponse.setQueryId(queryId.toString());
        expect(this.runningQuery.getMetric()).andReturn(this.queryMetric);
        this.runningQuery.setActiveCall(false);
        expectLastCall();
        this.queryMetric.setProxyServers(eq(new ArrayList<>(0)));
        expect(this.responseObjectFactory.getEventQueryResponse()).andReturn(new DefaultEventQueryResponse());

        cache.unlock(queryId.toString());
        expect(this.transaction.getStatus()).andReturn(Status.STATUS_ACTIVE).anyTimes();
        this.transaction.commit();

        expect(this.responseObjectFactory.getQueryImpl()).andReturn(new QueryImpl());
        expect(queryLogic1.getResultLimit(anyObject(QueryImpl.class))).andReturn(-1L);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, UserOperationsBean.class, userOperations);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactoryImpl.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, QueryMetricsBean.class, metrics);
        setInternalState(subject, Multimap.class, traceInfos);
        setInternalState(subject, SecurityMarking.class, new ColumnVisibilitySecurityMarking());
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);
        BaseQueryResponse result1 = subject.createQueryAndNext(queryLogicName, queryParameters);
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a non-null response", result1);
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

        MultiValueMap<String,String> queryParameters = new LinkedMultiValueMap<>();
        queryParameters.set(QueryParameters.QUERY_STRING, query);
        queryParameters.set(QueryParameters.QUERY_NAME, queryName);
        queryParameters.set(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        queryParameters.set(QueryParameters.QUERY_BEGIN, DefaultQueryParameters.formatDate(beginDate));
        queryParameters.set(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        queryParameters.set(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expirationDate));
        queryParameters.set(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.set(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.set(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.set(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.set(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.set("valid", "param");
        queryParameters.set(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);

        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);

        QueryParameters qp = new DefaultQueryParameters();
        qp.validate(queryParameters);

        MultivaluedMap<String,String> op = MapUtils.toMultivaluedMap(qp.getUnknownParameters(queryParameters));
        // op.putSingle(PrivateAuditConstants.AUDIT_TYPE, AuditType.ACTIVE.name());
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());

        // Set expectations
        expect(context.getCallerPrincipal()).andReturn(principal);
        queryLogic1.validate(queryParameters);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getMaxPageSize()).andReturn(1000).times(2);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal);
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.principal.getUserDN()).andReturn(userDNpair);
        expect(this.principal.getDNs()).andReturn(new String[] {userDN});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0));
        expect(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).andReturn(true);
        expect(this.queryLogic1.getAuditType(null)).andReturn(AuditType.ACTIVE);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        expect(persister.create(eq(userDNpair.subjectDN()), eq(dnList), eq(marking), eq(queryLogicName), eq(qp), eq(op))).andReturn(this.query);
        expect(this.queryLogic1.getAuditType(this.query)).andReturn(AuditType.ACTIVE);
        expect(this.queryLogic1.getSelectors(this.query)).andReturn(null);
        expect(auditor.audit(anyObject())).andReturn(null);
        expect(this.queryLogic1.getConnectionPriority()).andReturn(Priority.NORMAL);
        expect(this.queryLogic1.getConnPoolName()).andReturn("connPool1");
        expect(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).andReturn(null);
        this.query.populateTrackingMap(null);
        this.connectionRequestBean.requestBegin(queryId.toString(), userDN.toLowerCase(), null);
        expect(this.connectionFactory.getClient(userDN.toLowerCase(), new ArrayList<>(0), "connPool1", Priority.NORMAL, null)).andReturn(this.client);
        this.connectionRequestBean.requestEnd(queryId.toString());
        expect(this.traceInfos.get(userSid)).andReturn(Arrays.asList(PatternWrapper.wrap(query)));
        expect(this.qlCache.add(queryId.toString(), userSid, this.queryLogic1, this.client))
                        .andThrow(new IllegalStateException("INTENTIONALLY THROWN TEST EXCEPTION: PROBLEM ADDING QUERY LOGIC TO CACHE"));
        this.queryLogic1.close();
        this.connectionFactory.returnClient(this.client);
        PowerMock.expectLastCall().andThrow(new IOException("INTENTIONALLY THROWN 2ND-LEVEL TEST EXCEPTION"));
        this.persister.remove(this.query);
        PowerMock.expectLastCall().andThrow(new IOException("INTENTIONALLY THROWN 3RD-LEVEL TEST EXCEPTION"));
        expect(this.query.getId()).andReturn(queryId).anyTimes();
        expect(this.qlCache.poll(queryId.toString())).andReturn(null);

        expect(this.responseObjectFactory.getQueryImpl()).andReturn(new QueryImpl());
        expect(queryLogic1.getResultLimit(anyObject(QueryImpl.class))).andReturn(-1L);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, Multimap.class, traceInfos);
        setInternalState(subject, SecurityMarking.class, marking);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);
        Throwable result1 = null;
        try {
            subject.createQueryAndNext(queryLogicName, MapUtils.toMultivaluedMap(queryParameters));
        } catch (DatawaveWebApplicationException e) {
            result1 = e.getCause();
        }
        PowerMock.verifyAll();

        // Verify results
        assertTrue("QueryException expected to have been thrown", result1 instanceof QueryException);
        assertEquals("Exception expected to have been caused by problem adding query logic to cache", "500-7", ((QueryException) result1).getErrorCode());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test(expected = NoResultsException.class)
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
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, DefaultQueryParameters.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.putSingle(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        queryParameters.putSingle("valid", "param");

        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);

        QueryParameters qp = new DefaultQueryParameters();
        qp.validate(queryParameters);

        MultivaluedMap<String,String> op = MapUtils.toMultivaluedMap(qp.getUnknownParameters(MapUtils.toMultiValueMap(queryParameters)));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());

        // Set expectations of the create logic
        queryLogic1.validate(queryParameters);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getMaxPageSize()).andReturn(1000).times(2);
        expect(this.queryLogic1.getConnPoolName()).andReturn("connPool1");
        expect(this.context.getCallerPrincipal()).andReturn(this.principal);
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.principal.getUserDN()).andReturn(userDNpair);
        expect(this.principal.getDNs()).andReturn(new String[] {userDN});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).andReturn(true);
        expect(this.queryLogic1.getAuditType(null)).andReturn(AuditType.NONE);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        expect(this.principal.getPrimaryUser()).andReturn(dwUser).anyTimes();
        expect(this.dwUser.getAuths()).andReturn(Collections.singleton(queryAuthorizations)).anyTimes();
        expect(this.principal.getProxiedUsers()).andReturn(Collections.singletonList(dwUser));
        expect(this.userOperations.getRemoteUser(this.principal)).andReturn(this.principal);
        expect(persister.create(eq(userDNpair.subjectDN()), eq(dnList), eq(marking), eq(queryLogicName), eq(qp), eq(op))).andReturn(this.query);
        expect(this.queryLogic1.getAuditType(this.query)).andReturn(AuditType.NONE);
        expect(this.queryLogic1.getConnectionPriority()).andReturn(Priority.NORMAL);
        expect(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).andReturn(null);
        this.query.populateTrackingMap(null);
        expect(this.traceInfos.get(userSid)).andReturn(new ArrayList<>(0));
        expect(this.traceInfos.get(null)).andReturn(Arrays.asList(PatternWrapper.wrap("NONMATCHING_REGEX")));
        expect(this.qlCache.add(queryId.toString(), userSid, this.queryLogic1, this.client)).andReturn(true);
        expect(this.queryLogic1.getCollectQueryMetrics()).andReturn(true);
        expect(this.query.getOwner()).andReturn(userSid).anyTimes();
        expect(this.query.getId()).andReturn(queryId).anyTimes();
        expect(this.query.getQuery()).andReturn(queryName).anyTimes();
        expect(this.query.getQueryLogicName()).andReturn(queryLogicName).anyTimes();
        expect(this.query.getBeginDate()).andReturn(null).anyTimes();
        expect(this.query.getEndDate()).andReturn(null).anyTimes();
        expect(this.query.getQueryAuthorizations()).andReturn(queryAuthorizations).anyTimes();
        expect(this.query.getQueryName()).andReturn(null).anyTimes();
        expect(this.query.getColumnVisibility()).andReturn(null).anyTimes();
        expect(this.query.getPagesize()).andReturn(0).anyTimes();
        expect(this.query.getPageTimeout()).andReturn(-1).anyTimes();
        expect(this.query.getExpirationDate()).andReturn(null).anyTimes();
        expect(this.query.getParameters()).andReturn((Set) Collections.emptySet()).anyTimes();
        expect(this.query.getUncaughtExceptionHandler()).andReturn(new QueryUncaughtExceptionHandler()).anyTimes();
        expect(this.query.findParameter(RemoteUserOperationsImpl.INCLUDE_REMOTE_SERVICES))
                        .andReturn(new QueryImpl.Parameter(RemoteUserOperationsImpl.INCLUDE_REMOTE_SERVICES, "true")).anyTimes();
        this.metrics.updateMetric(isA(QueryMetric.class));
        PowerMock.expectLastCall().times(2);
        expect(this.query.getUserDN()).andReturn(userDN).anyTimes();
        expect(this.query.getDnList()).andReturn(dnList).anyTimes();
        expect(this.queryLogic1.isLongRunningQuery()).andReturn(false);
        expect(this.queryLogic1.getResultLimit(this.query)).andReturn(-1L);
        expect(this.queryLogic1.getMaxResults()).andReturn(-1L);
        expect(this.queryLogic1.initialize(eq(this.client), eq(this.query), isA(Set.class))).andReturn(this.genericConfiguration);
        this.queryLogic1.setupQuery(this.genericConfiguration);
        expect(this.queryLogic1.getTransformIterator(this.query)).andReturn(this.transformIterator);
        expect(this.genericConfiguration.getQueryString()).andReturn(queryName).once();
        this.cache.put(eq(queryId.toString()), isA(RunningQuery.class));
        expect(this.qlCache.poll(queryId.toString())).andReturn(null);

        // Set expectations of the next logic
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.context.getUserTransaction()).andReturn(this.transaction).anyTimes();
        this.transaction.begin();
        expect(this.cache.get(queryId.toString())).andReturn(this.runningQuery);
        expect(cache.lock(queryId.toString())).andReturn(true);
        expect(this.runningQuery.getSettings()).andReturn(this.query).anyTimes();
        this.connectionRequestBean.requestBegin(queryId.toString(), userDN.toLowerCase(), null);
        expect(this.runningQuery.getClient()).andReturn(this.client);
        this.connectionRequestBean.requestEnd(queryId.toString());
        this.runningQuery.setActiveCall(true);
        expectLastCall();
        // expect(this.runningQuery.getTraceInfo()).andReturn(this.traceInfo);
        expect(this.runningQuery.next()).andReturn(this.resultsPage);
        expect(this.runningQuery.getLastPageNumber()).andReturn(pageNumber);
        expect(this.runningQuery.getLogic()).andReturn((QueryLogic) this.queryLogic1).times(2);
        expect(this.queryLogic1.getEnrichedTransformer(this.query)).andReturn(this.transformer);
        expect(this.transformer.createResponse(this.resultsPage)).andReturn(this.baseResponse);
        expect(this.resultsPage.getStatus()).andReturn(ResultsPage.Status.NONE).times(2);
        this.baseResponse.setHasResults(false);
        this.baseResponse.setPageNumber(pageNumber);
        expect(this.queryLogic1.getLogicName()).andReturn(queryLogicName);
        this.baseResponse.setLogicName(queryLogicName);
        this.baseResponse.setQueryId(queryId.toString());
        expect(this.runningQuery.getMetric()).andReturn(this.queryMetric).times(2);
        this.runningQuery.setActiveCall(false);
        expectLastCall();
        this.queryMetric.setProxyServers(eq(new ArrayList<>(0)));
        this.baseResponse.addException(isA(NoResultsQueryException.class));

        expect(this.runningQuery.getLogic()).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getCollectQueryMetrics()).andReturn(true);
        this.queryLogic1.preInitialize(this.query, WSAuthorizationsUtil.buildAuthorizations(Collections.singleton(Sets.newHashSet("AUTH_1"))));
        expect(this.queryLogic1.getUserOperations()).andReturn(null);

        this.metrics.updateMetric(this.queryMetric);
        cache.unlock(queryId.toString());
        this.transaction.setRollbackOnly();
        expect(this.transaction.getStatus()).andReturn(Status.STATUS_ACTIVE).anyTimes();
        this.transaction.commit();

        // Set expectations
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName).anyTimes();
        expect(this.principal.getShortName()).andReturn(userSid).anyTimes();
        expect(this.principal.getUserDN()).andReturn(SubjectIssuerDNPair.of(userDN));
        expect(this.principal.getDNs()).andReturn(new String[] {userDN});
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations))).anyTimes();
        expect(this.connectionRequestBean.cancelConnectionRequest(queryId.toString(), userDN.toLowerCase())).andReturn(false);
        expect(this.qlCache.pollIfOwnedBy(queryId.toString(), userSid)).andReturn(null);
        expect(this.cache.get(queryId.toString())).andReturn(this.runningQuery);
        expect(this.connectionFactory.getClient(userDN.toLowerCase(), new ArrayList<>(0), "connPool1", Priority.NORMAL, null)).andReturn(this.client);
        this.runningQuery.closeConnection(this.connectionFactory);
        this.cache.remove(queryId.toString());
        this.closedCache.add(queryId.toString());
        this.closedCache.remove(queryId.toString());
        // expect(this.runningQuery.getTraceInfo()).andReturn(null);
        expect(this.responseObjectFactory.getEventQueryResponse()).andReturn(new DefaultEventQueryResponse());

        expect(this.responseObjectFactory.getQueryImpl()).andReturn(new QueryImpl());
        expect(queryLogic1.getResultLimit(anyObject(QueryImpl.class))).andReturn(-1L);

        // Run the test
        PowerMock.replayAll();
        try {
            QueryExecutorBean subject = new QueryExecutorBean();
            setInternalState(subject, EJBContext.class, context);
            setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
            setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
            setInternalState(subject, UserOperationsBean.class, userOperations);
            setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
            setInternalState(subject, QueryCache.class, cache);
            setInternalState(subject, ClosedQueryCache.class, closedCache);
            setInternalState(subject, Persister.class, persister);
            setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
            setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
            setInternalState(subject, AuditBean.class, auditor);
            setInternalState(subject, QueryMetricsBean.class, metrics);
            setInternalState(subject, Multimap.class, traceInfos);
            setInternalState(subject, SecurityMarking.class, new ColumnVisibilitySecurityMarking());
            setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
            setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
            setInternalState(connectionRequestBean, EJBContext.class, context);
            setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);

            subject.createQueryAndNext(queryLogicName, queryParameters);
        } finally {
            PowerMock.verifyAll();
        }
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
        String systemFrom = null;
        String parameters = null;
        boolean trace = false;

        MultivaluedMap<String,String> p = MapUtils.toMultivaluedMap(
                        DefaultQueryParameters.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                                        expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, systemFrom, parameters, trace));

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        Throwable result1 = null;
        try {

            subject.createQueryAndNext(queryLogicName, p);

        } catch (DatawaveWebApplicationException e) {
            result1 = e;
        }
        PowerMock.verifyAll();

        // Verify results
        assertTrue("BadRequestException expected to have been thrown", result1 instanceof BadRequestException);
        assertEquals("Thrown exception expected to have been due to invalid expiration date", "400-3", ((QueryException) result1.getCause()).getErrorCode());
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
        String systemFrom = null;
        String parameters = null;
        boolean trace = false;

        MultivaluedMap<String,String> p = MapUtils.toMultivaluedMap(
                        DefaultQueryParameters.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                                        expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, systemFrom, parameters, trace));

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        Throwable result1 = null;
        try {
            subject.createQueryAndNext(queryLogicName, p);

        } catch (DatawaveWebApplicationException e) {
            result1 = e;
        }
        PowerMock.verifyAll();

        // Verify results
        assertTrue("BadRequestException expected to have been thrown", result1 instanceof BadRequestException);
        assertEquals("Thrown exception expected to have been due to invalid page size", "400-2", ((QueryException) result1.getCause()).getErrorCode());
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
        String systemFrom = null;
        String parameters = null;
        boolean trace = false;
        // Set expectations

        MultivaluedMap<String,String> queryParameters = MapUtils.toMultivaluedMap(
                        DefaultQueryParameters.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                                        expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, systemFrom, parameters, trace));

        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);

        QueryParameters qp = new DefaultQueryParameters();
        qp.validate(queryParameters);

        MultivaluedMap<String,String> op = MapUtils.toMultivaluedMap(qp.getUnknownParameters(MapUtils.toMultiValueMap(queryParameters)));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();

        queryLogic1.validate(queryParameters);
        expect(this.principal.getName()).andReturn("userName Full");
        expect(this.principal.getShortName()).andReturn("userName");
        expect(this.principal.getUserDN()).andReturn(SubjectIssuerDNPair.of("userDN"));
        expect(this.principal.getDNs()).andReturn(new String[] {"userDN"});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.queryLogic1.containsDNWithAccess(Collections.singletonList("userDN"))).andReturn(true);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        expect(this.queryLogic1.getMaxPageSize()).andReturn(10).times(4);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, SecurityMarking.class, new ColumnVisibilitySecurityMarking());
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        Throwable result1 = null;
        try {
            subject.createQueryAndNext(queryLogicName, queryParameters);
        } catch (DatawaveWebApplicationException e) {
            result1 = e.getCause();
        }
        PowerMock.verifyAll();

        // Verify results
        assertTrue("QueryException expected to have been thrown", result1 instanceof QueryException);
        assertEquals("Thrown exception expected to have been due to undefined query logic", "400-6", ((QueryException) result1).getErrorCode());
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
        String systemFrom = null;
        String parameters = null;
        boolean trace = false;

        // Set expectations
        expect(context.getCallerPrincipal()).andReturn(principal);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, principal))
                        .andThrow(new IllegalArgumentException("INTENTIONALLY THROWN TEST EXCEPTION: UNDEFINED QUERY LOGIC"));

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        Throwable result1 = null;
        try {
            MultivaluedMap<String,String> queryParameters = MapUtils.toMultivaluedMap(
                            DefaultQueryParameters.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                                            expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, systemFrom, parameters, trace));

            subject.createQueryAndNext(queryLogicName, queryParameters);

        } catch (DatawaveWebApplicationException e) {
            result1 = e.getCause();
        }
        PowerMock.verifyAll();

        // Verify results
        assertTrue("QueryException expected to have been thrown", result1 instanceof QueryException);
        assertTrue("Thrown exception expected to have been due to undefined query logic",
                        result1.getCause().getMessage().toLowerCase().contains("undefined query logic"));
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
        String systemFrom = null;
        String parameters = null;
        boolean trace = false;

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        Throwable result1 = null;
        try {
            MultivaluedMap<String,String> queryParameters = MapUtils.toMultivaluedMap(
                            DefaultQueryParameters.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                                            expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, systemFrom, parameters, trace));

            subject.defineQuery(queryLogicName, queryParameters);

        } catch (DatawaveWebApplicationException e) {
            result1 = e;
        }
        PowerMock.verifyAll();

        // Verify results
        assertTrue("BadRequestException expected to have been thrown", result1 instanceof BadRequestException);
        assertEquals("Thrown exception expected to have been due to invalid expiration date", "400-3", ((QueryException) result1.getCause()).getErrorCode());
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
        String systemFrom = null;
        String parameters = null;

        boolean trace = false;

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        Throwable result1 = null;
        try {
            MultivaluedMap<String,String> queryParameters = MapUtils.toMultivaluedMap(
                            DefaultQueryParameters.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                                            expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, systemFrom, parameters, trace));

            subject.defineQuery(queryLogicName, queryParameters);
        } catch (DatawaveWebApplicationException e) {
            result1 = e;
        }
        PowerMock.verifyAll();

        // Verify results
        assertTrue("BadRequestException expected to have been thrown", result1 instanceof BadRequestException);
        assertEquals("Thrown exception expected to have been due to invalid page size", "400-2", ((QueryException) result1.getCause()).getErrorCode());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test(expected = IllegalArgumentException.class)
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
        String userDN = "userDN";
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);
        boolean trace = false;
        List<String> dnList = Collections.singletonList(userDN);
        UUID queryId = UUID.randomUUID();

        MultivaluedMap<String,String> queryParameters = MapUtils
                        .toMultivaluedMap(DefaultQueryParameters.paramsToMap(null, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                                        expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, null, null, trace));
        queryParameters.putSingle("valid", "param");

        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);

        QueryParameters qp = new DefaultQueryParameters();
        qp.validate(queryParameters);

        MultivaluedMap<String,String> op = MapUtils.toMultivaluedMap(qp.getUnknownParameters(MapUtils.toMultiValueMap(queryParameters)));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());

        // Set expectations
        queryLogic1.validate(queryParameters);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.principal.getDNs()).andReturn(new String[] {userDN});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0));
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        expect(this.principal.getUserDN()).andReturn(userDNpair);
        expect(persister.create(eq(userDNpair.subjectDN()), eq(dnList), eq(marking), eq(queryLogicName), eq(qp), eq(op))).andReturn(this.query);
        expect(this.query.getId()).andReturn(queryId).times(3);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getMaxPageSize()).andReturn(100).times(2);
        expect(this.traceInfos.get(userSid)).andReturn(new ArrayList<>(0));
        expect(this.traceInfos.get(null)).andReturn(Arrays.asList(PatternWrapper.wrap(query)));
        expect(this.queryLogic1.getConnectionPriority()).andThrow(ILLEGAL_STATE_EXCEPTION);

        // Run the test
        PowerMock.replayAll();
        try {
            QueryExecutorBean subject = new QueryExecutorBean();
            setInternalState(subject, EJBContext.class, context);
            setInternalState(subject, Persister.class, persister);
            setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
            setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
            setInternalState(subject, Multimap.class, traceInfos);
            setInternalState(subject, SecurityMarking.class, new ColumnVisibilitySecurityMarking());
            setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
            setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
            subject.defineQuery(queryLogicName, queryParameters);
        } finally {
            PowerMock.verifyAll();
        }
    }

    @Test
    public void testDisableAllTracing_HappyPath() throws Exception {
        // Set expectations
        this.traceInfos.clear();
        this.traceCache.put("traceInfos", this.traceInfos);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, Multimap.class, traceInfos);
        setInternalState(subject, QueryTraceCache.class, traceCache);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        VoidResponse result1 = subject.disableAllTracing();
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a non-null response", result1);
    }

    @Test
    public void testDisableTracing_NullChecksAndHappyPath() throws Exception {
        // Set local test input
        String user = "user";
        String queryRegex = "queryRegex";

        // Set expectations
        expect(this.traceInfos.removeAll(user)).andReturn(new ArrayList<>(0));
        this.traceCache.put("traceInfos", this.traceInfos);
        expectLastCall().times(2);
        expect(this.traceInfos.remove(eq(user), notNull())).andReturn(true);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, Multimap.class, traceInfos);
        setInternalState(subject, QueryTraceCache.class, traceCache);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        Exception result1 = null;
        try {
            subject.disableTracing(null, null);
        } catch (BadRequestException e) {
            result1 = e;
        }
        VoidResponse result2 = subject.disableTracing(null, user);
        VoidResponse result3 = subject.disableTracing(queryRegex, user);
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a BadRequestException due to null regex and user values", result1);

        assertNotNull("Expected a non-null response", result2);

        assertNotNull("Expected a non-null response", result3);
    }

    @Ignore
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
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, DefaultQueryParameters.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.putSingle(QueryParameters.QUERY_PARAMS, parameters);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.putSingle(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);

        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);

        QueryParameters qp = new DefaultQueryParameters();
        qp.validate(queryParameters);

        MultivaluedMap<String,String> op = MapUtils.toMultivaluedMap(qp.getUnknownParameters(MapUtils.toMultiValueMap(queryParameters)));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogic1.getClass().getSimpleName());
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDN);

        // Set expectations of the create logic
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).times(2);
        expect(this.principal.getName()).andReturn(userName).times(2);
        expect(this.principal.getShortName()).andReturn(userSid).times(2);
        expect(this.principal.getUserDN()).andReturn(userDNpair);
        expect(this.principal.getDNs()).andReturn(new String[] {userDN});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        queryLogic1.validate(queryParameters);
        expect(this.queryLogic1.getAuditType(null)).andReturn(AuditType.NONE);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations))).times(2);
        expect(this.cache.get(queryId.toString())).andReturn(this.runningQuery);
        expect(this.runningQuery.getSettings()).andReturn(this.query).times(2);
        expect(this.query.getOwner()).andReturn(userSid);
        QueryImpl newQuery1 = new QueryImpl();
        newQuery1.setId(UUID.randomUUID());
        newQuery1.setQuery(query);
        newQuery1.setQueryName(newQueryName);
        newQuery1.setBeginDate(beginDate);
        newQuery1.setEndDate(endDate);
        newQuery1.setExpirationDate(expirationDate);
        newQuery1.setDnList(Collections.singletonList(userDN));
        expect(this.query.duplicate(newQueryName)).andReturn(newQuery1);
        expect(context.getCallerPrincipal()).andReturn(principal).times(2);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, principal)).andReturn((QueryLogic) this.queryLogic1).times(2);
        expect(this.queryLogic1.getLogicName()).andReturn(queryLogicName).times(2);
        expect(this.queryLogic1.getMaxPageSize()).andReturn(100).times(2);
        QueryImpl newQuery2 = new TestQuery(newQuery1);
        expect(persister.create(eq(userDNpair.subjectDN()), eq(dnList), eq(marking), eq(queryLogicName), eq(qp), eq(op))).andReturn(newQuery2);
        expect(this.queryLogic1.getAuditType(newQuery2)).andReturn(AuditType.NONE);
        expect(this.queryLogic1.getConnectionPriority()).andReturn(Priority.NORMAL);
        expect(this.queryLogic1.getConnPoolName()).andReturn("connPool1");
        expect(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).andReturn(null);
        this.query.populateTrackingMap(null);
        expect(this.connectionFactory.getClient(userDN.toLowerCase(), new ArrayList<>(0), "connPool1", Priority.NORMAL, null)).andReturn(this.client);
        expect(this.qlCache.add(newQuery1.getId().toString(), userSid, this.queryLogic1, this.client)).andReturn(true);
        expect(this.queryLogic1.getCollectQueryMetrics()).andReturn(false);
        expect(this.queryLogic1.initialize(eq(this.client), isA(Query.class), isA(Set.class))).andReturn(this.genericConfiguration);
        this.queryLogic1.setupQuery(this.genericConfiguration);
        expect(this.queryLogic1.getTransformIterator(eq(newQuery2))).andReturn(this.transformIterator);
        expect(this.genericConfiguration.getQueryString()).andReturn(query).once();
        this.cache.put(eq(newQuery2.getId().toString()), isA(RunningQuery.class));
        expect(this.qlCache.poll(newQuery1.getId().toString())).andReturn(null);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, QueryMetricsBean.class, metrics);
        setInternalState(subject, Multimap.class, traceInfos);
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        GenericResponse<String> result1 = subject.duplicateQuery(queryId.toString(), newQueryName, queryLogicName, query, queryVisibility, beginDate, endDate,
                        queryAuthorizations, expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters, trace);
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a non-null response", result1);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test(expected = DatawaveWebApplicationException.class)
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
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        expect(this.cache.get(queryId.toString())).andReturn(null);
        expect(this.persister.findById(queryId.toString())).andReturn(null);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());

        try {
            subject.duplicateQuery(queryId.toString(), queryName, queryLogicName, query, queryVisibility, beginDate, endDate, queryAuthorizations,
                            expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters, trace);
        } finally {
            PowerMock.verifyAll();
        }
    }

    @Ignore
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test(expected = DatawaveWebApplicationException.class)
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
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).times(4);
        expect(this.principal.getName()).andReturn(userName).times(2);
        expect(this.principal.getShortName()).andReturn(userSid).times(2);
        expect(this.principal.getUserDN()).andReturn(userDNpair);
        expect(this.principal.getDNs()).andReturn(new String[] {userDN});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.queryLogic1.getAuditType(null)).andReturn(AuditType.NONE);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations))).times(2);
        expect(this.cache.get(queryId.toString())).andReturn(this.runningQuery);
        expect(this.runningQuery.getSettings()).andReturn(this.query).times(2);
        expect(this.query.getOwner()).andReturn(userSid);
        QueryImpl newQuery1 = new QueryImpl();
        newQuery1.setId(UUID.randomUUID());
        expect(this.query.duplicate(newQueryName)).andReturn(newQuery1);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, principal)).andReturn((QueryLogic) this.queryLogic1).times(2);
        expect(this.queryLogic1.getLogicName()).andReturn(queryLogicName);
        expect(this.queryLogic1.getMaxPageSize()).andReturn(100).times(2);
        QueryImpl newQuery2 = new TestQuery(newQuery1);

        expect(this.queryLogic1.getAuditType(newQuery2)).andReturn(AuditType.NONE);
        Exception uncheckedException = new IllegalStateException("INTENTIONALLY THROWN UNCHECKED TEST EXCEPTION");
        expect(this.queryLogic1.getConnectionPriority()).andThrow(uncheckedException);
        this.queryLogic1.close();
        this.persister.remove(newQuery2);
        expect(this.qlCache.poll(newQuery1.getId().toString())).andReturn(null);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, QueryMetricsBean.class, metrics);
        setInternalState(subject, Multimap.class, traceInfos);
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());

        try {
            subject.duplicateQuery(queryId.toString(), newQueryName, queryLogicName, query, queryVisibility, beginDate, endDate, queryAuthorizations,
                            expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters, trace);
        } finally {
            PowerMock.verifyAll();
        }
    }

    @Test(expected = DatawaveWebApplicationException.class)
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
        expect(this.context.getCallerPrincipal()).andThrow(uncheckedException);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());

        try {
            subject.duplicateQuery(queryId.toString(), newQueryName, queryLogicName, query, queryVisibility, beginDate, endDate, queryAuthorizations,
                            expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters, trace);
        } finally {
            PowerMock.verifyAll();
        }
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
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();

        Exception result1 = null;
        try {
            subject.duplicateQuery(queryId.toString(), newQueryName, queryLogicName, query, queryVisibility, beginDate, endDate, queryAuthorizations,
                            expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters, trace);
        } catch (DatawaveWebApplicationException e) {
            result1 = e;
        }
        PowerMock.verifyAll();

        assertNotNull("Expected a DatawaveWebApplicationException.", result1);
        assertEquals("Expected a Bad Request status code.", 400, ((DatawaveWebApplicationException) result1).getResponse().getStatus());
    }

    @Test
    public void testEnableTracing_NullChecksAndHappyPath() throws Exception {
        // Set local test input
        String user = "user";
        String queryRegex = "queryRegex";

        PowerMock.resetAll();

        // Set expectations
        expect(traceInfos.containsEntry(user, PatternWrapper.wrap(queryRegex))).andReturn(false);
        expect(traceInfos.put(user, PatternWrapper.wrap(queryRegex))).andReturn(true);
        traceCache.put("traceInfos", traceInfos);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, Multimap.class, traceInfos);
        setInternalState(subject, QueryTraceCache.class, traceCache);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        Exception result1 = null;
        try {
            subject.enableTracing(null, null);
        } catch (BadRequestException e) {
            result1 = e;
        }
        VoidResponse result2 = subject.enableTracing(queryRegex, user);
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a BadRequestException due to null regex and user values", result1);
        assertNotNull("Expected a non-null response", result2);

    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testGet_HappyPath() throws Exception {
        // Set local test input
        String userName = "userName";
        String sid = "sid";
        UUID queryId = UUID.randomUUID();

        // Set expectations
        expect(this.context.getCallerPrincipal()).andReturn(this.principal);
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(sid);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList("AUTH_1")));
        expect(this.cache.get(queryId.toString())).andReturn(this.runningQuery);
        expect(this.runningQuery.getSettings()).andReturn(this.query).times(2);
        expect(this.query.getOwner()).andReturn(sid);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        QueryImplListResponse result1 = subject.get(queryId.toString());
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected non-null response", result1);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test(expected = DatawaveWebApplicationException.class)
    public void testGet_UncheckedException() throws Exception {
        // Set local test input
        String userName = "userName";
        String sid = "sid";
        UUID queryId = UUID.randomUUID();

        // Set expectations
        expect(this.context.getCallerPrincipal()).andReturn(this.principal);
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(sid);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList((Collection) Arrays.asList("AUTH_1")));
        expect(this.cache.get(queryId.toString())).andReturn(this.runningQuery);
        expect(this.runningQuery.getSettings()).andReturn(this.query);
        expect(this.query.getOwner()).andReturn(sid);
        expect(this.runningQuery.getSettings()).andThrow(new IllegalStateException("INTENTIONALLY THROWN UNCHECKED TEST EXCEPTION"));

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());

        try {
            subject.get(queryId.toString());
        } finally {
            PowerMock.verifyAll();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test(expected = DatawaveWebApplicationException.class)
    public void testGet_QueryExceptionDueToNonMatchingSids() throws Exception {
        // Set local test input
        String userName = "userName";
        String sid = "sid";
        UUID queryId = UUID.randomUUID();

        // Set expectations
        expect(this.context.getCallerPrincipal()).andReturn(this.principal);
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(sid);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList("AUTH_1")));
        expect(this.cache.get(queryId.toString())).andReturn(this.runningQuery);
        expect(this.runningQuery.getSettings()).andReturn(this.query).times(2);
        expect(this.query.getOwner()).andReturn("nonmatching_sid").times(2);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());

        try {
            subject.get(queryId.toString());
        } finally {
            PowerMock.verifyAll();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testInit() throws Exception {
        // Set expectations
        expect(this.traceCache.putIfAbsent(isA(String.class), (Multimap) notNull())).andReturn(null);
        this.traceCache.addListener(isA(CacheListener.class));
        expect(this.lookupUUIDConfiguration.getUuidTypes()).andReturn(null);
        expect(this.lookupUUIDConfiguration.getBeginDate()).andReturn("not a date");
        expect(this.lookupUUIDConfiguration.getBatchLookupUpperLimit()).andReturn(0);
        expect(this.lookupUUIDConfiguration.getContentLookupTypes()).andReturn(Collections.emptyMap());
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        LookupUUIDConfiguration tmpCfg = new LookupUUIDConfiguration();
        tmpCfg.setColumnVisibility("PUBLIC");
        expect(this.lookupUUIDConfiguration.optionalParamsToMap()).andDelegateTo(tmpCfg);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryTraceCache.class, traceCache);
        setInternalState(subject, LookupUUIDConfiguration.class, lookupUUIDConfiguration);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());

        try {
            subject.init();
        } finally {
            PowerMock.verifyAll();
        }
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
        expect(this.context.getCallerPrincipal()).andReturn(this.principal);
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(sid);
        expect(this.principal.getPrimaryUser()).andReturn(dwUser).anyTimes();
        expect(this.dwUser.getAuths()).andReturn(Collections.singletonList("AUTH_1")).anyTimes();
        expect(this.principal.getProxiedUsers()).andReturn(Collections.singletonList(dwUser));
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList("AUTH_1")));
        expect(this.userOperations.getRemoteUser(this.principal)).andReturn(this.principal);
        expect(this.persister.findByName(queryName)).andReturn(Arrays.asList((Query) this.query));
        expect(this.query.getOwner()).andReturn(sid).anyTimes();
        expect(this.query.getUserDN()).andReturn(sid).anyTimes();
        expect(this.query.getQueryLogicName()).andReturn(queryLogicName).anyTimes();
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getConnectionPriority()).andReturn(Priority.HIGH);
        expect(this.query.getQueryAuthorizations()).andReturn(null).anyTimes();
        expect(this.queryLogic1.getCollectQueryMetrics()).andReturn(false);
        expect(this.query.getId()).andReturn(queryId).anyTimes();
        expect(this.query.getQuery()).andReturn(queryName).anyTimes();
        expect(this.query.getBeginDate()).andReturn(null).anyTimes();
        expect(this.query.getEndDate()).andReturn(null).anyTimes();
        expect(this.query.getColumnVisibility()).andReturn(null).anyTimes();
        expect(this.cache.containsKey(queryId.toString())).andReturn(false);
        expect(this.query.getQueryName()).andReturn(null).anyTimes();
        expect(this.query.getPagesize()).andReturn(0).anyTimes();
        expect(this.query.getPageTimeout()).andReturn(-1).anyTimes();
        expect(this.query.getExpirationDate()).andReturn(null).anyTimes();
        expect(this.query.getParameters()).andReturn((Set) Collections.emptySet()).anyTimes();
        expect(this.query.findParameter(RemoteUserOperationsImpl.INCLUDE_REMOTE_SERVICES))
                        .andReturn(new QueryImpl.Parameter(RemoteUserOperationsImpl.INCLUDE_REMOTE_SERVICES, "true")).anyTimes();
        expect(this.query.getDnList()).andReturn(dnList).anyTimes();
        expect(this.queryLogic1.getResultLimit(this.query)).andReturn(-1L);
        expect(this.queryLogic1.getMaxResults()).andReturn(-1L);
        this.queryLogic1.preInitialize(this.query, WSAuthorizationsUtil.buildAuthorizations(null));
        expect(this.queryLogic1.getUserOperations()).andReturn(null);
        this.cache.put(eq(queryId.toString()), isA(RunningQuery.class));

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(subject, UserOperationsBean.class, userOperations);
        QueryImplListResponse result1 = subject.list(queryName);
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("QueryLogicResponse should not be returned null", result1);
    }

    @Test
    public void testListQueriesForUser_HappyPath() throws Exception {
        // Set local test input
        String userSid = "userSid";

        // Set expectations
        expect(this.persister.findByUser(userSid)).andReturn(Arrays.asList(this.query));

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        QueryImplListResponse result1 = subject.listQueriesForUser(userSid);
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a non-null response", result1);
    }

    @Test(expected = NoResultsException.class)
    public void testListQueriesForUser_NoResultsException() throws Exception {
        // Set local test input
        String userSid = "userSid";

        // Set expectations
        expect(this.persister.findByUser(userSid)).andReturn(null);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());

        try {
            subject.listQueriesForUser(userSid);
        } finally {
            PowerMock.verifyAll();
        }
    }

    @Test(expected = DatawaveWebApplicationException.class)
    public void testListQueriesForUser_UncheckedException() throws Exception {
        // Set local test input
        String userSid = "userSid";

        // Set expectations
        expect(this.persister.findByUser(userSid)).andThrow(ILLEGAL_STATE_EXCEPTION);

        // Run the test
        PowerMock.replayAll();
        try {
            QueryExecutorBean subject = new QueryExecutorBean();
            setInternalState(subject, Persister.class, persister);
            setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());

            subject.listQueriesForUser(userSid);
        } finally {
            PowerMock.verifyAll();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testListQueryLogic() throws Exception {
        // Set expectations
        expect(this.queryLogicFactory.getQueryLogicList()).andReturn(Arrays.asList(this.queryLogic1, this.queryLogic2));
        expect(this.queryLogic1.getLogicName()).andReturn("logic1").times(1); // Begin 1st loop
        expect(this.queryLogic1.getAuditType(null)).andReturn(AuditType.LOCALONLY);
        expect(this.queryLogic1.getLogicDescription()).andReturn("description1");
        expect(this.queryLogic1.getOptionalQueryParameters()).andReturn(new TreeSet<>());
        expect(this.queryLogic1.getRequiredQueryParameters()).andReturn(new TreeSet<>());
        expect(this.queryLogic1.getExampleQueries()).andReturn(new TreeSet<>());
        expect(this.queryLogic1.getRequiredRoles()).andReturn(new HashSet<>()).anyTimes();
        expect(this.queryLogic1.getResponseClass(anyObject(Query.class))).andThrow(ILLEGAL_STATE_EXCEPTION);
        expect(this.queryLogic2.getLogicName()).andReturn("logic2").times(1); // Begin 1st loop
        expect(this.queryLogic2.getAuditType(null)).andReturn(AuditType.LOCALONLY);
        expect(this.queryLogic2.getLogicDescription()).andReturn("description2");
        expect(this.queryLogic2.getOptionalQueryParameters()).andReturn(new TreeSet<>());
        expect(this.queryLogic2.getRequiredQueryParameters()).andReturn(new TreeSet<>());
        expect(this.queryLogic2.getExampleQueries()).andReturn(new TreeSet<>());
        expect(this.queryLogic2.getRequiredRoles()).andReturn(new HashSet<>(Arrays.asList("ROLE_1", "ROLE_2"))).times(2);
        expect(this.queryLogic2.getResponseClass(anyObject(Query.class))).andReturn(this.baseResponse.getClass().getCanonicalName());
        expect(this.responseObjectFactory.getQueryImpl()).andReturn(new QueryImpl());
        Map<String,String> parsers = new HashMap<>();
        parsers.put("PARSER1", null);
        expect(this.queryLogic2.getQuerySyntaxParsers()).andReturn((Map) parsers);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        QueryLogicResponse result1 = subject.listQueryLogic();
        subject.close();
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("QueryLogicResponse should not be returned null", result1);
    }

    @Test
    public void testListUserQueries_HappyPath() throws Exception {
        // Set expectations
        expect(this.persister.findByUser()).andReturn(Arrays.asList(this.query));

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        QueryImplListResponse result1 = subject.listUserQueries();
        subject.close();
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Query response should not be returned null", result1);
    }

    @Test
    public void testListUserQueries_NoResultsException() throws Exception {
        // Set expectations
        expect(this.persister.findByUser()).andReturn(null);

        // Run the test
        PowerMock.replayAll();
        Exception result1 = null;
        try {
            QueryExecutorBean subject = new QueryExecutorBean();
            setInternalState(subject, Persister.class, persister);
            setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
            subject.listUserQueries();
        } catch (Exception e) {
            result1 = e;
        }
        PowerMock.verifyAll();

        assertNotNull("Expected a DatawaveWebApplicationException.", result1);
        assertEquals("Expected a No Results status code.", 204, ((DatawaveWebApplicationException) result1).getResponse().getStatus());
    }

    @Test
    public void testListUserQueries_UncheckedException() throws Exception {
        // Set expectations
        Exception uncheckedException = new IllegalStateException("INTENTIONALLY THROWN UNCHECKED TEST EXCEPTION");
        expect(this.persister.findByUser()).andThrow(uncheckedException);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        Exception result1 = null;
        try {
            subject.listUserQueries();
        } catch (DatawaveWebApplicationException e) {
            result1 = e;
        }
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected an exception to be thrown", result1);
        assertTrue("Expected an QueryException to be wrapped by a DatawaveWebApplicationException", result1.getCause() instanceof QueryException);
        assertSame("Expected an unchecked exception to be wrapped by a QueryException", result1.getCause().getCause(), uncheckedException);
    }

    @Test
    public void testNext_QueryExceptionDueToCacheLock() throws Exception {
        // Set local test input
        String userName = "userName";
        String userSid = "userSid";
        UUID queryId = UUID.randomUUID();

        // Set expectations
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName).anyTimes();
        expect(this.principal.getShortName()).andReturn(userSid).anyTimes();
        expect(this.principal.getUserDN()).andReturn(SubjectIssuerDNPair.of(userName));
        expect(this.principal.getDNs()).andReturn(new String[] {userName});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.context.getUserTransaction()).andReturn(this.transaction).anyTimes();
        this.transaction.begin();
        expect(this.cache.get(queryId.toString())).andReturn(this.runningQuery);
        expect(this.cache.lock(queryId.toString())).andReturn(false);
        expect(this.responseObjectFactory.getEventQueryResponse()).andReturn(new DefaultEventQueryResponse());
        this.runningQuery.setActiveCall(false);
        expect(this.runningQuery.getLogic()).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getCollectQueryMetrics()).andReturn(true);
        expect(this.runningQuery.getMetric()).andReturn(this.queryMetric).times(2);
        expectLastCall();
        this.queryMetric.setError(isA(Throwable.class));
        this.metrics.updateMetric(this.queryMetric);
        cache.unlock(queryId.toString());
        this.transaction.setRollbackOnly();
        expect(this.transaction.getStatus()).andThrow(ILLEGAL_STATE_EXCEPTION);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, QueryMetricsBean.class, metrics);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        Exception result1 = null;
        try {
            subject.next(queryId.toString());
        } catch (DatawaveWebApplicationException e) {
            result1 = e;
        }
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a DatawaveWebApplicationException to be thrown", result1);
        assertEquals("Expected DatawaveWebApplicationException to have been caused by a locked cache entry", "500-9",
                        ((QueryException) result1.getCause().getCause()).getErrorCode());
    }

    @Test(expected = DatawaveWebApplicationException.class)
    public void testNext_UncheckedException() throws Exception {
        // Set local test input
        String userName = "userName";
        String userSid = "userSid";
        UUID queryId = UUID.randomUUID();

        // Set expectations
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName).anyTimes();
        expect(this.principal.getShortName()).andReturn(userSid).anyTimes();
        expect(this.principal.getUserDN()).andReturn(SubjectIssuerDNPair.of(userName));
        expect(this.principal.getDNs()).andReturn(new String[] {userName});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.context.getUserTransaction()).andReturn(this.transaction).anyTimes();
        this.transaction.begin();
        expect(this.cache.get(queryId.toString())).andReturn(this.runningQuery);
        expect(this.cache.lock(queryId.toString())).andThrow(new IllegalStateException("INTENTIONALLY THROWN UNCHECKED TEST EXCEPTION"));
        expect(this.responseObjectFactory.getEventQueryResponse()).andReturn(new DefaultEventQueryResponse());
        this.runningQuery.setActiveCall(false);
        expect(this.runningQuery.getLogic()).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getCollectQueryMetrics()).andReturn(true);
        expect(this.runningQuery.getMetric()).andReturn(this.queryMetric).times(2);
        expectLastCall();
        this.queryMetric.setError(isA(Throwable.class));
        cache.unlock(queryId.toString());
        this.transaction.setRollbackOnly();
        expect(this.transaction.getStatus()).andReturn(Status.STATUS_PREPARING).times(2);
        this.transaction.commit();
        PowerMock.expectLastCall().andThrow(new HeuristicMixedException("INTENTIONALLY THROWN TEST EXCEPTION"));

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());

        try {
            subject.next(queryId.toString());
        } finally {
            PowerMock.verifyAll();
        }
    }

    @Test
    public void testNext_UserNotOwner() throws Exception {
        // Set local test input
        String userName = "userName";
        String userSid = "userSid";
        String otherSid = "otherSid";
        UUID queryId = UUID.randomUUID();

        // Set expectations
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName).anyTimes();
        expect(this.principal.getShortName()).andReturn(otherSid).anyTimes();
        expect(this.principal.getUserDN()).andReturn(SubjectIssuerDNPair.of(userName));
        expect(this.principal.getDNs()).andReturn(new String[] {userName});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.context.getUserTransaction()).andReturn(this.transaction).anyTimes();
        this.transaction.begin();
        expect(this.cache.get(queryId.toString())).andReturn(this.runningQuery);
        expect(this.cache.lock(queryId.toString())).andReturn(true);
        expect(this.responseObjectFactory.getEventQueryResponse()).andReturn(new DefaultEventQueryResponse());
        expect(this.runningQuery.getClient()).andReturn(this.client);
        expect(this.runningQuery.getSettings()).andReturn(this.query);
        expect(this.query.getOwner()).andReturn(userSid);
        expect(this.runningQuery.getSettings()).andReturn(this.query);
        expect(this.query.getOwner()).andReturn(userSid);
        cache.unlock(queryId.toString());
        expect(this.transaction.getStatus()).andReturn(Status.STATUS_PREPARING).times(2);
        this.transaction.setRollbackOnly();
        this.transaction.commit();
        this.runningQuery.setActiveCall(false);
        expect(this.runningQuery.getLogic()).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getCollectQueryMetrics()).andReturn(true);
        expect(this.runningQuery.getMetric()).andReturn(this.queryMetric).times(2);
        expectLastCall();
        this.queryMetric.setError(isA(Throwable.class));

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        Exception result1 = null;
        try {
            subject.next(queryId.toString());
        } catch (DatawaveWebApplicationException e) {
            result1 = e;
            assertTrue(e.getCause() instanceof QueryException);
            assertEquals("401-1", ((QueryException) e.getCause().getCause()).getErrorCode());
        }
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a DatawaveWebApplicationException to be thrown due an unchecked exception", result1);
    }

    @Test
    public void testNext_NullQueryReturnedFromCache() throws Exception {
        // Set local test input
        String userName = "userName";
        String userSid = "userSid";
        UUID queryId = UUID.randomUUID();

        // Set expectations
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName).anyTimes();
        expect(this.principal.getShortName()).andReturn(userSid).anyTimes();
        expect(this.principal.getUserDN()).andReturn(SubjectIssuerDNPair.of(userName));
        expect(this.principal.getDNs()).andReturn(new String[] {userName});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.context.getUserTransaction()).andReturn(this.transaction).anyTimes();
        this.transaction.begin();
        expect(this.cache.get(queryId.toString())).andReturn(null);
        expect(this.cache.lock(queryId.toString())).andReturn(true);
        expect(this.responseObjectFactory.getEventQueryResponse()).andReturn(new DefaultEventQueryResponse());
        expect(this.persister.findById(queryId.toString())).andReturn(new ArrayList<>(0));
        cache.unlock(queryId.toString());
        this.transaction.setRollbackOnly();
        PowerMock.expectLastCall().andThrow(ILLEGAL_STATE_EXCEPTION);
        expect(this.transaction.getStatus()).andReturn(Status.STATUS_UNKNOWN).times(2);
        this.transaction.commit();
        PowerMock.expectLastCall().andThrow(new HeuristicRollbackException("INTENTIONALLY THROWN TEST EXCEPTION"));

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryMetricsBean.class, metrics);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());

        Exception result1 = null;
        try {
            subject.next(queryId.toString());
        } catch (DatawaveWebApplicationException e) {
            result1 = e;
        }
        PowerMock.verifyAll();

        assertNotNull("Expected a DatawaveWebApplicationException.", result1);
        assertEquals("Expected a Not Found status code.", 404, ((DatawaveWebApplicationException) result1).getResponse().getStatus());
    }

    @Test(expected = DatawaveWebApplicationException.class)
    public void testPurgeQueryCache_UncheckedException() throws Exception {
        // Set expectations
        this.cache.clear();
        PowerMock.expectLastCall().andThrow(new IllegalStateException("INTENTIONALLY THROWN UNCHECKED TEST EXCEPTION"));

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());

        try {
            subject.purgeQueryCache();
        } finally {
            PowerMock.verifyAll();
        }
    }

    @Test
    public void testPurgeQueryCacheAndMiscAccessors_HappyPath() throws Exception {
        // Set expectations
        this.cache.clear();

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        VoidResponse result1 = subject.purgeQueryCache();
        QueryMetricsBean result5 = subject.getMetrics();
        QueryLogicFactory result6 = subject.getQueryFactory();
        Persister result7 = subject.getPersister();
        QueryCache result8 = subject.getQueryCache();
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a non-null response", result1);
        assertNull("Expected a null metrics instance", result5);
        assertNull("Expected a null query logic factory", result6);
        assertNull("Expected a null persister", result7);
        assertNotNull("Expected a NON-null cache", result8);
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
        map.set(PrivateAuditConstants.AUDIT_TYPE, AuditType.PASSIVE.name());
        map.set(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        map.set(PrivateAuditConstants.COLUMN_VISIBILITY, authorization);
        map.set(PrivateAuditConstants.USER_DN, userDN);
        map.set(AuditParameters.AUDIT_ID, queryName);
        MultiValueMap<String,String> auditMap = new LinkedMultiValueMap();
        auditMap.putAll(map);

        // Set expectations
        expect(this.context.getUserTransaction()).andReturn(this.transaction).anyTimes();
        this.transaction.begin();
        expect(this.transaction.getStatus()).andReturn(Status.STATUS_ACTIVE).anyTimes();
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName).anyTimes();
        expect(this.principal.getShortName()).andReturn(sid).anyTimes();
        expect(this.principal.getUserDN()).andReturn(SubjectIssuerDNPair.of(userDN));
        expect(this.principal.getDNs()).andReturn(new String[] {userDN});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>());
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(authorization)));
        expect(this.principal.getPrimaryUser()).andReturn(dwUser).anyTimes();
        expect(this.dwUser.getAuths()).andReturn(Collections.singleton(authorization)).anyTimes();
        expect(this.principal.getProxiedUsers()).andReturn(Collections.singletonList(dwUser));
        expect(this.userOperations.getRemoteUser(this.principal)).andReturn(this.principal);
        expect(this.cache.get(queryName)).andReturn(null);
        expect(this.persister.findById(queryName)).andReturn(Arrays.asList((Query) this.query));
        expect(this.query.getQueryLogicName()).andReturn(queryLogicName).anyTimes();
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getConnectionPriority()).andReturn(Priority.NORMAL);
        expect(this.query.getQueryAuthorizations()).andReturn(authorization).anyTimes();
        expect(this.queryLogic1.getCollectQueryMetrics()).andReturn(false);
        expect(this.query.getUncaughtExceptionHandler()).andReturn(exceptionHandler).anyTimes();
        expect(this.exceptionHandler.getThrowable()).andReturn(null).anyTimes();
        expect(this.query.getOwner()).andReturn(sid).anyTimes();
        expect(this.query.getId()).andReturn(queryId).anyTimes();
        expect(this.query.getQuery()).andReturn(queryName).anyTimes();
        expect(this.query.getQueryName()).andReturn(queryName).anyTimes();
        this.cache.put(eq(queryId.toString()), isA(RunningQuery.class));
        expect(this.cache.lock(queryName)).andReturn(true);
        expect(this.queryLogic1.getAuditType(this.query)).andReturn(AuditType.PASSIVE);
        expect(this.query.getUserDN()).andReturn(userDN).anyTimes();
        expect(this.query.getDnList()).andReturn(dnList).anyTimes();
        expect(this.queryLogic1.isLongRunningQuery()).andReturn(false);
        expect(this.queryLogic1.getResultLimit(this.query)).andReturn(-1L);
        expect(this.queryLogic1.getMaxResults()).andReturn(-1L);
        this.queryLogic1.preInitialize(this.query, WSAuthorizationsUtil.buildAuthorizations(Collections.singleton(Sets.newHashSet("AUTH_1"))));
        expect(this.queryLogic1.getUserOperations()).andReturn(null);
        expect(this.query.toMap()).andReturn(map);
        expect(this.query.getColumnVisibility()).andReturn(authorization);
        expect(this.query.getBeginDate()).andReturn(null);
        expect(this.query.getEndDate()).andReturn(null);
        expect(this.query.getParameters()).andReturn((Set) Collections.emptySet());
        expect(this.query.findParameter(RemoteUserOperationsImpl.INCLUDE_REMOTE_SERVICES))
                        .andReturn(new QueryImpl.Parameter(RemoteUserOperationsImpl.INCLUDE_REMOTE_SERVICES, "true")).anyTimes();
        expect(this.query.getColumnVisibility()).andReturn(authorization);
        expect(this.queryLogic1.getSelectors(this.query)).andReturn(null);
        expect(this.auditor.audit(auditMap)).andReturn(null);
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
        expect(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).andReturn(new HashMap<>());
        this.query.populateTrackingMap(new HashMap<>());
        expect(this.queryLogic1.getConnPoolName()).andReturn("connPool1");
        expect(this.queryLogic1.getLogicName()).andReturn(queryLogicName);
        connectionRequestBean.requestBegin(queryName, userDN.toLowerCase(), new HashMap<>());
        expect(this.connectionFactory.getClient(eq(userDN.toLowerCase()), eq(new ArrayList<>()), eq("connPool1"), eq(Priority.NORMAL), eq(new HashMap<>())))
                        .andReturn(this.client);
        connectionRequestBean.requestEnd(queryName);
        expect(this.queryLogic1.initialize(eq(this.client), eq(this.query), isA(Set.class))).andReturn(this.genericConfiguration);
        this.queryLogic1.setupQuery(this.genericConfiguration);
        expect(this.queryLogic1.getTransformIterator(this.query)).andReturn(this.transformIterator);
        expect(this.genericConfiguration.getQueryString()).andReturn(queryName).once();
        this.connectionFactory.returnClient(null); // These 2 lines prevent the bean's exception-handling logic (in combination
        PowerMock.expectLastCall().anyTimes(); // with PowerMock) from masking an actual problem if one occurs.
        cache.unlock(queryName);
        this.transaction.commit();

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, UserOperationsBean.class, userOperations);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, QueryMetricsBean.class, metrics);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);
        VoidResponse result1 = subject.reset(queryName);
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("VoidResponse should not be returned null", result1);
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
        expect(this.context.getUserTransaction()).andReturn(this.transaction).anyTimes();
        this.transaction.begin();
        expect(this.transaction.getStatus()).andReturn(Status.STATUS_ACTIVE).anyTimes();
        expect(this.context.getCallerPrincipal()).andReturn(this.principal);
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(authorization)));
        expect(this.cache.get(queryName)).andReturn(this.runningQuery);
        expect(this.runningQuery.getSettings()).andReturn(this.query);
        expect(this.query.getOwner()).andReturn(userSid);
        // expect(this.runningQuery.getTraceInfo()).andReturn(this.traceInfo);
        expect(this.cache.lock(queryName)).andReturn(true);
        expect(this.runningQuery.getClient()).andReturn(this.client);
        this.runningQuery.closeConnection(this.connectionFactory);
        PowerMock.expectLastCall().andThrow(new IOException("INTENTIONALLY THROWN 1ST-LEVEL TEST EXCEPTION"));
        cache.unlock(queryName);
        this.transaction.commit();
        PowerMock.expectLastCall().andThrow(new IllegalStateException("INTENTIONALLY THROWN 3RD-LEVEL TEST EXCEPTION"));

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, QueryMetricsBean.class, metrics);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        Throwable result1 = null;
        try {
            subject.reset(queryName);
        } catch (DatawaveWebApplicationException e) {
            result1 = e.getCause();
        }
        PowerMock.verifyAll();

        // Verify results
        assertTrue("Query exception expected to have been thrown due to locking problem", result1 instanceof QueryException);
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
        expect(this.context.getUserTransaction()).andReturn(this.transaction).anyTimes();
        this.transaction.begin();
        expect(this.transaction.getStatus()).andReturn(Status.STATUS_ACTIVE);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal);
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(authorization)));
        expect(this.cache.get(queryName)).andReturn(this.runningQuery);
        expect(this.runningQuery.getSettings()).andReturn(this.query);
        expect(this.query.getOwner()).andReturn(userSid);
        // expect(this.runningQuery.getTraceInfo()).andReturn(this.traceInfo);
        expect(this.cache.lock(queryName)).andReturn(false);
        expect(this.transaction.getStatus()).andReturn(Status.STATUS_NO_TRANSACTION).anyTimes();
        this.connectionFactory.returnClient(null); // These 2 lines prevent the bean's exception-handling logic (in combination
        PowerMock.expectLastCall().anyTimes(); // with PowerMock) from masking an actual problem if one occurs.
        cache.unlock(queryName);
        this.transaction.commit();

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, QueryMetricsBean.class, metrics);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        Throwable result1 = null;
        try {
            subject.reset(queryName);
        } catch (DatawaveWebApplicationException e) {
            result1 = e.getCause();
        }
        PowerMock.verifyAll();

        // Verify results
        assertTrue("Query exception expected to have been thrown", result1 instanceof QueryException);
        assertEquals("Thrown exception expected to have been due to locking problem", "500-9", ((QueryException) result1.getCause()).getErrorCode());
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
        Query duplicateQuery = PowerMock.createMock(Query.class);

        MultiValueMap<String,String> p = new LinkedMultiValueMap<>();
        p.set(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        p.set(QueryParameters.QUERY_BEGIN, DefaultQueryParameters.formatDate(beginDate));
        p.set(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        p.set(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expirationDate));
        p.set(QueryParameters.QUERY_NAME, queryName);
        p.set(QueryParameters.QUERY_PAGESIZE, Integer.toString(pagesize));
        p.set(QueryParameters.QUERY_PAGETIMEOUT, Integer.toString(pageTimeout));
        p.set(QueryParameters.QUERY_STRING, query);
        p.set(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        p.set(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        p.set(QueryParameters.QUERY_PARAMS, parameters);

        p.set(PrivateAuditConstants.AUDIT_TYPE, AuditType.LOCALONLY.name());
        p.set(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        p.set(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        p.set(PrivateAuditConstants.USER_DN, userDN);
        MultiValueMap<String,String> auditMap = new LinkedMultiValueMap();
        auditMap.putAll(p);
        auditMap.set(AuditParameters.AUDIT_ID, queryId.toString());

        // Set expectations
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).times(4);
        expect(this.principal.getName()).andReturn(userName).times(2);
        expect(this.principal.getShortName()).andReturn(userSid).times(2);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations))).times(2);
        expect(this.cache.get(queryId.toString())).andReturn(this.runningQuery);
        expect(this.runningQuery.getSettings()).andReturn(this.query).times(3);
        expect(this.query.getOwner()).andReturn(userSid);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).andReturn((QueryLogic) this.queryLogic1).times(2);
        expect(this.queryLogic1.getLogicName()).andReturn(queryLogicName).times(2);
        expect(this.query.duplicate(queryName)).andReturn(duplicateQuery);
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
        expect(duplicateQuery.toMap()).andReturn(p);
        expect(this.auditor.audit(eq(auditMap))).andReturn(null);
        this.query.setQueryLogicName(queryLogicName);
        this.query.setQuery(query);
        this.query.setBeginDate(beginDate);
        this.query.setEndDate(endDate);
        this.query.setQueryAuthorizations(queryAuthorizations);
        this.query.setExpirationDate(expirationDate);
        this.query.setPagesize(pagesize);
        this.query.setPageTimeout(pageTimeout);
        this.query.setParameters(isA(Set.class));
        expect(this.query.getQueryName()).andReturn(queryName).times(2);
        this.persister.update(this.query);
        expect(this.query.getId()).andReturn(queryId).times(3);
        this.cache.put(queryId.toString(), this.runningQuery);
        expect(this.runningQuery.getLogic()).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getAuditType(this.query)).andReturn(AuditType.LOCALONLY);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        GenericResponse<String> result1 = subject.updateQuery(queryId.toString(), queryLogicName, query, queryVisibility, beginDate, endDate,
                        queryAuthorizations, expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, parameters);
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a non-null response performing an admin close", result1);
    }

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

        MultiValueMap<String,String> params = new LinkedMultiValueMap<>(qp.toMap());
        params.set(QueryParameters.QUERY_TRACE, Boolean.toString(trace));
        params.set(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        params.set(QueryParameters.QUERY_PARAMS, parameters);

        QueryExecutorBean subject = PowerMock.createPartialMock(QueryExecutorBean.class, "createQuery");

        // Set expectations of the create logic
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.httpHeaders.getAcceptableMediaTypes()).andReturn(mediaTypes);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getEnrichedTransformer(isA(Query.class))).andReturn(this.transformer);
        expect(this.transformer.createResponse(isA(ResultsPage.class))).andReturn(this.baseResponse);
        expect(subject.createQuery(queryLogicName, MapUtils.toMultivaluedMap(params), httpHeaders)).andReturn(createResponse);
        expect(this.cache.get(eq(queryId.toString()))).andReturn(this.runningQuery);
        expect(this.runningQuery.getMetric()).andReturn(this.queryMetric);
        expect(this.responseObjectFactory.getQueryImpl()).andReturn(new QueryImpl());
        this.queryMetric.setCreateCallTime(EasyMock.geq(0L));
        // return streaming response

        // Run the test
        PowerMock.replayAll();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, QueryMetricsBean.class, metrics);
        setInternalState(subject, Multimap.class, traceInfos);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        StreamingOutput result1 = subject.execute(queryLogicName, MapUtils.toMultivaluedMap(params), httpHeaders);
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a non-null response", result1);
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

        QueryExecutorBean subject = PowerMock.createPartialMock(QueryExecutorBean.class, "createQuery");

        // Set expectations of the create logic
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.httpHeaders.getAcceptableMediaTypes()).andReturn(mediaTypes).anyTimes();

        // Run the test
        PowerMock.replayAll();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, QueryMetricsBean.class, metrics);
        setInternalState(subject, Multimap.class, traceInfos);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        StreamingOutput result1 = null;
        try {
            MultivaluedMap<String,String> queryParameters = MapUtils.toMultivaluedMap(
                            DefaultQueryParameters.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                                            expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, systemFrom, parameters, trace));

            result1 = subject.execute(queryLogicName, queryParameters, httpHeaders);

            fail("Should have failed due to unsupported media type");
        } catch (DatawaveWebApplicationException e) {
            if (!(((QueryException) e.getCause()).getErrorCode().equals("500-13"))) {
                fail("Unknown exception type: " + e.getCause());
            }
        }
        PowerMock.verifyAll();

        // Verify results
        assertNull("Expected a non-null response", result1);
    }

    @Test
    public void testLookupUUID_happyPath() {
        UUIDType uuidType = PowerMock.createMock(UUIDType.class);
        BaseQueryResponse response = PowerMock.createMock(BaseQueryResponse.class);
        ManagedExecutorService executor = PowerMock.createMock(ManagedExecutorService.class);

        expect(uriInfo.getQueryParameters()).andReturn(new MultivaluedHashMap<>());
        expect(lookupUUIDUtil.getUUIDType("uuidType")).andReturn(uuidType);
        expect(uuidType.getQueryLogic(null)).andReturn("abc");
        expect(lookupUUIDUtil.createUUIDQueryAndNext(isA(GetUUIDCriteria.class))).andReturn(response);
        expect(response.getQueryId()).andReturn("11111");
        expect(context.getCallerPrincipal()).andReturn(principal);
        expect(executor.submit(isA(Runnable.class))).andReturn(null);

        PowerMock.replayAll();

        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(subject, LookupUUIDUtil.class, lookupUUIDUtil);
        setInternalState(subject, ManagedExecutorService.class, executor);

        subject.lookupUUID("uuidType", "1234567890", uriInfo, httpHeaders);

        PowerMock.verifyAll();
    }

    @Test
    public void testLookupUUID_closeFail() {
        QueryExecutorBean subject = PowerMock.createPartialMock(QueryExecutorBean.class, "close");
        UUIDType uuidType = PowerMock.createMock(UUIDType.class);
        BaseQueryResponse response = PowerMock.createMock(BaseQueryResponse.class);
        ManagedExecutorService executor = PowerMock.createMock(ManagedExecutorService.class);

        expect(uriInfo.getQueryParameters()).andReturn(new MultivaluedHashMap<>());
        expect(lookupUUIDUtil.getUUIDType("uuidType")).andReturn(uuidType);
        expect(uuidType.getQueryLogic(null)).andReturn("abc");
        expect(lookupUUIDUtil.createUUIDQueryAndNext(isA(GetUUIDCriteria.class))).andReturn(response);
        expect(response.getQueryId()).andReturn("11111");
        expect(context.getCallerPrincipal()).andReturn(principal);
        expect(executor.submit(isA(Runnable.class))).andThrow(new RejectedExecutionException("INTENTIONALLY THROWN TEST EXCEPTION: Async close rejected"));
        expect(subject.close("11111")).andReturn(null);

        PowerMock.replayAll();

        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(subject, LookupUUIDUtil.class, lookupUUIDUtil);
        setInternalState(subject, ManagedExecutorService.class, executor);
        setInternalState(subject, Logger.class, Logger.getLogger(QueryExecutorBean.class));

        subject.lookupUUID("uuidType", "1234567890", uriInfo, httpHeaders);

        PowerMock.verifyAll();
    }

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
        String userDN = "userDN";
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);
        List<String> dnList = Collections.singletonList(userDN);
        UUID queryId = UUID.randomUUID();

        HashMap<String,Collection<String>> authsMap = new HashMap<>();
        authsMap.put("userdn", Arrays.asList(queryAuthorizations));

        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putSingle(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        queryParameters.putSingle(QueryParameters.QUERY_STRING, query);
        queryParameters.putSingle(QueryParameters.QUERY_NAME, queryName);
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, DefaultQueryParameters.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.putSingle(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        queryParameters.putSingle("valid", "param");

        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);

        QueryParameters qp = new DefaultQueryParameters();
        qp.validate(queryParameters);

        MultivaluedMap<String,String> op = MapUtils.toMultivaluedMap(qp.getUnknownParameters(MapUtils.toMultiValueMap(queryParameters)));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());

        // Set expectations of the create logic
        queryLogic1.validate(queryParameters);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getMaxPageSize()).andReturn(1000).times(2);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.principal.getUserDN()).andReturn(userDNpair);
        expect(this.principal.getDNs()).andReturn(new String[] {userDN});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).andReturn(true);
        expect(this.queryLogic1.getAuditType(null)).andReturn(AuditType.PASSIVE);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        expect(persister.create(eq(userDNpair.subjectDN()), eq(dnList), eq(marking), eq(queryLogicName), eq(qp), eq(op))).andReturn(this.query);
        expect(this.queryLogic1.getAuditType(this.query)).andReturn(AuditType.PASSIVE);
        expect(this.queryLogic1.getConnectionPriority()).andReturn(Priority.NORMAL);
        expect(this.queryLogic1.getConnPoolName()).andReturn("connPool1");
        expect(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).andReturn(null);
        this.query.populateTrackingMap(null);
        this.connectionRequestBean.requestBegin(queryId.toString(), userDN.toLowerCase(), null);
        expect(this.connectionFactory.getClient(userDN.toLowerCase(), new ArrayList<>(0), "connPool1", Priority.NORMAL, null)).andReturn(this.client);
        this.connectionRequestBean.requestEnd(queryId.toString());
        expect(this.principal.getPrimaryUser()).andReturn(dwUser).anyTimes();
        expect(this.dwUser.getAuths()).andReturn(Collections.singleton(queryAuthorizations)).anyTimes();
        expect(this.principal.getProxiedUsers()).andReturn(Collections.singletonList(dwUser));
        expect(this.userOperations.getRemoteUser(this.principal)).andReturn(this.principal);
        this.queryLogic1.preInitialize(this.query, WSAuthorizationsUtil.buildAuthorizations(Collections.singleton(Sets.newHashSet("AUTH_1"))));
        expect(this.queryLogic1.getUserOperations()).andReturn(null);
        expect(this.query.getOwner()).andReturn(userSid).anyTimes();
        expect(this.query.getId()).andReturn(queryId).anyTimes();
        expect(this.query.getQuery()).andReturn(queryName).anyTimes();
        expect(this.query.getQueryLogicName()).andReturn(queryLogicName).anyTimes();
        expect(this.query.getBeginDate()).andReturn(null).anyTimes();
        expect(this.query.getEndDate()).andReturn(null).anyTimes();
        expect(this.query.getColumnVisibility()).andReturn(null).anyTimes();
        expect(this.query.getQueryAuthorizations()).andReturn(queryAuthorizations).anyTimes();
        expect(this.query.getQueryName()).andReturn(null).anyTimes();
        expect(this.query.getPagesize()).andReturn(0).anyTimes();
        expect(this.query.getExpirationDate()).andReturn(null).anyTimes();
        expect(this.query.getParameters()).andReturn((Set) Collections.emptySet()).anyTimes();
        expect(this.query.getUncaughtExceptionHandler()).andReturn(new QueryUncaughtExceptionHandler()).anyTimes();
        expect(this.query.getUserDN()).andReturn(userDN).anyTimes();

        expect(this.responseObjectFactory.getQueryImpl()).andReturn(new QueryImpl());
        expect(queryLogic1.getResultLimit(anyObject(QueryImpl.class))).andReturn(-1L);

        // Set expectations of the plan
        Authorizations queryAuths = new Authorizations(queryAuthorizations);
        expect(this.queryLogic1.getPlan(this.client, this.query, Collections.singleton(queryAuths), true, false)).andReturn("a query plan");

        // Set expectations of the cleanup
        this.connectionFactory.returnClient(this.client);
        EasyMock.expectLastCall().times(2);
        queryLogic1.close();
        EasyMock.expectLastCall();

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, UserOperationsBean.class, userOperations);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactoryImpl.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, QueryMetricsBean.class, metrics);
        setInternalState(subject, Multimap.class, traceInfos);
        setInternalState(subject, SecurityMarking.class, new ColumnVisibilitySecurityMarking());
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);
        GenericResponse<String> result1 = subject.planQuery(queryLogicName, queryParameters);
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a non-null response", result1);
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
        String userDN = "userDN";
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);
        List<String> dnList = Collections.singletonList(userDN);
        UUID queryId = UUID.randomUUID();

        HashMap<String,Collection<String>> authsMap = new HashMap<>();
        authsMap.put("userdn", Arrays.asList(queryAuthorizations));

        MultiValueMap<String,String> queryParameters = new LinkedMultiValueMap<>();
        queryParameters.set(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        queryParameters.set(QueryParameters.QUERY_STRING, query);
        queryParameters.set(QueryParameters.QUERY_NAME, queryName);
        queryParameters.set(QueryParameters.QUERY_BEGIN, DefaultQueryParameters.formatDate(beginDate));
        queryParameters.set(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        queryParameters.set(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expirationDate));
        queryParameters.set(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.set(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.set(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.set(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.set(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.set(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        queryParameters.set("valid", "param");
        queryParameters.set(QueryExecutorBean.EXPAND_VALUES, "true");
        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);

        QueryParameters qp = new DefaultQueryParameters();
        qp.validate(queryParameters);

        MultivaluedMap<String,String> op = MapUtils.toMultivaluedMap(qp.getUnknownParameters(queryParameters));
        // op.putSingle(PrivateAuditConstants.AUDIT_TYPE, AuditType.NONE.name());
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());

        // Set expectations of the create logic
        queryLogic1.validate(queryParameters);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getMaxPageSize()).andReturn(1000).times(2);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.principal.getUserDN()).andReturn(userDNpair);
        expect(this.principal.getDNs()).andReturn(new String[] {userDN});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).andReturn(true);
        expect(this.queryLogic1.getAuditType(null)).andReturn(AuditType.PASSIVE);
        expect(this.queryLogic1.getSelectors(this.query)).andReturn(null);
        expect(auditor.audit(anyObject())).andReturn(null);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        expect(persister.create(eq(userDNpair.subjectDN()), eq(dnList), eq(marking), eq(queryLogicName), eq(qp), eq(op))).andReturn(this.query);
        expect(this.queryLogic1.getAuditType(this.query)).andReturn(AuditType.PASSIVE);
        expect(this.queryLogic1.getConnectionPriority()).andReturn(Priority.NORMAL);
        expect(this.queryLogic1.getConnPoolName()).andReturn("connPool1");
        this.queryLogic1.preInitialize(this.query, WSAuthorizationsUtil.buildAuthorizations(Collections.singleton(Sets.newHashSet("AUTH_1"))));
        expect(this.queryLogic1.getUserOperations()).andReturn(null);
        expect(this.connectionFactory.getTrackingMap(isA(StackTraceElement[].class))).andReturn(null);
        this.query.populateTrackingMap(null);
        this.connectionRequestBean.requestBegin(queryId.toString(), userDN.toLowerCase(), null);
        expect(this.connectionFactory.getClient(userDN.toLowerCase(), new ArrayList<>(0), "connPool1", Priority.NORMAL, null)).andReturn(this.client);
        this.connectionRequestBean.requestEnd(queryId.toString());

        expect(this.principal.getPrimaryUser()).andReturn(dwUser).anyTimes();
        expect(this.dwUser.getAuths()).andReturn(Collections.singleton(queryAuthorizations)).anyTimes();
        expect(this.principal.getProxiedUsers()).andReturn(Collections.singletonList(dwUser));
        expect(this.userOperations.getRemoteUser(this.principal)).andReturn(this.principal);
        expect(this.query.getOwner()).andReturn(userSid).anyTimes();
        expect(this.query.getId()).andReturn(queryId).anyTimes();
        expect(this.query.getQuery()).andReturn(queryName).anyTimes();
        expect(this.query.getQueryLogicName()).andReturn(queryLogicName).anyTimes();
        expect(this.query.getBeginDate()).andReturn(null).anyTimes();
        expect(this.query.getEndDate()).andReturn(null).anyTimes();
        expect(this.query.getColumnVisibility()).andReturn(null).anyTimes();
        expect(this.query.getQueryAuthorizations()).andReturn(queryAuthorizations).anyTimes();
        expect(this.query.getQueryName()).andReturn(null).anyTimes();
        expect(this.query.getPagesize()).andReturn(0).anyTimes();
        expect(this.query.getExpirationDate()).andReturn(null).anyTimes();
        expect(this.query.getParameters()).andReturn((Set) Collections.emptySet()).anyTimes();
        expect(this.query.getUncaughtExceptionHandler()).andReturn(new QueryUncaughtExceptionHandler()).anyTimes();
        // this.metrics.updateMetric(isA(QueryMetric.class));
        // PowerMock.expectLastCall().times(2);
        expect(this.query.getUserDN()).andReturn(userDN).anyTimes();
        // expect(this.genericConfiguration.getQueryString()).andReturn(queryName).once();
        // expect(this.qlCache.poll(queryId.toString())).andReturn(null);

        expect(this.responseObjectFactory.getQueryImpl()).andReturn(new QueryImpl());
        expect(queryLogic1.getResultLimit(anyObject(QueryImpl.class))).andReturn(-1L);

        // Set expectations of the plan
        Authorizations queryAuths = new Authorizations(queryAuthorizations);
        expect(this.queryLogic1.getPlan(this.client, this.query, Collections.singleton(queryAuths), true, true)).andReturn("a query plan");

        // Set expectations of the cleanup
        this.connectionFactory.returnClient(this.client);
        EasyMock.expectLastCall().times(2);
        queryLogic1.close();
        EasyMock.expectLastCall();

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, UserOperationsBean.class, userOperations);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactoryImpl.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, QueryMetricsBean.class, metrics);
        setInternalState(subject, Multimap.class, traceInfos);
        setInternalState(subject, SecurityMarking.class, new ColumnVisibilitySecurityMarking());
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);
        GenericResponse<String> result1 = subject.planQuery(queryLogicName, MapUtils.toMultivaluedMap(queryParameters));
        PowerMock.verifyAll();

        // Verify results
        assertNotNull("Expected a non-null response", result1);
        assertEquals("a query plan", result1.getResult());
    }

    @Test(expected = DatawaveWebApplicationException.class)
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
        MultiValueMap<String,String> queryParameters = new LinkedMultiValueMap<>();
        queryParameters.set(QueryParameters.QUERY_STRING, query);
        queryParameters.set(QueryParameters.QUERY_NAME, queryName);
        queryParameters.set(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        queryParameters.set(QueryParameters.QUERY_BEGIN, DefaultQueryParameters.formatDate(beginDate));
        queryParameters.set(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        queryParameters.set(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expirationDate));
        queryParameters.set(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.set(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.set(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.set(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.set(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        queryParameters.set("valid", "param");

        expect(context.getCallerPrincipal()).andReturn(principal).anyTimes();
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).andReturn((QueryLogic) this.queryLogic1);
        queryLogic1.validate(queryParameters);
        expect(principal.getName()).andReturn(userName);
        expect(principal.getShortName()).andReturn(userSid);
        expect(principal.getUserDN()).andReturn(userDNpair);
        expect(principal.getDNs()).andReturn(new String[] {userDN});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).andReturn(true);
        expect(this.queryLogic1.getAuditType(null)).andReturn(AuditType.ACTIVE);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations)));
        expect(this.queryLogic1.getMaxPageSize()).andReturn(10).anyTimes();
        expect(queryLogic1.getSelectors(null)).andReturn(null);
        expect(this.responseObjectFactory.getQueryImpl()).andReturn(new QueryImpl());
        expect(queryLogic1.getResultLimit(anyObject(QueryImpl.class))).andReturn(-1L);
        expect(auditor.audit(EasyMock.anyObject())).andThrow(new JMSRuntimeException("EXPECTED TESTING EXCEPTION"));
        queryLogic1.close();

        PowerMock.replayAll();

        QueryExecutorBean executor = new QueryExecutorBean();
        setInternalState(executor, EJBContext.class, context);
        setInternalState(executor, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(executor, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(executor, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(executor, QueryCache.class, cache);
        setInternalState(executor, ClosedQueryCache.class, closedCache);
        setInternalState(executor, Persister.class, persister);
        setInternalState(executor, QueryLogicFactoryImpl.class, queryLogicFactory);
        setInternalState(executor, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(executor, AuditBean.class, auditor);
        setInternalState(executor, QueryMetricsBean.class, metrics);
        setInternalState(executor, Multimap.class, traceInfos);
        setInternalState(executor, SecurityMarking.class, new ColumnVisibilitySecurityMarking());
        setInternalState(executor, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(executor, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(executor, AccumuloConnectionRequestBean.class, connectionRequestBean);

        executor.createQuery(queryLogicName, MapUtils.toMultivaluedMap(queryParameters));

        PowerMock.verifyAll();
    }

    @Test(expected = DatawaveWebApplicationException.class)
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

        MultiValueMap<String,String> map = new LinkedMultiValueMap<>(qp.toMap());
        map.set(PrivateAuditConstants.AUDIT_TYPE, AuditType.PASSIVE.name());
        map.set(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        map.set(PrivateAuditConstants.COLUMN_VISIBILITY, authorization);
        map.set(PrivateAuditConstants.USER_DN, userDN);
        map.set(AuditParameters.AUDIT_ID, queryName);
        MultiValueMap<String,String> auditMap = new LinkedMultiValueMap();
        auditMap.putAll(map);

        // Set expectations
        expect(this.context.getUserTransaction()).andReturn(this.transaction).anyTimes();
        this.transaction.begin();
        expect(this.transaction.getStatus()).andReturn(Status.STATUS_ACTIVE).anyTimes();
        expect(this.context.getCallerPrincipal()).andReturn(this.principal);
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(sid);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(authorization)));
        expect(this.principal.getUserDN()).andReturn(userDNpair);
        expect(this.cache.get(queryName)).andReturn(null);
        expect(this.persister.findById(queryName)).andReturn(Arrays.asList((Query) this.query));
        expect(this.query.getQueryLogicName()).andReturn(queryLogicName).anyTimes();
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getConnectionPriority()).andReturn(Priority.NORMAL);
        expect(this.query.getQueryAuthorizations()).andReturn(authorization).anyTimes();
        expect(this.queryLogic1.getCollectQueryMetrics()).andReturn(false);
        expect(this.query.getUncaughtExceptionHandler()).andReturn(exceptionHandler).anyTimes();
        expect(this.exceptionHandler.getThrowable()).andReturn(null).anyTimes();
        expect(this.query.getOwner()).andReturn(sid).anyTimes();
        expect(this.query.getId()).andReturn(queryId).anyTimes();
        expect(this.query.getQuery()).andReturn(queryName).anyTimes();
        this.cache.put(eq(queryId.toString()), isA(RunningQuery.class));
        expect(this.cache.lock(queryName)).andReturn(true);
        expect(this.queryLogic1.getAuditType(this.query)).andReturn(AuditType.PASSIVE);
        expect(this.query.getUserDN()).andReturn(userDN).anyTimes();
        expect(this.query.toMap()).andReturn(map);
        expect(this.query.getColumnVisibility()).andReturn(authorization);
        expect(this.queryLogic1.getSelectors(this.query)).andReturn(new ArrayList<>());
        expect(this.auditor.audit(auditMap)).andReturn(null);
        expectLastCall().andThrow(new Exception("EXPECTED EXCEPTION IN AUDIT"));
        cache.unlock(queryName);
        transaction.commit();
        expect(this.queryLogic1.getLogicName()).andReturn(queryLogicName);

        // Run the test
        PowerMock.replayAll();

        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, QueryMetricsBean.class, metrics);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);

        subject.reset(queryName);

        PowerMock.verifyAll();
    }

    @Test(expected = DatawaveWebApplicationException.class)
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
        Query duplicateQuery = PowerMock.createMock(Query.class);

        MultiValueMap<String,String> p = new LinkedMultiValueMap<>();
        p.set(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        p.set(QueryParameters.QUERY_BEGIN, DefaultQueryParameters.formatDate(beginDate));
        p.set(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        p.set(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expirationDate));
        p.set(QueryParameters.QUERY_NAME, queryName);
        p.set(QueryParameters.QUERY_PAGESIZE, Integer.toString(pagesize));
        p.set(QueryParameters.QUERY_PAGETIMEOUT, Integer.toString(pageTimeout));
        p.set(QueryParameters.QUERY_STRING, query);
        p.set(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        p.set(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        p.set(QueryParameters.QUERY_PARAMS, parameters);

        p.set(PrivateAuditConstants.AUDIT_TYPE, AuditType.LOCALONLY.name());
        p.set(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        p.set(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        p.set(PrivateAuditConstants.USER_DN, userDN);
        p.set(AuditParameters.AUDIT_ID, queryId.toString());
        MultiValueMap<String,String> auditMap = new LinkedMultiValueMap();
        auditMap.putAll(p);

        // Set expectations
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName).times(2);
        expect(this.principal.getShortName()).andReturn(userSid).times(2);
        expect(this.principal.getAuthorizations()).andReturn((Collection) Arrays.asList(Arrays.asList(queryAuthorizations))).times(2);
        expect(this.cache.get(queryId.toString())).andReturn(this.runningQuery);
        expect(this.runningQuery.getSettings()).andReturn(this.query).anyTimes();
        expect(this.query.getOwner()).andReturn(userSid);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).andReturn((QueryLogic) this.queryLogic1).times(1);
        expect(this.queryLogic1.getLogicName()).andReturn(queryLogicName).times(1);
        expect(this.query.duplicate(queryName)).andReturn(duplicateQuery);
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
        expect(duplicateQuery.toMap()).andReturn(p);
        expect(duplicateQuery.getColumnVisibility()).andReturn(queryVisibility);
        expect(duplicateQuery.getUserDN()).andReturn(userDN);
        expect(this.query.getQueryName()).andReturn(queryName).times(2);
        expect(this.query.getId()).andReturn(queryId).times(2);
        expect(this.runningQuery.getLogic()).andReturn((QueryLogic) this.queryLogic1);
        expect(this.queryLogic1.getAuditType(this.query)).andReturn(AuditType.LOCALONLY);
        expect(this.query.getQueryAuthorizations()).andReturn(queryAuthorizations);

        expect(this.auditor.audit(eq(auditMap))).andThrow(new Exception("INTENTIONALLY THROWN EXCEPTION"));

        // Run the test
        PowerMock.replayAll();

        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());

        subject.updateQuery(queryId.toString(), queryLogicName, query, queryVisibility, beginDate, endDate, queryAuthorizations, expirationDate, pagesize,
                        pageTimeout, maxResultsOverride, persistenceMode, parameters);
        PowerMock.verifyAll();
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
        String systemFrom = null;
        String parameters = null;
        boolean trace = false;
        // Set expectations

        MultivaluedMap<String,String> queryParameters = MapUtils.toMultivaluedMap(
                        DefaultQueryParameters.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                                        expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, systemFrom, parameters, trace));

        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);

        QueryParameters qp = new DefaultQueryParameters();
        qp.validate(queryParameters);

        MultivaluedMap<String,String> op = MapUtils.toMultivaluedMap(qp.getUnknownParameters(MapUtils.toMultiValueMap(queryParameters)));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();

        queryLogic1.validate(queryParameters);
        expect(this.principal.getName()).andReturn("userName Full");
        expect(this.principal.getShortName()).andReturn("userName");
        expect(this.principal.getUserDN()).andReturn(SubjectIssuerDNPair.of("userDN"));
        expect(this.principal.getDNs()).andReturn(new String[] {"userDN"});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.queryLogic1.containsDNWithAccess(Collections.singletonList("userDN"))).andReturn(false);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, SecurityMarking.class, new ColumnVisibilitySecurityMarking());
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        Throwable result1 = null;
        try {
            subject.defineQuery(queryLogicName, queryParameters);
        } catch (DatawaveWebApplicationException e) {
            result1 = e.getCause();
        }
        PowerMock.verifyAll();

        // Verify results
        assertTrue("QueryException expected to have been thrown", result1 instanceof QueryException);
        assertEquals("Thrown exception expected to have been due to access denied", "401", ((QueryException) result1).getErrorCode());
        assertEquals("Thrown exception expected to detail reason for access denial", "None of the DNs used have access to this query logic: [userDN]",
                        result1.getMessage());
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
        String systemFrom = null;
        String parameters = null;
        boolean trace = false;
        // Set expectations

        MultivaluedMap<String,String> queryParameters = MapUtils.toMultivaluedMap(
                        DefaultQueryParameters.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                                        expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, systemFrom, parameters, trace));

        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);

        QueryParameters qp = new DefaultQueryParameters();
        qp.validate(queryParameters);

        MultivaluedMap<String,String> op = MapUtils.toMultivaluedMap(qp.getUnknownParameters(MapUtils.toMultiValueMap(queryParameters)));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();

        queryLogic1.validate(queryParameters);
        expect(this.principal.getName()).andReturn("userName Full");
        expect(this.principal.getShortName()).andReturn("userName");
        expect(this.principal.getUserDN()).andReturn(SubjectIssuerDNPair.of("userDN"));
        expect(this.principal.getDNs()).andReturn(new String[] {"userDN"});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.queryLogic1.containsDNWithAccess(Collections.singletonList("userDN"))).andReturn(false);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, SecurityMarking.class, new ColumnVisibilitySecurityMarking());
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        Throwable result1 = null;
        try {
            subject.createQuery(queryLogicName, queryParameters);
        } catch (DatawaveWebApplicationException e) {
            result1 = e.getCause();
        }
        PowerMock.verifyAll();

        // Verify results
        assertTrue("QueryException expected to have been thrown", result1 instanceof QueryException);
        assertEquals("Thrown exception expected to have been due to access denied", "401", ((QueryException) result1).getErrorCode());
        assertEquals("Thrown exception expected to detail reason for access denial", "None of the DNs used have access to this query logic: [userDN]",
                        result1.getMessage());
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
        String systemFrom = null;
        String parameters = null;
        boolean trace = false;
        // Set expectations

        MultivaluedMap<String,String> queryParameters = MapUtils.toMultivaluedMap(
                        DefaultQueryParameters.paramsToMap(queryLogicName, query, queryName, queryVisibility, beginDate, endDate, queryAuthorizations,
                                        expirationDate, pagesize, pageTimeout, maxResultsOverride, persistenceMode, systemFrom, parameters, trace));

        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);

        QueryParameters qp = new DefaultQueryParameters();
        qp.validate(queryParameters);

        MultivaluedMap<String,String> op = MapUtils.toMultivaluedMap(qp.getUnknownParameters(MapUtils.toMultiValueMap(queryParameters)));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();

        queryLogic1.validate(queryParameters);
        expect(this.principal.getName()).andReturn("userName Full");
        expect(this.principal.getShortName()).andReturn("userName");
        expect(this.principal.getUserDN()).andReturn(SubjectIssuerDNPair.of("userDN"));
        expect(this.principal.getDNs()).andReturn(new String[] {"userDN"});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.queryLogic1.containsDNWithAccess(Collections.singletonList("userDN"))).andReturn(false);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, SecurityMarking.class, new ColumnVisibilitySecurityMarking());
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        Throwable result1 = null;
        try {
            subject.createQueryAndNext(queryLogicName, queryParameters);
        } catch (DatawaveWebApplicationException e) {
            result1 = e.getCause();
        }
        PowerMock.verifyAll();

        // Verify results
        assertTrue("QueryException expected to have been thrown", result1 instanceof QueryException);
        assertEquals("Thrown exception expected to have been due to access denied", "401", ((QueryException) result1).getErrorCode());
        assertEquals("Thrown exception expected to detail reason for access denial", "None of the DNs used have access to this query logic: [userDN]",
                        result1.getMessage());
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
        String userDN = "userDN";
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);

        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putSingle(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        queryParameters.putSingle(QueryParameters.QUERY_STRING, query);
        queryParameters.putSingle(QueryParameters.QUERY_NAME, queryName);
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, DefaultQueryParameters.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.putSingle(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        queryParameters.putSingle("valid", "param");

        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);

        QueryParameters qp = new DefaultQueryParameters();
        qp.validate(queryParameters);

        MultivaluedMap<String,String> op = MapUtils.toMultivaluedMap(qp.getUnknownParameters(MapUtils.toMultiValueMap(queryParameters)));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());

        // Set expectations of the create logic
        queryLogic1.validate(queryParameters);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.principal.getUserDN()).andReturn(userDNpair);
        expect(this.principal.getDNs()).andReturn(new String[] {userDN});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).andReturn(false);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactoryImpl.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, QueryMetricsBean.class, metrics);
        setInternalState(subject, Multimap.class, traceInfos);
        setInternalState(subject, SecurityMarking.class, new ColumnVisibilitySecurityMarking());
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);
        Throwable result1 = null;
        try {
            subject.planQuery(queryLogicName, queryParameters);
        } catch (DatawaveWebApplicationException e) {
            result1 = e.getCause();
        }
        PowerMock.verifyAll();

        // Verify results
        assertTrue("QueryException expected to have been thrown", result1 instanceof QueryException);
        assertEquals("Thrown exception expected to have been due to access denied", "401", ((QueryException) result1).getErrorCode());
        assertEquals("Thrown exception expected to detail reason for access denial", "None of the DNs used have access to this query logic: [userDN]",
                        result1.getMessage());
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
        String userDN = "userDN";
        SubjectIssuerDNPair userDNpair = SubjectIssuerDNPair.of(userDN);

        MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putSingle(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        queryParameters.putSingle(QueryParameters.QUERY_STRING, query);
        queryParameters.putSingle(QueryParameters.QUERY_NAME, queryName);
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, DefaultQueryParameters.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, queryAuthorizations);
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.putSingle(QueryParameters.QUERY_PAGETIMEOUT, String.valueOf(pageTimeout));
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persistenceMode.name());
        queryParameters.putSingle(QueryParameters.QUERY_TRACE, String.valueOf(trace));
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, queryVisibility);
        queryParameters.putSingle("valid", "param");

        ColumnVisibilitySecurityMarking marking = new ColumnVisibilitySecurityMarking();
        marking.validate(queryParameters);

        QueryParameters qp = new DefaultQueryParameters();
        qp.validate(queryParameters);

        MultivaluedMap<String,String> op = MapUtils.toMultivaluedMap(qp.getUnknownParameters(MapUtils.toMultiValueMap(queryParameters)));
        op.putSingle(PrivateAuditConstants.LOGIC_CLASS, queryLogicName);
        op.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, queryVisibility);
        op.putSingle(PrivateAuditConstants.USER_DN, userDNpair.subjectDN());

        // Set expectations of the create logic
        queryLogic1.validate(queryParameters);
        expect(this.queryLogicFactory.getQueryLogic(queryLogicName, this.principal)).andReturn((QueryLogic) this.queryLogic1);
        expect(this.context.getCallerPrincipal()).andReturn(this.principal).anyTimes();
        expect(this.principal.getName()).andReturn(userName);
        expect(this.principal.getShortName()).andReturn(userSid);
        expect(this.principal.getUserDN()).andReturn(userDNpair);
        expect(this.principal.getDNs()).andReturn(new String[] {userDN});
        expect(this.principal.getProxyServers()).andReturn(new ArrayList<>(0)).anyTimes();
        expect(this.queryLogic1.containsDNWithAccess(Collections.singletonList(userDN))).andReturn(false);

        // Run the test
        PowerMock.replayAll();
        QueryExecutorBean subject = new QueryExecutorBean();
        setInternalState(subject, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(subject, ResponseObjectFactory.class, responseObjectFactory);
        setInternalState(subject, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(subject, QueryCache.class, cache);
        setInternalState(subject, ClosedQueryCache.class, closedCache);
        setInternalState(subject, Persister.class, persister);
        setInternalState(subject, QueryLogicFactoryImpl.class, queryLogicFactory);
        setInternalState(subject, QueryExpirationProperties.class, queryExpirationConf);
        setInternalState(subject, AuditBean.class, auditor);
        setInternalState(subject, QueryMetricsBean.class, metrics);
        setInternalState(subject, Multimap.class, traceInfos);
        setInternalState(subject, SecurityMarking.class, new ColumnVisibilitySecurityMarking());
        setInternalState(subject, QueryParameters.class, new DefaultQueryParameters());
        setInternalState(subject, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(connectionRequestBean, EJBContext.class, context);
        setInternalState(subject, AccumuloConnectionRequestBean.class, connectionRequestBean);
        Throwable result1 = null;
        try {
            subject.predictQuery(queryLogicName, queryParameters);
        } catch (DatawaveWebApplicationException e) {
            result1 = e.getCause();
        }
        PowerMock.verifyAll();

        // Verify results
        assertTrue("QueryException expected to have been thrown", result1 instanceof QueryException);
        assertEquals("Thrown exception expected to have been due to access denied", "401", ((QueryException) result1).getErrorCode());
        assertEquals("Thrown exception expected to detail reason for access denial", "None of the DNs used have access to this query logic: [userDN]",
                        result1.getMessage());
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
        public GenericQueryConfiguration initialize(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
            return null;
        }

        @Override
        public void setupQuery(GenericQueryConfiguration configuration) throws Exception {
            // No op
        }

        @Override
        public String getPlan(AccumuloClient client, Query settings, Set<Authorizations> runtimeQueryAuthorizations, boolean expandFields, boolean expandValues)
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

    public void populateMetric(Query query, QueryMetric qm) {
        qm.setQueryType(query.getClass());
        qm.setQueryId(query.getId().toString());
        qm.setUser(query.getOwner());
        qm.setUserDN(query.getUserDN());
        qm.setQuery(query.getQuery());
        qm.setQueryLogic(query.getQueryLogicName());
        qm.setBeginDate(query.getBeginDate());
        qm.setEndDate(query.getEndDate());
        qm.setQueryAuthorizations(query.getQueryAuthorizations());
        qm.setQueryName(query.getQueryName());
        qm.setParameters(query.getParameters());
        qm.setColumnVisibility(query.getColumnVisibility());
    }

}
