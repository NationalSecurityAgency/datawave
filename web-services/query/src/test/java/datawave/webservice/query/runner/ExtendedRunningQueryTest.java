package datawave.webservice.query.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.functors.NOPTransformer;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.connection.AccumuloConnectionFactory.Priority;
import datawave.core.query.cache.ResultsPage;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.QueryLogic;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.microservice.query.Query;
import datawave.microservice.querymetric.QueryMetric;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnProperties;
import datawave.webservice.query.cache.RunningQueryTimingImpl;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;

@ExtendWith(MockitoExtension.class)
public class ExtendedRunningQueryTest {

    AccumuloClient client = mock(AccumuloClient.class);
    AccumuloConnectionFactory connectionFactory = mock(AccumuloConnectionFactory.class);
    GenericQueryConfiguration genericConfiguration = mock(GenericQueryConfiguration.class);
    Query query = mock(Query.class);
    QueryUncaughtExceptionHandler exceptionHandler = mock(QueryUncaughtExceptionHandler.class);
    QueryLogic<?> queryLogic = mock(QueryLogic.class);
    QueryMetricsBean queryMetrics = mock(QueryMetricsBean.class);
    TransformIterator transformIterator = mock(TransformIterator.class);

    private ExecutorService executor;

    private final Transformer<?,?> transformer = NOPTransformer.nopTransformer();

    @BeforeEach
    public void beforeEach() {
        System.setProperty(DnProperties.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    public void afterEach() {
        if (executor != null) {
            executor.shutdown();
            executor = null;
        }
    }

    @Test
    public void testConstructor_NoArg() throws Exception {
        RunningQuery subject = new RunningQuery();
        Exception result1 = null;
        try {
            subject.next();
        } catch (NullPointerException e) {
            result1 = e;
        }
        AccumuloClient result2 = subject.getClient();
        Priority result3 = subject.getConnectionPriority();
        QueryLogic<?> result4 = subject.getLogic();
        Query result5 = subject.getSettings();
        TransformIterator<?,?> result6 = subject.getTransformIterator();
        Set<Authorizations> result7 = subject.getCalculatedAuths();

        // Verify results
        assertNotNull(result1, "Expected an exception to be thrown due to uninitialized instance variables");
        assertNull(result2, "Expected a null connector");
        assertNull(result3, "Expected a null priority");
        assertNull(result4, "Expected null logic");
        assertNull(result5, "Expected a null query (a.k.a. settings)");
        assertNull(result6, "Expected a null iterator");
        assertNull(result7, "Expected a null set of authorizations");
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testNext_HappyPathUsingDeprecatedConstructor() throws Exception {

        // Set local test input
        String userDN = "userDN";
        String userSid = "userSid";
        UUID queryId = UUID.randomUUID();
        String methodAuths = "AUTH_1";
        String columnVisibility = "AUTH_1";
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of("userDN", "issuerDN"), UserType.USER, Collections.singleton(methodAuths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        String query = "query";
        String queryLogicName = "queryLogicName";
        String queryName = "queryName";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        Date expirationDate = new Date(currentTime + 9999);
        int pageSize = 3;
        int maxPageSize = 10;
        long pageByteTrigger = 4 * 1024L;
        long maxWork = Long.MAX_VALUE;
        long maxResults = 100L;

        // Set expectations
        when(this.queryLogic.getCollectQueryMetrics()).thenReturn(true);
        when(this.query.getUncaughtExceptionHandler()).thenReturn(exceptionHandler);
        when(this.exceptionHandler.getUncaughtException()).thenReturn(null);
        when(this.query.getId()).thenReturn(queryId);
        when(this.query.getOwner()).thenReturn(userSid);
        when(this.query.getQuery()).thenReturn(query);
        when(this.query.getQueryLogicName()).thenReturn(queryLogicName);
        when(this.query.getQueryName()).thenReturn(queryName);

        when(this.query.getBeginDate()).thenReturn(beginDate);
        when(this.query.getEndDate()).thenReturn(endDate);
        when(this.query.isMaxResultsOverridden()).thenReturn(false);
        when(this.query.getExpirationDate()).thenReturn(expirationDate);
        when(this.query.getParameters()).thenReturn(new HashSet<>());
        when(this.query.getQueryAuthorizations()).thenReturn(methodAuths);
        when(this.query.getColumnVisibility()).thenReturn(columnVisibility);
        when(this.query.getUserDN()).thenReturn(userDN);
        when(this.queryLogic.initialize(eq(this.client), eq(this.query), isA(Set.class))).thenReturn(this.genericConfiguration);
        this.queryLogic.setupQuery(this.genericConfiguration);

        when(this.queryLogic.getTransformIterator(this.query)).thenReturn(this.transformIterator);

        when(this.transformIterator.hasNext()).thenReturn(true, true, true, false);
        when(this.transformIterator.next()).thenReturn(new Object(), "resultObject1", null);
        when(this.transformIterator.getTransformer()).thenReturn(transformer);

        when(this.query.getPagesize()).thenReturn(pageSize);
        when(this.queryLogic.getMaxPageSize()).thenReturn(maxPageSize);
        when(this.queryLogic.getPageByteTrigger()).thenReturn(pageByteTrigger);
        when(this.queryLogic.getMaxWork()).thenReturn(maxWork);
        when(this.queryLogic.getMaxResults()).thenReturn(maxResults);
        when(this.genericConfiguration.getQueryString()).thenReturn(query);
        when(this.queryLogic.isLongRunningQuery()).thenReturn(false);
        when(this.queryLogic.getResultLimit(eq(this.query))).thenReturn(maxResults);
        this.queryLogic.preInitialize(this.query, AuthorizationsUtil.buildAuthorizations(Collections.singleton(Collections.singleton("AUTH_1"))));
        when(this.queryLogic.getUserOperations()).thenReturn(null);
        this.queryLogic.setPageProcessingStartTime(anyLong());

        RunningQuery subject = new RunningQuery(this.client, Priority.NORMAL, this.queryLogic, this.query, methodAuths, principal,
                        new QueryMetricFactoryImpl());
        subject.setExecutor(executor);

        ResultsPage result1 = subject.next();
        String result2 = subject.toString();
        QueryMetric.Lifecycle status = subject.getMetric().getLifecycle();

        // Verify results
        assertNotNull(result1, "Expected a non-null page");
        assertNotNull(result1.getResults(), "Expected a non-null list of results");
        assertEquals(2, result1.getResults().size(), "Expected 2 non-null items in the list of results");
        assertEquals(QueryMetric.Lifecycle.RESULTS, status, "Expected status to be closed");

        assertNotNull(result2, "Expected a non-null toString() representation");

        assertEquals(QueryMetric.Lifecycle.RESULTS, subject.getMetric().getLifecycle(), "Expected lifecycle to be results");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNext_HappyPathUsingTimingConstructor() throws Exception {

        // Set local test input
        String userDN = "userDN";
        String userSid = "userSid";
        UUID queryId = UUID.randomUUID();
        String methodAuths = "AUTH_1";
        String columnVisibility = "AUTH_1";
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of("userDN", "issuerDN"), UserType.USER, Collections.singleton(methodAuths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        String query = "query";
        String queryLogicName = "queryLogicName";
        String queryName = "queryName";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        Date expirationDate = new Date(currentTime + 9999);
        int pageSize = 3;
        int maxPageSize = 10;
        long pageByteTrigger = 4 * 1024L;
        long maxWork = Long.MAX_VALUE;
        long maxResults = 100L;

        // Set expectations
        when(this.queryLogic.getCollectQueryMetrics()).thenReturn(true);
        when(this.query.getUncaughtExceptionHandler()).thenReturn(exceptionHandler);
        when(this.exceptionHandler.getUncaughtException()).thenReturn(null);
        when(this.query.getId()).thenReturn(queryId);
        when(this.query.getOwner()).thenReturn(userSid);
        when(this.query.getQuery()).thenReturn(query);
        when(this.query.getQueryLogicName()).thenReturn(queryLogicName);
        when(this.query.getQueryName()).thenReturn(queryName);

        when(this.query.getBeginDate()).thenReturn(beginDate);
        when(this.query.getEndDate()).thenReturn(endDate);
        when(this.query.isMaxResultsOverridden()).thenReturn(false);
        when(this.query.getExpirationDate()).thenReturn(expirationDate);
        when(this.query.getParameters()).thenReturn(new HashSet<>());
        when(this.query.getQueryAuthorizations()).thenReturn(methodAuths);
        when(this.query.getColumnVisibility()).thenReturn(columnVisibility);
        when(this.query.getUserDN()).thenReturn(userDN);
        when(this.queryLogic.initialize(eq(this.client), eq(this.query), isA(Set.class))).thenReturn(this.genericConfiguration);
        this.queryLogic.setupQuery(this.genericConfiguration);
        when(this.queryLogic.getTransformIterator(this.query)).thenReturn(this.transformIterator);

        when(this.transformIterator.hasNext()).thenReturn(true);
        when(this.transformIterator.next()).thenReturn(new Object(), "resultObject1", null);
        when(this.transformIterator.getTransformer()).thenReturn(transformer);

        when(this.query.getPagesize()).thenReturn(pageSize);
        when(this.queryLogic.getMaxPageSize()).thenReturn(maxPageSize);
        when(this.queryLogic.getPageByteTrigger()).thenReturn(pageByteTrigger);
        when(this.queryLogic.getMaxWork()).thenReturn(maxWork);
        when(this.queryLogic.getMaxResults()).thenReturn(maxResults);
        when(this.genericConfiguration.getQueryString()).thenReturn(query);
        when(this.queryLogic.isLongRunningQuery()).thenReturn(false);
        when(this.queryLogic.getResultLimit(eq(this.query))).thenReturn(maxResults);
        this.queryLogic.preInitialize(this.query, AuthorizationsUtil.buildAuthorizations(Collections.singleton(Collections.singleton("AUTH_1"))));
        when(this.queryLogic.getUserOperations()).thenReturn(null);
        this.queryLogic.setPageProcessingStartTime(anyLong());

        RunningQuery subject = new RunningQuery(this.client, Priority.NORMAL, this.queryLogic, this.query, methodAuths, principal,
                        new RunningQueryTimingImpl(3600, 1200, 3500, 10), new QueryMetricFactoryImpl());
        subject.setExecutor(executor);

        ResultsPage<?> result1 = subject.next();
        String result2 = subject.toString();
        QueryMetric.Lifecycle status = subject.getMetric().getLifecycle();

        // Verify results
        assertNotNull(result1, "Expected a non-null page");
        assertNotNull(result1.getResults(), "Expected a non-null list of results");
        assertEquals(2, result1.getResults().size(), "Expected 2 non-null items in the list of results");
        assertEquals(QueryMetric.Lifecycle.RESULTS, status, "Expected status to be closed");

        assertNotNull(result2, "Expected a non-null toString() representation");

        assertEquals(QueryMetric.Lifecycle.RESULTS, subject.getMetric().getLifecycle(), "Expected lifecycle to be results");
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testNextMaxResults_HappyPathUsingDeprecatedConstructor() throws Exception {
        // Set local test input
        String userDN = "userDN";
        String userSid = "userSid";
        UUID queryId = UUID.randomUUID();
        String methodAuths = "AUTH_1";
        String columnVisibility = "AUTH_1";
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of("userDN", "issuerDN"), UserType.USER, Collections.singleton(methodAuths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        String query = "query";
        String queryLogicName = "queryLogicName";
        String queryName = "queryName";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        Date expirationDate = new Date(currentTime + 9999);
        int pageSize = 5;
        int maxPageSize = 5;

        long pageByteTrigger = 4 * 1024L;
        long maxWork = Long.MAX_VALUE;
        long maxResults = 4L;

        // Set expectations
        when(this.queryLogic.getCollectQueryMetrics()).thenReturn(true);
        when(this.query.getUncaughtExceptionHandler()).thenReturn(exceptionHandler);
        when(this.exceptionHandler.getUncaughtException()).thenReturn(null);
        when(this.query.getId()).thenReturn(queryId);
        when(this.query.getOwner()).thenReturn(userSid);
        when(this.query.getQuery()).thenReturn(query);
        when(this.query.getQueryLogicName()).thenReturn(queryLogicName);
        when(this.query.getQueryName()).thenReturn(queryName);
        when(this.query.getBeginDate()).thenReturn(beginDate);
        when(this.query.getEndDate()).thenReturn(endDate);
        when(this.query.isMaxResultsOverridden()).thenReturn(false);
        when(this.query.getExpirationDate()).thenReturn(expirationDate);
        when(this.query.getParameters()).thenReturn(new HashSet<>());
        when(this.query.getQueryAuthorizations()).thenReturn(methodAuths);
        when(this.query.getUserDN()).thenReturn(userDN);
        when(this.query.getColumnVisibility()).thenReturn(columnVisibility);
        when(this.queryLogic.initialize(eq(this.client), eq(this.query), isA(Set.class))).thenReturn(this.genericConfiguration);
        this.queryLogic.setupQuery(this.genericConfiguration);
        when(this.queryLogic.getTransformIterator(this.query)).thenReturn(this.transformIterator);
        when(this.queryLogic.isLongRunningQuery()).thenReturn(false);
        when(this.queryLogic.getResultLimit(eq(this.query))).thenReturn(maxResults);

        List<Object> resultObjects = Arrays.asList(new Object(), "resultObject1", "resultObject2", "resultObject3", "resultObject4", "resultObject5");
        when(this.transformIterator.hasNext()).thenReturn(true, true, true, true, true, true, false);
        when(this.transformIterator.next()).thenReturn(resultObjects.get(0), resultObjects.get(1), resultObjects.get(2), resultObjects.get(3),
                        resultObjects.get(4), resultObjects.get(5));

        when(this.transformIterator.getTransformer()).thenReturn(transformer);

        when(this.query.getPagesize()).thenReturn(pageSize);
        when(this.queryLogic.getMaxPageSize()).thenReturn(maxPageSize);
        when(this.queryLogic.getPageByteTrigger()).thenReturn(pageByteTrigger);
        when(this.queryLogic.getMaxWork()).thenReturn(maxWork);
        when(this.queryLogic.getMaxResults()).thenReturn(maxResults);
        this.queryLogic.preInitialize(this.query, AuthorizationsUtil.buildAuthorizations(Collections.singleton(Collections.singleton("AUTH_1"))));
        when(this.queryLogic.getUserOperations()).thenReturn(null);
        when(this.genericConfiguration.getQueryString()).thenReturn(query);
        this.queryLogic.setPageProcessingStartTime(anyLong());

        RunningQuery subject = new RunningQuery(this.client, Priority.NORMAL, this.queryLogic, this.query, methodAuths, principal,
                        new QueryMetricFactoryImpl());
        subject.setExecutor(executor);

        ResultsPage result1 = subject.next();

        String result2 = subject.toString();
        QueryMetric.Lifecycle status = subject.getMetric().getLifecycle();

        // Verify results
        assertNotNull(result1, "Expected a non-null page");
        assertNotNull(result1.getResults(), "Expected a non-null list of results");
        assertEquals(QueryMetric.Lifecycle.MAXRESULTS, status, "Expected status to be MAXRESULTS");

        assertNotNull(result2, "Expected a non-null toString() representation");
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testNext_NoResultsAfterCancellationUsingDeprecatedConstructor() throws Exception {
        // Set local test input
        String userDN = "userDN";
        String userSid = "userSid";
        UUID queryId = UUID.randomUUID();
        String query = "query";
        String queryLogicName = "queryLogicName";
        String queryName = "queryName";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        String methodAuths = "AUTH_1";
        String columnVisibility = "AUTH_1";
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of("userDN", "issuerDN"), UserType.USER, Collections.singleton(methodAuths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        long maxResults = 100L;
        int pageSize = 5;
        int maxPageSize = 5;

        // Set expectations
        when(this.queryLogic.getCollectQueryMetrics()).thenReturn(true);
        when(this.query.getUncaughtExceptionHandler()).thenReturn(exceptionHandler);
        when(this.exceptionHandler.getUncaughtException()).thenReturn(null);
        when(this.query.getId()).thenReturn(queryId);
        when(this.query.getUserDN()).thenReturn(userDN);
        when(this.query.getOwner()).thenReturn(userSid);
        when(this.query.getQuery()).thenReturn(query);
        when(this.query.getQueryLogicName()).thenReturn(queryLogicName);
        when(this.query.getQueryName()).thenReturn(queryName);
        when(this.query.getBeginDate()).thenReturn(beginDate);
        when(this.query.getEndDate()).thenReturn(endDate);
        when(this.query.getParameters()).thenReturn(new HashSet<>());
        when(this.query.getQueryAuthorizations()).thenReturn(methodAuths);
        when(this.query.getColumnVisibility()).thenReturn(columnVisibility);
        when(this.queryLogic.initialize(eq(this.client), eq(this.query), isA(Set.class))).thenReturn(this.genericConfiguration);
        this.queryLogic.setupQuery(this.genericConfiguration);
        this.queryMetrics.updateMetric(isA(QueryMetric.class));

        when(this.queryLogic.getTransformIterator(this.query)).thenReturn(this.transformIterator);
        when(this.transformIterator.hasNext()).thenReturn(true);
        when(this.genericConfiguration.getQueryString()).thenReturn("query");
        when(this.queryLogic.isLongRunningQuery()).thenReturn(false);
        when(this.queryLogic.getResultLimit(eq(this.query))).thenReturn(maxResults);
        when(this.queryLogic.getMaxResults()).thenReturn(maxResults);
        when(this.query.getPagesize()).thenReturn(pageSize);
        when(this.queryLogic.getMaxPageSize()).thenReturn(maxPageSize);
        this.queryLogic.preInitialize(this.query, AuthorizationsUtil.buildAuthorizations(Collections.singleton(Collections.singleton("AUTH_1"))));
        when(this.queryLogic.getUserOperations()).thenReturn(null);
        this.queryLogic.setPageProcessingStartTime(anyLong());

        RunningQuery subject = new RunningQuery(this.queryMetrics, this.client, Priority.NORMAL, this.queryLogic, this.query, methodAuths, principal,
                        new QueryMetricFactoryImpl());
        subject.setExecutor(executor);
        subject.cancel();
        boolean result1 = subject.isCanceled();
        ResultsPage result2 = subject.next();

        // Verify results
        assertTrue(result1, "Expected isCanceled() to return true");

        assertNotNull(result2, "Expected a non-null page");
        assertNotNull(result2.getResults(), "Expected a non-null list of results");
        assertTrue(result2.getResults().isEmpty(), "Expected an empty list of results");
        assertEquals(QueryMetric.Lifecycle.CANCELLED, subject.getMetric().getLifecycle(), "Expected status to be cancelled");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCloseConnection_HappyPath() throws Exception {
        // Set local test input
        String userDN = "userDN";
        UUID queryId = UUID.randomUUID();
        String methodAuths = "AUTH_1";
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of("userDN", "issuerDN"), UserType.USER, Collections.singleton(methodAuths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        long maxResults = 100L;

        // Set expectations
        when(this.transformIterator.getTransformer()).thenReturn(transformer);
        when(this.queryLogic.getCollectQueryMetrics()).thenReturn(true);
        when(this.query.getUncaughtExceptionHandler()).thenReturn(exceptionHandler);
        when(this.exceptionHandler.getUncaughtException()).thenReturn(null);
        when(this.query.getId()).thenReturn(queryId);
        when(this.query.getUserDN()).thenReturn(userDN);
        when(this.query.getOwner()).thenReturn(null);
        when(this.query.getQuery()).thenReturn(null);
        when(this.query.getQueryLogicName()).thenReturn(null);
        when(this.query.getQueryName()).thenReturn(null);
        when(this.query.getBeginDate()).thenReturn(null);
        when(this.query.getEndDate()).thenReturn(null);
        when(this.query.getParameters()).thenReturn(new HashSet<>());
        when(this.query.getQueryAuthorizations()).thenReturn(null);
        when(this.query.getColumnVisibility()).thenReturn(null);
        when(this.queryLogic.initialize(eq(this.client), eq(this.query), isA(Set.class))).thenReturn(this.genericConfiguration);
        when(this.genericConfiguration.getQueryString()).thenReturn("query");
        when(this.queryLogic.isLongRunningQuery()).thenReturn(false);
        when(this.queryLogic.getResultLimit(eq(this.query))).thenReturn(maxResults);
        when(this.queryLogic.getMaxResults()).thenReturn(maxResults);
        this.queryLogic.preInitialize(this.query, AuthorizationsUtil.buildAuthorizations(Collections.singleton(Collections.singleton("AUTH_1"))));
        when(this.queryLogic.getUserOperations()).thenReturn(null);
        this.queryLogic.setupQuery(this.genericConfiguration);
        this.queryMetrics.updateMetric(isA(QueryMetric.class));

        when(this.queryLogic.getTransformIterator(this.query)).thenReturn(this.transformIterator);
        this.connectionFactory.returnClient(this.client);
        this.queryLogic.close();

        // Run the test
        RunningQuery subject = new RunningQuery(this.queryMetrics, this.client, Priority.NORMAL, this.queryLogic, this.query, methodAuths, principal,
                        new QueryMetricFactoryImpl());
        subject.setExecutor(executor);
        subject.closeConnection(this.connectionFactory);
        QueryMetric.Lifecycle status = subject.getMetric().getLifecycle();

        assertEquals(QueryMetric.Lifecycle.CLOSED, status, "Expected status to be closed");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testNextWithDnResultLimit_HappyPathUsingDeprecatedConstructor() throws Exception {
        // Set local test input
        String userDN = "userDN";
        String userSid = "userSid";
        UUID queryId = UUID.randomUUID();
        String methodAuths = "AUTH_1";
        String columnVisibility = "AUTH_1";
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of("userDN", "issuerDN"), UserType.USER, Collections.singleton(methodAuths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        String query = "query";
        String queryLogicName = "queryLogicName";
        String queryName = "queryName";
        long currentTime = System.currentTimeMillis();
        Date beginDate = new Date(currentTime - 5000);
        Date endDate = new Date(currentTime - 1000);
        Date expirationDate = new Date(currentTime + 9999);
        int pageSize = 5;
        int maxPageSize = 5;

        long pageByteTrigger = 4 * 1024L;
        long maxWork = Long.MAX_VALUE;
        long maxResults = 10L;
        long dnResultLimit = 2L;
        List<Object> resultObjects = Arrays.asList(new Object(), "resultObject1", "resultObject2", "resultObject3", "resultObject4", "resultObject5");

        // Set expectations
        when(this.queryLogic.getCollectQueryMetrics()).thenReturn(true);
        when(this.query.getUncaughtExceptionHandler()).thenReturn(exceptionHandler);
        when(this.exceptionHandler.getUncaughtException()).thenReturn(null);
        when(this.query.getId()).thenReturn(queryId);
        when(this.query.getOwner()).thenReturn(userSid);
        when(this.query.getQuery()).thenReturn(query);
        when(this.query.getQueryLogicName()).thenReturn(queryLogicName);
        when(this.query.getQueryName()).thenReturn(queryName);
        when(this.query.getBeginDate()).thenReturn(beginDate);
        when(this.query.getEndDate()).thenReturn(endDate);
        when(this.query.isMaxResultsOverridden()).thenReturn(false);
        when(this.query.getExpirationDate()).thenReturn(expirationDate);
        when(this.query.getParameters()).thenReturn(new HashSet<>());
        when(this.query.getQueryAuthorizations()).thenReturn(methodAuths);
        when(this.query.getUserDN()).thenReturn(userDN);
        when(this.query.getColumnVisibility()).thenReturn(columnVisibility);
        when(this.queryLogic.initialize(eq(this.client), eq(this.query), isA(Set.class))).thenReturn(this.genericConfiguration);
        this.queryLogic.setupQuery(this.genericConfiguration);
        when(this.queryLogic.getTransformIterator(this.query)).thenReturn(this.transformIterator);
        when(this.queryLogic.isLongRunningQuery()).thenReturn(false);
        when(this.queryLogic.getResultLimit(eq(this.query))).thenReturn(dnResultLimit);

        Iterator<Object> iterator = resultObjects.iterator();
        int count = 0;
        while (iterator.hasNext() && count < dnResultLimit) {
            when(this.transformIterator.hasNext()).thenReturn(iterator.hasNext());
            when(this.transformIterator.next()).thenReturn(iterator.next());
            count++;
        }
        // now that the results thread is separate from the running query thread, we could continue getting stuff
        when(this.transformIterator.getTransformer()).thenReturn(transformer);
        when(this.transformIterator.hasNext()).thenReturn(iterator.hasNext());
        when(this.transformIterator.next()).thenReturn(iterator.next());

        when(this.query.getPagesize()).thenReturn(pageSize);
        when(this.queryLogic.getMaxPageSize()).thenReturn(maxPageSize);
        when(this.queryLogic.getPageByteTrigger()).thenReturn(pageByteTrigger);
        when(this.queryLogic.getMaxWork()).thenReturn(maxWork);
        when(this.queryLogic.getMaxResults()).thenReturn(maxResults);
        this.queryLogic.preInitialize(this.query, AuthorizationsUtil.buildAuthorizations(Collections.singleton(Collections.singleton("AUTH_1"))));
        when(this.queryLogic.getUserOperations()).thenReturn(null);
        when(this.genericConfiguration.getQueryString()).thenReturn(query);
        this.queryLogic.setPageProcessingStartTime(anyLong());

        // Run the test
        RunningQuery subject = new RunningQuery(this.client, Priority.NORMAL, this.queryLogic, this.query, methodAuths, principal,
                        new QueryMetricFactoryImpl());
        subject.setExecutor(executor);

        ResultsPage<?> result1 = subject.next();

        String result2 = subject.toString();
        QueryMetric.Lifecycle status = subject.getMetric().getLifecycle();

        // Verify results
        assertNotNull(result1, "Expected a non-null page");
        assertNotNull(result1.getResults(), "Expected a non-null list of results");
        assertTrue(resultObjects.size() > dnResultLimit, "Expected DN max results non-null items in the list of results");
        assertEquals(QueryMetric.Lifecycle.MAXRESULTS, status, "Expected status to be MAXRESULTS");

        assertNotNull(result2, "Expected a non-null toString() representation");
    }
}
