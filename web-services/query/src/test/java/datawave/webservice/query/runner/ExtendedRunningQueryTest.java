package datawave.webservice.query.runner;

import com.google.common.collect.Lists;
import datawave.microservice.querymetric.QueryMetric;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils.NpeUtils;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import datawave.webservice.query.Query;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.functors.NOPTransformer;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
public class ExtendedRunningQueryTest {
    @Mock
    Connector connector;
    
    @Mock
    AccumuloConnectionFactory connectionFactory;
    
    @Mock
    GenericQueryConfiguration genericConfiguration;
    
    @Mock
    Query query;
    
    @Mock
    QueryUncaughtExceptionHandler exceptionHandler;
    
    @Mock
    QueryLogic<?> queryLogic;
    
    @Mock
    QueryMetricsBean queryMetrics;
    
    @Mock
    TransformIterator transformIterator;
    
    private Transformer transformer = NOPTransformer.nopTransformer();
    
    @Before
    public void setup() {
        System.setProperty(NpeUtils.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
    }
    
    @Test
    public void testConstructor_NoArg() throws Exception {
        // Run the test
        PowerMock.replayAll();
        RunningQuery subject = new RunningQuery();
        Exception result1 = null;
        try {
            subject.next();
        } catch (NullPointerException e) {
            result1 = e;
        }
        Connector result2 = subject.getConnection();
        Priority result3 = subject.getConnectionPriority();
        QueryLogic<?> result4 = subject.getLogic();
        Query result5 = subject.getSettings();
        TransformIterator result6 = subject.getTransformIterator();
        Set<Authorizations> result7 = subject.getCalculatedAuths();
        PowerMock.verifyAll();
        
        // Verify results
        assertNotNull("Expected an exception to be thrown due to uninitialized instance variables", result1);
        
        assertNull("Expected a null connector", result2);
        
        assertNull("Expected a null priority", result3);
        
        assertNull("Expected null logic", result4);
        
        assertNull("Expected a null query (a.k.a. settings)", result5);
        
        assertNull("Expected a null iterator", result6);
        
        assertNull("Expected a null set of authorizations", result7);
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testNext_HappyPathUsingDeprecatedConstructor() throws Exception {
        
        // Set local test input
        String userDN = "userDN";
        List<String> dnList = Lists.newArrayList(userDN);
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
        List<Object> resultObjects = Arrays.asList(new Object(), "resultObject1", null);
        
        // Set expectations
        expect(this.queryLogic.getCollectQueryMetrics()).andReturn(true);
        expect(this.query.getUncaughtExceptionHandler()).andReturn(exceptionHandler).times(5);
        expect(this.exceptionHandler.getThrowable()).andReturn(null).times(5);
        expect(this.query.getId()).andReturn(queryId).times(4);
        expect(this.query.getOwner()).andReturn(userSid).times(2);
        expect(this.query.getQuery()).andReturn(query).times(2);
        expect(this.query.getQueryLogicName()).andReturn(queryLogicName).times(2);
        expect(this.query.getQueryName()).andReturn(queryName).times(2);
        
        expect(this.query.getBeginDate()).andReturn(beginDate).times(2);
        expect(this.query.getEndDate()).andReturn(endDate).times(2);
        expect(this.query.isMaxResultsOverridden()).andReturn(false).anyTimes();
        expect(this.query.getExpirationDate()).andReturn(expirationDate);
        expect(this.query.getParameters()).andReturn(new HashSet<>()).times(2);
        expect(this.query.getQueryAuthorizations()).andReturn(methodAuths).times(2);
        expect(this.query.getColumnVisibility()).andReturn(columnVisibility);
        expect(this.query.getUserDN()).andReturn(userDN).times(3);
        expect(this.query.getDnList()).andReturn(dnList);
        expect(this.queryLogic.initialize(eq(this.connector), eq(this.query), isA(Set.class))).andReturn(this.genericConfiguration);
        this.queryLogic.setupQuery(this.genericConfiguration);
        expect(this.queryLogic.getTransformIterator(this.query)).andReturn(this.transformIterator);
        Iterator<Object> iterator = resultObjects.iterator();
        while (iterator.hasNext()) {
            expect(this.transformIterator.hasNext()).andReturn(iterator.hasNext());
            expect(this.transformIterator.next()).andReturn(iterator.next());
            expect(this.transformIterator.getTransformer()).andReturn(transformer);
        }
        expect(this.query.getPagesize()).andReturn(pageSize).anyTimes();
        expect(this.queryLogic.getMaxPageSize()).andReturn(maxPageSize).anyTimes();
        expect(this.queryLogic.getPageByteTrigger()).andReturn(pageByteTrigger).anyTimes();
        expect(this.queryLogic.getMaxWork()).andReturn(maxWork).anyTimes();
        expect(this.queryLogic.getMaxResults()).andReturn(maxResults).anyTimes();
        expect(this.genericConfiguration.getQueryString()).andReturn(query).once();
        expect(this.queryLogic.getResultLimit(eq(dnList))).andReturn(maxResults);
        
        // Run the test
        PowerMock.replayAll();
        RunningQuery subject = new RunningQuery(this.connector, Priority.NORMAL, this.queryLogic, this.query, methodAuths, principal,
                        new QueryMetricFactoryImpl());
        
        ResultsPage result1 = subject.next();
        String result2 = subject.toString();
        QueryMetric.Lifecycle status = subject.getMetric().getLifecycle();
        PowerMock.verifyAll();
        
        // Verify results
        assertNotNull("Expected a non-null page", result1);
        assertNotNull("Expected a non-null list of results", result1.getResults());
        assertEquals("Expected 2 non-null items in the list of results", 2, result1.getResults().size());
        assertSame("Expected status to be closed", status, QueryMetric.Lifecycle.RESULTS);
        
        assertNotNull("Expected a non-null toString() representation", result2);
        
        assertSame("Expected lifecycle to be results", QueryMetric.Lifecycle.RESULTS, subject.getMetric().getLifecycle());
        
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testNextMaxResults_HappyPathUsingDeprecatedConstructor() throws Exception {
        // Set local test input
        String userDN = "userDN";
        List<String> dnList = Lists.newArrayList(userDN);
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
        List<Object> resultObjects = Arrays.asList(new Object(), "resultObject1", "resultObject2", "resultObject3", "resultObject4", "resultObject5");
        
        // Set expectations
        expect(this.queryLogic.getCollectQueryMetrics()).andReturn(true);
        expect(this.query.getUncaughtExceptionHandler()).andReturn(exceptionHandler).times(7);
        expect(this.exceptionHandler.getThrowable()).andReturn(null).times(7);
        expect(this.query.getId()).andReturn(queryId).times(4);
        expect(this.query.getOwner()).andReturn(userSid).times(2);
        expect(this.query.getQuery()).andReturn(query).times(2);
        expect(this.query.getQueryLogicName()).andReturn(queryLogicName).times(2);
        expect(this.query.getQueryName()).andReturn(queryName).times(2);
        expect(this.query.getBeginDate()).andReturn(beginDate).times(2);
        expect(this.query.getEndDate()).andReturn(endDate).times(2);
        expect(this.query.isMaxResultsOverridden()).andReturn(false).anyTimes();
        expect(this.query.getExpirationDate()).andReturn(expirationDate);
        expect(this.query.getParameters()).andReturn(new HashSet<>()).times(2);
        expect(this.query.getQueryAuthorizations()).andReturn(methodAuths).times(2);
        expect(this.query.getUserDN()).andReturn(userDN).times(3);
        expect(this.query.getColumnVisibility()).andReturn(columnVisibility);
        expect(this.query.getDnList()).andReturn(dnList);
        expect(this.queryLogic.initialize(eq(this.connector), eq(this.query), isA(Set.class))).andReturn(this.genericConfiguration);
        this.queryLogic.setupQuery(this.genericConfiguration);
        expect(this.queryLogic.getTransformIterator(this.query)).andReturn(this.transformIterator);
        expect(this.queryLogic.getResultLimit(eq(dnList))).andReturn(maxResults);
        
        Iterator<Object> iterator = resultObjects.iterator();
        int count = 0;
        expect(this.transformIterator.hasNext()).andReturn(iterator.hasNext());
        while (iterator.hasNext() && count < maxResults) {
            expect(this.transformIterator.hasNext()).andReturn(iterator.hasNext());
            expect(this.transformIterator.next()).andReturn(iterator.next());
            count++;
        }
        expect(this.transformIterator.getTransformer()).andReturn(transformer).times(count);
        
        expect(this.query.getPagesize()).andReturn(pageSize).anyTimes();
        expect(this.queryLogic.getMaxPageSize()).andReturn(maxPageSize).anyTimes();
        expect(this.queryLogic.getPageByteTrigger()).andReturn(pageByteTrigger).anyTimes();
        expect(this.queryLogic.getMaxWork()).andReturn(maxWork).anyTimes();
        expect(this.queryLogic.getMaxResults()).andReturn(maxResults).anyTimes();
        expect(this.genericConfiguration.getQueryString()).andReturn(query).once();
        
        // Run the test
        PowerMock.replayAll();
        RunningQuery subject = new RunningQuery(this.connector, Priority.NORMAL, this.queryLogic, this.query, methodAuths, principal,
                        new QueryMetricFactoryImpl());
        
        ResultsPage result1 = subject.next();
        
        String result2 = subject.toString();
        QueryMetric.Lifecycle status = subject.getMetric().getLifecycle();
        PowerMock.verifyAll();
        
        // Verify results
        assertNotNull("Expected a non-null page", result1);
        assertNotNull("Expected a non-null list of results", result1.getResults());
        assertTrue("Expected MAXRESULTS non-null items in the list of results", resultObjects.size() > maxResults);
        assertSame("Expected status to be MAXRESULTS", status, QueryMetric.Lifecycle.MAXRESULTS);
        
        assertNotNull("Expected a non-null toString() representation", result2);
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testNext_NoResultsAfterCancellationUsingDeprecatedConstructor() throws Exception {
        // Set local test input
        String userDN = "userDN";
        String userSid = "userSid";
        List<String> dnList = Lists.newArrayList(userDN);
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
        
        // Set expectations
        expect(this.queryLogic.getCollectQueryMetrics()).andReturn(true);
        expect(this.query.getUncaughtExceptionHandler()).andReturn(exceptionHandler).times(3);
        expect(this.exceptionHandler.getThrowable()).andReturn(null).times(3);
        expect(this.query.getId()).andReturn(queryId).times(3);
        expect(this.query.getUserDN()).andReturn(userDN).times(3);
        expect(this.query.getDnList()).andReturn(dnList);
        expect(this.query.getOwner()).andReturn(userSid);
        expect(this.query.getQuery()).andReturn(query);
        expect(this.query.getQueryLogicName()).andReturn(queryLogicName);
        expect(this.query.getQueryName()).andReturn(queryName);
        expect(this.query.getBeginDate()).andReturn(beginDate);
        expect(this.query.getEndDate()).andReturn(endDate);
        expect(this.query.getParameters()).andReturn(new HashSet<>());
        expect(this.query.getQueryAuthorizations()).andReturn(methodAuths);
        expect(this.query.getColumnVisibility()).andReturn(columnVisibility);
        expect(this.queryLogic.initialize(eq(this.connector), eq(this.query), isA(Set.class))).andReturn(this.genericConfiguration);
        this.queryLogic.setupQuery(this.genericConfiguration);
        this.queryMetrics.updateMetric(isA(QueryMetric.class));
        PowerMock.expectLastCall().times(3);
        expect(this.queryLogic.getTransformIterator(this.query)).andReturn(this.transformIterator);
        expect(this.transformIterator.hasNext()).andReturn(true);
        expect(this.genericConfiguration.getQueryString()).andReturn("query").once();
        expect(this.queryLogic.getResultLimit(eq(dnList))).andReturn(maxResults);
        expect(this.queryLogic.getMaxResults()).andReturn(maxResults);
        
        // Run the test
        PowerMock.replayAll();
        RunningQuery subject = new RunningQuery(this.queryMetrics, this.connector, Priority.NORMAL, this.queryLogic, this.query, methodAuths, principal,
                        new QueryMetricFactoryImpl());
        subject.cancel();
        boolean result1 = subject.isCanceled();
        ResultsPage result2 = subject.next();
        PowerMock.verifyAll();
        
        // Verify results
        assertTrue("Expected isCanceled() to return true", result1);
        
        assertNotNull("Expected a non-null page", result2);
        assertNotNull("Expected a non-null list of results", result2.getResults());
        assertTrue("Expected an empty list of results", result2.getResults().isEmpty());
        assertSame("Expected status to be cancelled", QueryMetric.Lifecycle.CANCELLED, subject.getMetric().getLifecycle());
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testCloseConnection_HappyPath() throws Exception {
        // Set local test input
        String userDN = "userDN";
        List<String> dnList = Lists.newArrayList(userDN);
        UUID queryId = UUID.randomUUID();
        String methodAuths = "AUTH_1";
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of("userDN", "issuerDN"), UserType.USER, Collections.singleton(methodAuths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        long maxResults = 100L;
        
        // Set expectations
        expect(this.transformIterator.getTransformer()).andReturn(transformer);
        expect(this.queryLogic.getCollectQueryMetrics()).andReturn(true);
        expect(this.query.getUncaughtExceptionHandler()).andReturn(exceptionHandler);
        expect(this.exceptionHandler.getThrowable()).andReturn(null);
        expect(this.query.getId()).andReturn(queryId).times(3);
        expect(this.query.getUserDN()).andReturn(userDN).times(3);
        expect(this.query.getDnList()).andReturn(dnList);
        expect(this.query.getOwner()).andReturn(null);
        expect(this.query.getQuery()).andReturn(null);
        expect(this.query.getQueryLogicName()).andReturn(null);
        expect(this.query.getQueryName()).andReturn(null);
        expect(this.query.getBeginDate()).andReturn(null);
        expect(this.query.getEndDate()).andReturn(null);
        expect(this.query.getParameters()).andReturn(new HashSet<>());
        expect(this.query.getQueryAuthorizations()).andReturn(null);
        expect(this.query.getColumnVisibility()).andReturn(null);
        expect(this.queryLogic.initialize(eq(this.connector), eq(this.query), isA(Set.class))).andReturn(this.genericConfiguration);
        expect(this.genericConfiguration.getQueryString()).andReturn("query").once();
        expect(this.queryLogic.getResultLimit(eq(dnList))).andReturn(maxResults);
        expect(this.queryLogic.getMaxResults()).andReturn(maxResults);
        this.queryLogic.setupQuery(this.genericConfiguration);
        this.queryMetrics.updateMetric(isA(QueryMetric.class));
        PowerMock.expectLastCall().times(3);
        expect(this.queryLogic.getTransformIterator(this.query)).andReturn(this.transformIterator);
        this.connectionFactory.returnConnection(this.connector);
        this.queryLogic.close();
        
        // Run the test
        PowerMock.replayAll();
        RunningQuery subject = new RunningQuery(this.queryMetrics, this.connector, Priority.NORMAL, this.queryLogic, this.query, methodAuths, principal,
                        new QueryMetricFactoryImpl());
        subject.closeConnection(this.connectionFactory);
        QueryMetric.Lifecycle status = subject.getMetric().getLifecycle();
        PowerMock.verifyAll();
        
        assertSame("Expected status to be closed", status, QueryMetric.Lifecycle.CLOSED);
    }
    
    @SuppressWarnings({"unchecked"})
    @Test
    public void testNextWithDnResultLimit_HappyPathUsingDeprecatedConstructor() throws Exception {
        // Set local test input
        String userDN = "userDN";
        List<String> dnList = Lists.newArrayList(userDN);
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
        expect(this.queryLogic.getCollectQueryMetrics()).andReturn(true);
        expect(this.query.getUncaughtExceptionHandler()).andReturn(exceptionHandler).times(5);
        expect(this.exceptionHandler.getThrowable()).andReturn(null).times(5);
        expect(this.query.getId()).andReturn(queryId).times(4);
        expect(this.query.getOwner()).andReturn(userSid).times(2);
        expect(this.query.getQuery()).andReturn(query).times(2);
        expect(this.query.getQueryLogicName()).andReturn(queryLogicName).times(2);
        expect(this.query.getQueryName()).andReturn(queryName).times(2);
        expect(this.query.getBeginDate()).andReturn(beginDate).times(2);
        expect(this.query.getEndDate()).andReturn(endDate).times(2);
        expect(this.query.isMaxResultsOverridden()).andReturn(false).anyTimes();
        expect(this.query.getExpirationDate()).andReturn(expirationDate);
        expect(this.query.getParameters()).andReturn(new HashSet<>()).times(2);
        expect(this.query.getQueryAuthorizations()).andReturn(methodAuths).times(2);
        expect(this.query.getUserDN()).andReturn(userDN).times(4);
        expect(this.query.getColumnVisibility()).andReturn(columnVisibility);
        expect(this.query.getDnList()).andReturn(dnList);
        expect(this.queryLogic.initialize(eq(this.connector), eq(this.query), isA(Set.class))).andReturn(this.genericConfiguration);
        this.queryLogic.setupQuery(this.genericConfiguration);
        expect(this.queryLogic.getTransformIterator(this.query)).andReturn(this.transformIterator);
        expect(this.queryLogic.getResultLimit(eq(dnList))).andReturn(dnResultLimit);
        
        Iterator<Object> iterator = resultObjects.iterator();
        int count = 0;
        expect(this.transformIterator.hasNext()).andReturn(iterator.hasNext());
        while (iterator.hasNext() && count < dnResultLimit) {
            expect(this.transformIterator.hasNext()).andReturn(iterator.hasNext());
            expect(this.transformIterator.next()).andReturn(iterator.next());
            count++;
        }
        expect(this.transformIterator.getTransformer()).andReturn(transformer).times(count);
        
        expect(this.query.getPagesize()).andReturn(pageSize).anyTimes();
        expect(this.queryLogic.getMaxPageSize()).andReturn(maxPageSize).anyTimes();
        expect(this.queryLogic.getPageByteTrigger()).andReturn(pageByteTrigger).anyTimes();
        expect(this.queryLogic.getMaxWork()).andReturn(maxWork).anyTimes();
        expect(this.queryLogic.getMaxResults()).andReturn(maxResults).anyTimes();
        expect(this.genericConfiguration.getQueryString()).andReturn(query).once();
        
        // Run the test
        PowerMock.replayAll();
        RunningQuery subject = new RunningQuery(this.connector, Priority.NORMAL, this.queryLogic, this.query, methodAuths, principal,
                        new QueryMetricFactoryImpl());
        
        ResultsPage result1 = subject.next();
        
        String result2 = subject.toString();
        QueryMetric.Lifecycle status = subject.getMetric().getLifecycle();
        PowerMock.verifyAll();
        
        // Verify results
        assertNotNull("Expected a non-null page", result1);
        assertNotNull("Expected a non-null list of results", result1.getResults());
        assertTrue("Expected DN max results non-null items in the list of results", resultObjects.size() > dnResultLimit);
        assertSame("Expected status to be MAXRESULTS", status, QueryMetric.Lifecycle.MAXRESULTS);
        
        assertNotNull("Expected a non-null toString() representation", result2);
    }
}
