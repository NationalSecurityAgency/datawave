package datawave.webservice.query.runner;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils.NpeUtils;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.connection.AccumuloConnectionFactory.Priority;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl.Parameter;
import datawave.webservice.query.cache.QueryMetricFactoryImpl;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.metric.QueryMetric;
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
        System.setProperty("metadatahelper.default.auths", "A,B,C,D");
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
        assertTrue("Expected an exception to be thrown due to uninitialized instance variables", null != result1);
        
        assertTrue("Expected a null connector", null == result2);
        
        assertTrue("Expected a null priority", null == result3);
        
        assertTrue("Expected null logic", null == result4);
        
        assertTrue("Expected a null query (a.k.a. settings)", null == result5);
        
        assertTrue("Expected a null iterator", null == result6);
        
        assertTrue("Expected a null set of authorizations", null == result7);
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testNext_HappyPathUsingDeprecatedConstructor() throws Exception {
        
        // Set local test input
        String userDN = "userDN";
        String userSid = "userSid";
        UUID queryId = UUID.randomUUID();
        String methodAuths = "AUTH_1";
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
        long maxRowsToScan = Long.MAX_VALUE;
        long maxResults = 100L;
        List<Object> resultObjects = Arrays.asList(new Object(), "resultObject1", null);
        
        // Set expectations
        expect(this.queryLogic.getCollectQueryMetrics()).andReturn(true);
        this.query.populateMetric(isA(QueryMetric.class));
        expect(this.query.getUncaughtExceptionHandler()).andReturn(exceptionHandler).times(5);
        expect(this.exceptionHandler.getThrowable()).andReturn(null).times(5);
        expect(this.query.getId()).andReturn(queryId).times(3);
        expect(this.query.getOwner()).andReturn(userSid);
        expect(this.query.getQuery()).andReturn(query);
        expect(this.query.getQueryLogicName()).andReturn(queryLogicName);
        expect(this.query.getQueryName()).andReturn(queryName);
        expect(this.query.getBeginDate()).andReturn(beginDate);
        expect(this.query.getEndDate()).andReturn(endDate);
        expect(this.query.getExpirationDate()).andReturn(expirationDate);
        expect(this.query.getParameters()).andReturn(new HashSet<>());
        expect(this.query.getQueryAuthorizations()).andReturn(methodAuths);
        expect(this.query.getUserDN()).andReturn(userDN).times(2);
        expect(this.queryLogic.initialize(eq(this.connector), eq(this.query), isA(Set.class))).andReturn(this.genericConfiguration);
        this.queryLogic.setupQuery(this.genericConfiguration);
        expect(this.queryLogic.getTransformIterator(this.query)).andReturn(this.transformIterator);
        Iterator<Object> iterator = resultObjects.iterator();
        while (iterator.hasNext()) {
            expect(this.transformIterator.hasNext()).andReturn(iterator.hasNext());
            expect(this.transformIterator.next()).andReturn(iterator.next());
        }
        expect(this.query.getPagesize()).andReturn(pageSize).anyTimes();
        expect(this.queryLogic.getMaxPageSize()).andReturn(maxPageSize).anyTimes();
        expect(this.queryLogic.getPageByteTrigger()).andReturn(pageByteTrigger).anyTimes();
        expect(this.queryLogic.getMaxRowsToScan()).andReturn(maxRowsToScan).anyTimes();
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
        assertTrue("Expected a non-null page", null != result1);
        assertTrue("Expected a non-null list of results", null != result1.getResults());
        assertTrue("Expected 2 non-null items in the list of results", result1.getResults().size() == 2);
        assertTrue("Expected status to be closed", status == QueryMetric.Lifecycle.RESULTS);
        
        assertTrue("Expected a non-null toString() representation", null != result2);
        
        assertTrue("Expected lifecycle to be results", QueryMetric.Lifecycle.RESULTS == subject.getMetric().getLifecycle());
        
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testNextMaxResults_HappyPathUsingDeprecatedConstructor() throws Exception {
        // Set local test input
        String userDN = "userDN";
        String userSid = "userSid";
        UUID queryId = UUID.randomUUID();
        String methodAuths = "AUTH_1";
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
        long maxRowsToScan = Long.MAX_VALUE;
        long maxResults = 4L;
        List<Object> resultObjects = Arrays.asList(new Object(), "resultObject1", "resultObject2", "resultObject3", "resultObject4", "resultObject5");
        
        // Set expectations
        expect(this.queryLogic.getCollectQueryMetrics()).andReturn(true);
        this.query.populateMetric(isA(QueryMetric.class));
        expect(this.query.getUncaughtExceptionHandler()).andReturn(exceptionHandler).times(7);
        expect(this.exceptionHandler.getThrowable()).andReturn(null).times(7);
        expect(this.query.getId()).andReturn(queryId).times(3);
        expect(this.query.getOwner()).andReturn(userSid);
        expect(this.query.getQuery()).andReturn(query);
        expect(this.query.getQueryLogicName()).andReturn(queryLogicName);
        expect(this.query.getQueryName()).andReturn(queryName);
        expect(this.query.getBeginDate()).andReturn(beginDate);
        expect(this.query.getEndDate()).andReturn(endDate);
        expect(this.query.getExpirationDate()).andReturn(expirationDate);
        expect(this.query.getParameters()).andReturn(new HashSet<>());
        expect(this.query.getQueryAuthorizations()).andReturn(methodAuths);
        expect(this.query.getUserDN()).andReturn(userDN).times(2);
        expect(this.queryLogic.initialize(eq(this.connector), eq(this.query), isA(Set.class))).andReturn(this.genericConfiguration);
        this.queryLogic.setupQuery(this.genericConfiguration);
        expect(this.queryLogic.getTransformIterator(this.query)).andReturn(this.transformIterator);
        
        Iterator<Object> iterator = resultObjects.iterator();
        int count = 0;
        expect(this.transformIterator.hasNext()).andReturn(iterator.hasNext());
        while (iterator.hasNext() && count < maxResults) {
            expect(this.transformIterator.hasNext()).andReturn(iterator.hasNext());
            expect(this.transformIterator.next()).andReturn(iterator.next());
            count++;
        }
        
        expect(this.query.getPagesize()).andReturn(pageSize).anyTimes();
        expect(this.queryLogic.getMaxPageSize()).andReturn(maxPageSize).anyTimes();
        expect(this.queryLogic.getPageByteTrigger()).andReturn(pageByteTrigger).anyTimes();
        expect(this.queryLogic.getMaxRowsToScan()).andReturn(maxRowsToScan).anyTimes();
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
        assertTrue("Expected a non-null page", null != result1);
        assertTrue("Expected a non-null list of results", null != result1.getResults());
        assertTrue("Expected MAXRESULTS non-null items in the list of results", resultObjects.size() > maxResults);
        assertTrue("Expected status to be MAXRESULTS", status == QueryMetric.Lifecycle.MAXRESULTS);
        
        assertTrue("Expected a non-null toString() representation", null != result2);
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testNext_NoResultsAfterCancellationUsingDeprecatedConstructor() throws Exception {
        // Set local test input
        String userDN = "userDN";
        UUID queryId = UUID.randomUUID();
        String methodAuths = "AUTH_1";
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of("userDN", "issuerDN"), UserType.USER, Collections.singleton(methodAuths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        
        // Set expectations
        expect(this.queryLogic.getCollectQueryMetrics()).andReturn(true);
        this.query.populateMetric(isA(QueryMetric.class));
        expect(this.query.getUncaughtExceptionHandler()).andReturn(exceptionHandler).times(3);
        expect(this.exceptionHandler.getThrowable()).andReturn(null).times(3);
        expect(this.query.getId()).andReturn(queryId).times(2);
        expect(this.query.getUserDN()).andReturn(userDN).times(2);
        expect(this.queryLogic.initialize(eq(this.connector), eq(this.query), isA(Set.class))).andReturn(this.genericConfiguration);
        this.queryLogic.setupQuery(this.genericConfiguration);
        expect(this.transformIterator.getTransformer()).andReturn(transformer);
        this.queryMetrics.updateMetric(isA(QueryMetric.class));
        PowerMock.expectLastCall().times(3);
        expect(this.queryLogic.getTransformIterator(this.query)).andReturn(this.transformIterator);
        expect(this.transformIterator.hasNext()).andReturn(true);
        expect(this.genericConfiguration.getQueryString()).andReturn("query").once();
        
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
        
        assertTrue("Expected a non-null page", null != result2);
        assertTrue("Expected a non-null list of results", null != result2.getResults());
        assertTrue("Expected an empty list of results", result2.getResults().isEmpty());
        assertTrue("Expected status to be cancelled", QueryMetric.Lifecycle.CANCELLED == subject.getMetric().getLifecycle());
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testCloseConnection_HappyPath() throws Exception {
        // Set local test input
        String userDN = "userDN";
        UUID queryId = UUID.randomUUID();
        String methodAuths = "AUTH_1";
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of("userDN", "issuerDN"), UserType.USER, Collections.singleton(methodAuths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        
        // Set expectations
        expect(this.transformIterator.getTransformer()).andReturn(transformer);
        expect(this.queryLogic.getCollectQueryMetrics()).andReturn(true);
        this.query.populateMetric(isA(QueryMetric.class));
        expect(this.query.getUncaughtExceptionHandler()).andReturn(exceptionHandler);
        expect(this.exceptionHandler.getThrowable()).andReturn(null);
        expect(this.query.getId()).andReturn(queryId).times(2);
        expect(this.query.getUserDN()).andReturn(userDN).times(2);
        expect(this.queryLogic.initialize(eq(this.connector), eq(this.query), isA(Set.class))).andReturn(this.genericConfiguration);
        expect(this.genericConfiguration.getQueryString()).andReturn("query").once();
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
        
        assertTrue("Expected status to be closed", status == QueryMetric.Lifecycle.CLOSED);
    }
}
