package datawave.webservice.query.runner;

import static org.easymock.EasyMock.eq;
import static org.junit.Assert.*;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.createStrictMock;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;
import static org.powermock.reflect.Whitebox.setInternalState;

import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.ejb.EJBContext;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.core.MultivaluedMap;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.marking.SecurityMarking;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils.NpeUtils;
import datawave.webservice.common.audit.AuditBean;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor.AuditType;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.QueryParameters;
import datawave.webservice.query.QueryParametersImpl;
import datawave.webservice.query.QueryPersistence;
import datawave.webservice.query.cache.CreatedQueryLogicCacheBean;
import datawave.webservice.query.cache.CreatedQueryLogicCacheBean.Triple;
import datawave.webservice.query.cache.QueryCache;
import datawave.webservice.query.cache.QueryExpirationConfiguration;
import datawave.webservice.query.cache.QueryMetricFactory;
import datawave.webservice.query.cache.QueryMetricFactoryImpl;
import datawave.webservice.query.cache.QueryTraceCache;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.configuration.LookupUUIDConfiguration;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.factory.Persister;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.logic.QueryLogicFactory;
import datawave.webservice.query.logic.QueryLogicFactoryImpl;
import datawave.webservice.query.metric.BaseQueryMetric;
import datawave.webservice.query.metric.BaseQueryMetric.Lifecycle;
import datawave.webservice.query.metric.BaseQueryMetric.Prediction;
import datawave.webservice.query.metric.QueryMetric;
import datawave.webservice.query.metric.QueryMetricsBean;

import datawave.webservice.result.GenericResponse;
import org.apache.accumulo.core.client.Connector;
import datawave.accumulo.inmemory.InMemoryInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.jboss.resteasy.core.Dispatcher;
import org.jboss.resteasy.mock.MockDispatcherFactory;
import org.jboss.resteasy.mock.MockHttpRequest;
import org.jboss.resteasy.mock.MockHttpResponse;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(QueryParameters.class)
@PowerMockIgnore({"java.*", "javax.*", "com.*", "org.apache.*", "org.w3c.*", "net.sf.*"})
public class QueryExecutorBeanTest {
    private static final Logger log = Logger.getLogger(QueryExecutorBeanTest.class);
    
    // QueryExecutorBean dependencies
    private QueryCache cache;
    private AccumuloConnectionRequestBean connectionRequestBean;
    private AccumuloConnectionFactory connectionFactory;
    private QueryMetricsBean metrics;
    private QueryLogicFactoryImpl queryLogicFactory;
    private QueryExpirationConfiguration queryExpirationConf;
    private Persister persister;
    private QueryPredictor predictor;
    private EJBContext ctx;
    private CreatedQueryLogicCacheBean qlCache;
    private QueryExecutorBean bean;
    private Dispatcher dispatcher;
    private MockHttpRequest request;
    private MockHttpResponse response;
    
    @Before
    public void setup() throws Exception {
        System.setProperty(NpeUtils.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("metadatahelper.default.auths", "A,B,C,D");
        QueryTraceCache traceCache = new QueryTraceCache();
        Whitebox.invokeMethod(traceCache, "init");
        
        cache = new QueryCache();
        cache.init();
        
        bean = new QueryExecutorBean();
        
        connectionFactory = createStrictMock(AccumuloConnectionFactory.class);
        AuditBean auditor = createStrictMock(AuditBean.class);
        metrics = createStrictMock(QueryMetricsBean.class);
        queryLogicFactory = createStrictMock(QueryLogicFactoryImpl.class);
        persister = createStrictMock(Persister.class);
        predictor = createStrictMock(QueryPredictor.class);
        ctx = createStrictMock(EJBContext.class);
        qlCache = new CreatedQueryLogicCacheBean();
        queryExpirationConf = new QueryExpirationConfiguration();
        queryExpirationConf.setPageSizeShortCircuitCheckTime(45);
        queryExpirationConf.setPageShortCircuitTimeout(58);
        queryExpirationConf.setCallTime(60);
        connectionRequestBean = createStrictMock(AccumuloConnectionRequestBean.class);
        setInternalState(connectionRequestBean, EJBContext.class, ctx);
        setInternalState(bean, QueryCache.class, cache);
        setInternalState(bean, AccumuloConnectionFactory.class, connectionFactory);
        setInternalState(bean, AuditBean.class, auditor);
        setInternalState(bean, QueryMetricsBean.class, metrics);
        setInternalState(bean, QueryLogicFactory.class, queryLogicFactory);
        setInternalState(bean, QueryExpirationConfiguration.class, queryExpirationConf);
        setInternalState(bean, Persister.class, persister);
        setInternalState(bean, QueryPredictor.class, predictor);
        setInternalState(bean, EJBContext.class, ctx);
        setInternalState(bean, CreatedQueryLogicCacheBean.class, qlCache);
        setInternalState(bean, QueryTraceCache.class, traceCache);
        setInternalState(bean, Multimap.class, HashMultimap.create());
        setInternalState(bean, LookupUUIDConfiguration.class, new LookupUUIDConfiguration());
        setInternalState(bean, SecurityMarking.class, new ColumnVisibilitySecurityMarking());
        setInternalState(bean, AuditParameters.class, new AuditParameters());
        setInternalState(bean, QueryParameters.class, new QueryParametersImpl());
        setInternalState(bean, QueryMetricFactory.class, new QueryMetricFactoryImpl());
        setInternalState(bean, AccumuloConnectionRequestBean.class, connectionRequestBean);
        
        // RESTEasy mock stuff
        dispatcher = MockDispatcherFactory.createDispatcher();
        dispatcher.getRegistry().addSingletonResource(bean, "/DataWave/Query");
        response = new MockHttpResponse();
    }
    
    @Test
    public void testTriple_Nulls() throws Exception {
        Triple subject = new Triple(null, null, null);
        subject.hashCode();
        assertTrue("Should not be equal", !subject.equals(null));
        assertTrue("Should not be equal", !subject.equals(new Triple("test", null, null)));
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testDefine() throws Exception {
        String queryLogicName = "EventQueryLogic";
        String queryName = "Something";
        String query = "FOO == BAR";
        Date beginDate = new Date();
        Date endDate = beginDate;
        Date expirationDate = DateUtils.addDays(new Date(), 1);
        int pagesize = 10;
        QueryPersistence persist = QueryPersistence.TRANSIENT;
        Set<QueryImpl.Parameter> parameters = new HashSet<>();
        
        // need to call the getQueryByName() method. Maybe a partial mock of QueryExecutorBean would be better
        // setup principal mock
        String userDN = "CN=Guy Some Other soguy, OU=MY_SUBDIVISION, OU=MY_DIVISION, O=ORG, C=US";
        String[] auths = new String[2];
        auths[0] = "PRIVATE";
        auths[1] = "PUBLIC";
        QueryImpl q = new QueryImpl();
        q.setBeginDate(beginDate);
        q.setEndDate(endDate);
        q.setExpirationDate(expirationDate);
        q.setPagesize(pagesize);
        q.setParameters(parameters);
        q.setQuery(query);
        q.setQueryAuthorizations(StringUtils.join(auths, ","));
        q.setQueryLogicName(queryLogicName);
        q.setUserDN(userDN);
        q.setId(UUID.randomUUID());
        @SuppressWarnings("rawtypes")
        QueryLogic logic = createMock(BaseQueryLogic.class);
        
        MultivaluedMap<String,String> p = new MultivaluedMapImpl<>();
        p.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, StringUtils.join(auths, ","));
        p.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        p.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        p.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expirationDate));
        p.putSingle(QueryParameters.QUERY_NAME, queryName);
        p.putSingle(QueryParameters.QUERY_PAGESIZE, Integer.toString(pagesize));
        p.putSingle(QueryParameters.QUERY_STRING, query);
        p.putSingle(QueryParameters.QUERY_PERSISTENCE, persist.name());
        p.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, "PRIVATE|PUBLIC");
        
        QueryParameters qp = new QueryParametersImpl();
        MultivaluedMap<String,String> optionalParameters = qp.getUnknownParameters(p);
        optionalParameters.putSingle(AuditParameters.USER_DN, userDN.toLowerCase());
        optionalParameters.putSingle(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "PRIVATE|PUBLIC");
        optionalParameters.putSingle("logicClass", q.getQueryLogicName());
        
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(userDN, "<CN=MY_CA, OU=MY_SUBDIVISION, OU=MY_DIVISION, O=ORG, C=US>"), UserType.USER,
                        Arrays.asList(auths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        String[] dns = principal.getDNs();
        Arrays.sort(dns);
        List<String> dnList = Arrays.asList(dns);
        
        PowerMock.resetAll();
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal).anyTimes();
        suppress(constructor(QueryParametersImpl.class));
        EasyMock.expect(persister.create(principal.getUserDN().subjectDN(), dnList, (SecurityMarking) Whitebox.getField(bean.getClass(), "marking").get(bean),
                        queryLogicName, (QueryParameters) Whitebox.getField(bean.getClass(), "qp").get(bean), optionalParameters)).andReturn(q);
        
        EasyMock.expect(queryLogicFactory.getQueryLogic(queryLogicName, principal)).andReturn(logic);
        EasyMock.expect(logic.getRequiredQueryParameters()).andReturn(Collections.EMPTY_SET);
        EasyMock.expect(logic.getConnectionPriority()).andReturn(AccumuloConnectionFactory.Priority.NORMAL);
        EasyMock.expect(logic.getMaxPageSize()).andReturn(0);
        EasyMock.expect(logic.getCollectQueryMetrics()).andReturn(Boolean.FALSE);
        
        PowerMock.replayAll();
        
        bean.defineQuery(queryLogicName, p);
        
        PowerMock.verifyAll();
        
        Object cachedRunningQuery = cache.get(q.getId().toString());
        Assert.assertNotNull(cachedRunningQuery);
        RunningQuery rq2 = (RunningQuery) cachedRunningQuery;
        Assert.assertEquals(q, rq2.getSettings());
        
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testPredict() throws Exception {
        String queryLogicName = "EventQueryLogic";
        String queryName = "Something";
        String query = "FOO == BAR";
        Date beginDate = new Date();
        Date endDate = beginDate;
        Date expirationDate = DateUtils.addDays(new Date(), 1);
        int pagesize = 10;
        QueryPersistence persist = QueryPersistence.TRANSIENT;
        Set<QueryImpl.Parameter> parameters = new HashSet<>();
        
        // need to call the getQueryByName() method. Maybe a partial mock of QueryExecutorBean would be better
        // setup principal mock
        String userDN = "CN=Guy Some Other soguy, OU=MY_SUBDIVISION, OU=MY_DIVISION, O=ORG, C=US";
        String[] auths = new String[2];
        auths[0] = "PRIVATE";
        auths[1] = "PUBLIC";
        QueryImpl q = new QueryImpl();
        q.setBeginDate(beginDate);
        q.setEndDate(endDate);
        q.setExpirationDate(expirationDate);
        q.setPagesize(pagesize);
        q.setParameters(parameters);
        q.setQuery(query);
        q.setQueryAuthorizations(StringUtils.join(auths, ","));
        q.setQueryLogicName(queryLogicName);
        q.setUserDN(userDN);
        q.setId(UUID.randomUUID());
        @SuppressWarnings("rawtypes")
        QueryLogic logic = createMock(BaseQueryLogic.class);
        
        MultivaluedMap<String,String> p = new MultivaluedMapImpl<>();
        p.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, StringUtils.join(auths, ","));
        p.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        p.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        p.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expirationDate));
        p.putSingle(QueryParameters.QUERY_NAME, queryName);
        p.putSingle(QueryParameters.QUERY_PAGESIZE, Integer.toString(pagesize));
        p.putSingle(QueryParameters.QUERY_STRING, query);
        p.putSingle(QueryParameters.QUERY_PERSISTENCE, persist.name());
        p.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, "PRIVATE|PUBLIC");
        
        QueryParameters qp = new QueryParametersImpl();
        MultivaluedMap<String,String> optionalParameters = qp.getUnknownParameters(p);
        optionalParameters.putSingle(AuditParameters.USER_DN, userDN.toLowerCase());
        optionalParameters.putSingle(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "PRIVATE|PUBLIC");
        optionalParameters.putSingle("logicClass", q.getQueryLogicName());
        
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(userDN, "<CN=MY_CA, OU=MY_SUBDIVISION, OU=MY_DIVISION, O=ORG, C=US>"), UserType.USER,
                        Arrays.asList(auths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        String[] dns = principal.getDNs();
        Arrays.sort(dns);
        List<String> dnList = Arrays.asList(dns);
        
        PowerMock.resetAll();
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal).anyTimes();
        suppress(constructor(QueryParametersImpl.class));
        EasyMock.expect(persister.create(principal.getUserDN().subjectDN(), dnList, (SecurityMarking) Whitebox.getField(bean.getClass(), "marking").get(bean),
                        queryLogicName, (QueryParameters) Whitebox.getField(bean.getClass(), "qp").get(bean), optionalParameters)).andReturn(q);
        
        EasyMock.expect(queryLogicFactory.getQueryLogic(queryLogicName, principal)).andReturn(logic);
        EasyMock.expect(logic.getRequiredQueryParameters()).andReturn(Collections.EMPTY_SET);
        EasyMock.expect(logic.getMaxPageSize()).andReturn(0);
        
        BaseQueryMetric metric = new QueryMetricFactoryImpl().createMetric();
        q.populateMetric(metric);
        metric.setQueryType(RunningQuery.class.getSimpleName());
        System.out.println(metric.toString());
        
        Set<Prediction> predictions = new HashSet<Prediction>();
        predictions.add(new Prediction("source", 1));
        EasyMock.expect(predictor.predict(metric)).andReturn(predictions);
        
        PowerMock.replayAll();
        
        GenericResponse<String> response = bean.predictQuery(queryLogicName, p);
        
        PowerMock.verifyAll();
        
        Object cachedRunningQuery = cache.get(q.getId().toString());
        Assert.assertNull(cachedRunningQuery);
        
        Assert.assertEquals(predictions.toString(), response.getResult());
    }
    
    // @Test
    @SuppressWarnings("unchecked")
    public void testGoodListWithGet() throws URISyntaxException, CloneNotSupportedException, ParserConfigurationException, IOException, SAXException {
        String queryName = "Something";
        // need to call the getQueryByName() method. Maybe a partial mock of QueryExecutorBean would be better
        // setup principal mock
        String dn = "CN=Guy Some Other soguy, OU=MY_SUBDIVISION, OU=MY_DIVISION, O=ORG, C=US";
        String[] auths = new String[2];
        auths[0] = "PUBLIC";
        auths[1] = "PRIVATE";
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(dn), UserType.USER, Arrays.asList(auths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal);
        EasyMock.replay(ctx);
        
        // setup persister with queries
        String logicName = "EventQuery";
        QueryImpl q1 = new QueryImpl();
        q1.setUserDN(principal.getShortName());
        q1.setQueryLogicName(logicName);
        q1.setQueryAuthorizations(auths.toString());
        q1.setId(new UUID(1, 1));
        QueryImpl q2 = new QueryImpl();
        q2.setUserDN(principal.getShortName());
        q2.setQueryLogicName(logicName);
        q2.setQueryAuthorizations(auths.toString());
        q2.setId(new UUID(1, 2));
        
        List<Query> queries = new ArrayList<>();
        queries.add(q1);
        queries.add(q2);
        EasyMock.expect(persister.findByName(queryName)).andReturn(queries);
        EasyMock.replay(persister);
        
        @SuppressWarnings("rawtypes")
        QueryLogic logic = createMock(BaseQueryLogic.class);
        EasyMock.expect(logic.getConnectionPriority()).andReturn(AccumuloConnectionFactory.Priority.NORMAL).times(2);
        EasyMock.expect(logic.getMaxPageSize()).andReturn(0);
        EasyMock.replay(logic);
        
        EasyMock.expect(queryLogicFactory.getQueryLogic(logicName, null)).andReturn(logic).times(2);
        EasyMock.replay(queryLogicFactory);
        
        // setup test
        request = MockHttpRequest.get("/DataWave/Query/list?name=" + queryName);
        
        // execute
        dispatcher.invoke(request, response);
        
        // assert
        assertEquals(HttpServletResponse.SC_OK, response.getStatus());
        DocumentBuilder db = (DocumentBuilderFactory.newInstance()).newDocumentBuilder();
        Document doc = db.parse(new InputSource(new StringReader(response.getContentAsString())));
        NodeList returnedQueries = doc.getElementsByTagName("query");
        assertEquals(queries.size(), returnedQueries.getLength());
    }
    
    // @Test
    public void testListWithNoName() throws URISyntaxException, CloneNotSupportedException {
        // setup test
        request = MockHttpRequest.get("/DataWave/Query/list");
        
        // execute
        dispatcher.invoke(request, response);
        
        // assert
        assertEquals(HttpServletResponse.SC_BAD_REQUEST, response.getStatus());
    }
    
    // @Test
    public void testListWithWrongUser() {
        
    }
    
    // @Test
    public void testListWhenQueryDoesNotExist() {
        
    }
    
    // @Test
    public void testListWithPost() {
        
    }
    
    @Test
    public void testBeginDateAfterEndDate() throws Exception {
        String queryName = "Something";
        String query = "FOO == BAR";
        final Date beginDate = new Date(2018, 1, 2);
        final Date endDate = new Date(2018, 1, 1);
        Date expirationDate = DateUtils.addDays(new Date(), 1);
        QueryPersistence persist = QueryPersistence.TRANSIENT;
        final String[] auths = new String[2];
        auths[0] = "PUBLIC";
        auths[1] = "PRIVATE";
        
        final MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putSingle(QueryParameters.QUERY_STRING, query);
        queryParameters.putSingle(QueryParameters.QUERY_NAME, queryName);
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persist.name());
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, StringUtils.join(auths, ","));
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expirationDate));
        
        final Thread createQuery = new Thread(() -> {
            try {
                bean.createQuery("EventQueryLogic", queryParameters);
                fail(); // If doesn't throw exception, should fail
                    } catch (BadRequestException e) {
                        assertEquals(DatawaveErrorCode.BEGIN_DATE_AFTER_END_DATE.toString(), e.getCause().getMessage());
                    }
                });
        
        try {
            createQuery.start();
            createQuery.join();
        } finally {
            if (null != createQuery && createQuery.isAlive())
                createQuery.interrupt();
        }
    }
    
    @SuppressWarnings("unchecked")
    @Test(timeout = 5000)
    public void testCloseActuallyCloses() throws Exception {
        final String queryLogicName = "EventQueryLogic";
        final String queryName = "Something";
        final String query = "FOO == BAR";
        final Date beginDate = new Date();
        final Date endDate = beginDate;
        final Date expirationDate = DateUtils.addDays(new Date(), 1);
        final int pagesize = 10;
        final QueryPersistence persist = QueryPersistence.TRANSIENT;
        Set<QueryImpl.Parameter> parameters = new HashSet<>();
        // need to call the getQueryByName() method. Maybe a partial mock of QueryExecutorBean would be better
        // setup principal mock
        String userDN = "CN=Guy Some Other soguy, OU=MY_SUBDIVISION, OU=MY_DIVISION, O=ORG, C=US";
        final String[] auths = new String[2];
        auths[0] = "PUBLIC";
        auths[1] = "PRIVATE";
        QueryImpl q = new QueryImpl();
        q.setQueryName(queryName);
        q.setDnList(Collections.singletonList(userDN));
        q.setBeginDate(beginDate);
        q.setEndDate(endDate);
        q.setExpirationDate(expirationDate);
        q.setPagesize(pagesize);
        q.setParameters(parameters);
        q.setQuery(query);
        q.setQueryAuthorizations(StringUtils.join(auths, ","));
        q.setQueryLogicName(queryLogicName);
        q.setId(UUID.randomUUID());
        q.setColumnVisibility("PRIVATE|PUBLIC");
        
        final MultivaluedMap<String,String> queryParameters = new MultivaluedMapImpl<>();
        queryParameters.putSingle(QueryParameters.QUERY_STRING, query);
        queryParameters.putSingle(QueryParameters.QUERY_NAME, queryName);
        queryParameters.putSingle(QueryParameters.QUERY_PERSISTENCE, persist.name());
        queryParameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, StringUtils.join(auths, ","));
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        queryParameters.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expirationDate));
        queryParameters.putSingle(QueryParameters.QUERY_PAGESIZE, String.valueOf(pagesize));
        queryParameters.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, q.getColumnVisibility());
        
        final Thread createQuery = new Thread(() -> {
            try {
                bean.createQuery("EventQueryLogic", queryParameters);
            } catch (Exception e) {
                Assert.fail(e.getClass().getName() + " thrown with " + e.getMessage());
            }
        });
        
        @SuppressWarnings("rawtypes")
        QueryLogic logic = createMock(BaseQueryLogic.class);
        
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(userDN, "<CN=MY_CA, OU=MY_SUBDIVISION, OU=MY_DIVISION, O=ORG, C=US>"), UserType.USER,
                        Arrays.asList(auths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        principal.getShortName();
        String[] dns = principal.getDNs();
        Arrays.sort(dns);
        List<String> dnList = Arrays.asList(dns);
        
        InMemoryInstance instance = new InMemoryInstance();
        Connector c = instance.getConnector("root", new PasswordToken(""));
        
        QueryParameters qp = new QueryParametersImpl();
        qp.validate(queryParameters);
        MultivaluedMap<String,String> optionalParameters = qp.getUnknownParameters(queryParameters);
        optionalParameters.putSingle(AuditParameters.USER_DN, principal.getUserDN().subjectDN());
        optionalParameters.putSingle(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, q.getColumnVisibility());
        optionalParameters.putSingle("logicClass", queryLogicName);
        
        PowerMock.resetAll();
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal).anyTimes();
        EasyMock.expect(logic.getAuditType(null)).andReturn(AuditType.NONE);
        EasyMock.expect(persister.create(principal.getUserDN().subjectDN(), dnList, Whitebox.getInternalState(bean, SecurityMarking.class), queryLogicName,
                        Whitebox.getInternalState(bean, QueryParameters.class), optionalParameters)).andReturn(q);
        EasyMock.expect(connectionFactory.getTrackingMap((StackTraceElement[]) EasyMock.anyObject())).andReturn(Maps.<String,String> newHashMap()).anyTimes();
        
        BaseQueryMetric metric = new QueryMetricFactoryImpl().createMetric();
        q.populateMetric(metric);
        metric.setQueryType(RunningQuery.class.getSimpleName());
        metric.setLifecycle(Lifecycle.DEFINED);
        System.out.println(metric.toString());
        
        Set<Prediction> predictions = new HashSet<Prediction>();
        predictions.add(new Prediction("source", 1));
        EasyMock.expect(predictor.predict(metric)).andReturn(predictions);
        
        connectionRequestBean.requestBegin(q.getId().toString());
        EasyMock.expectLastCall();
        EasyMock.expect(connectionFactory.getConnection(eq("connPool1"), (AccumuloConnectionFactory.Priority) EasyMock.anyObject(),
                        (Map<String,String>) EasyMock.anyObject())).andReturn(c).anyTimes();
        connectionRequestBean.requestEnd(q.getId().toString());
        EasyMock.expectLastCall();
        
        EasyMock.expect(queryLogicFactory.getQueryLogic(queryLogicName, principal)).andReturn(logic);
        EasyMock.expect(logic.getRequiredQueryParameters()).andReturn(Collections.emptySet());
        EasyMock.expect(logic.getConnectionPriority()).andReturn(AccumuloConnectionFactory.Priority.NORMAL).atLeastOnce();
        EasyMock.expect(logic.getMaxPageSize()).andReturn(0);
        EasyMock.expect(logic.getAuditType(q)).andReturn(AuditType.NONE);
        EasyMock.expect(logic.getConnPoolName()).andReturn("connPool1");
        
        EasyMock.expect(connectionRequestBean.cancelConnectionRequest(q.getId().toString())).andReturn(false);
        connectionFactory.returnConnection(EasyMock.isA(Connector.class));
        
        final AtomicBoolean initializeLooping = new AtomicBoolean(false);
        
        // During initialize, mark that we get here, and then sleep
        final IAnswer<GenericQueryConfiguration> initializeAnswer = () -> {
            initializeLooping.set(true);
            while (true) {
                Thread.sleep(1000);
                log.debug("Initialize: woke up");
            }
        };
        
        EasyMock.expect(logic.initialize(EasyMock.anyObject(Connector.class), EasyMock.anyObject(Query.class), EasyMock.anyObject(Set.class))).andAnswer(
                        initializeAnswer);
        EasyMock.expect(logic.getCollectQueryMetrics()).andReturn(Boolean.FALSE);
        
        // On close, interrupt the thread to simulate the ScannerFactory cleaning up
        final IAnswer<Object> closeAnswer = () -> {
            if (null != createQuery) {
                log.debug("createQuery thread is not null. interrupting");
                createQuery.interrupt();
            } else {
                log.debug("createQuery thread is null. not interrupting");
            }
            return null;
        };
        
        logic.close();
        EasyMock.expectLastCall().andAnswer(closeAnswer);
        
        // Make the QueryLogic mock not threadsafe, otherwise it will be blocked infinitely
        // trying to get the lock on the infinite loop
        EasyMock.makeThreadSafe(logic, false);
        
        metrics.updateMetric(EasyMock.isA(QueryMetric.class));
        
        PowerMock.replayAll();
        try {
            createQuery.start();
            
            // Wait for the create call to get to initialize
            while (!initializeLooping.get()) {
                if (!createQuery.isAlive()) {
                    Assert.fail("createQuery thread died before reaching initialize");
                }
                Thread.sleep(50);
            }
            
            // initialize has not completed yet so it will not appear in the cache
            Object cachedRunningQuery = cache.get(q.getId().toString());
            Assert.assertNull(cachedRunningQuery);
            Pair<QueryLogic<?>,Connector> pair = qlCache.poll(q.getId().toString());
            Assert.assertNotNull(pair);
            Assert.assertEquals(logic, pair.getFirst());
            Assert.assertEquals(c, pair.getSecond());
            
            // Have to add these back because poll was destructive
            qlCache.add(q.getId().toString(), principal.getShortName(), pair.getFirst(), pair.getSecond());
            
            // Call close
            bean.close(q.getId().toString());
            
            // Make sure that it's gone from the qlCache
            pair = qlCache.poll(q.getId().toString());
            Assert.assertNull("Still found an entry in the qlCache: " + pair, pair);
            
            // Should have already joined by now, but just to be sure
            createQuery.join();
        } finally {
            if (null != createQuery && createQuery.isAlive()) {
                createQuery.interrupt();
            }
        }
    }
}
