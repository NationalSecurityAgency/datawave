package datawave.webservice.query.runner;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.marking.SecurityMarking;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.DnUtils.NpeUtils;
import datawave.webservice.common.audit.AuditBean;
import datawave.webservice.common.audit.AuditParameterBuilder;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.AuditService;
import datawave.webservice.common.audit.Auditor.AuditType;
import datawave.webservice.common.audit.DefaultAuditParameterBuilder;
import datawave.webservice.common.audit.PrivateAuditConstants;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.common.exception.BadRequestException;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.QueryParameters;
import datawave.webservice.query.QueryParametersImpl;
import datawave.webservice.query.QueryPersistence;
import datawave.webservice.query.cache.ClosedQueryCache;
import datawave.webservice.query.cache.CreatedQueryLogicCacheBean;
import datawave.webservice.query.cache.CreatedQueryLogicCacheBean.Triple;
import datawave.webservice.query.cache.QueryCache;
import datawave.webservice.query.cache.QueryExpirationConfiguration;
import datawave.webservice.query.cache.QueryTraceCache;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import datawave.webservice.query.configuration.LookupUUIDConfiguration;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.factory.Persister;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.logic.QueryLogic;
import datawave.webservice.query.logic.QueryLogicFactory;
import datawave.webservice.query.logic.QueryLogicFactoryImpl;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.Lifecycle;
import datawave.microservice.querymetric.BaseQueryMetric.Prediction;
import datawave.microservice.querymetric.QueryMetric;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.result.GenericResponse;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
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

import javax.ejb.EJBContext;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MultivaluedMap;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.createStrictMock;
import static org.powermock.api.support.membermodification.MemberMatcher.constructor;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;
import static org.powermock.reflect.Whitebox.setInternalState;

@RunWith(PowerMockRunner.class)
@PrepareForTest(QueryParameters.class)
@PowerMockIgnore({"java.*", "javax.*", "com.*", "org.apache.*", "org.w3c.*", "net.sf.*"})
public class QueryExecutorBeanTest {
    // Fields for building generic default queries
    private final String queryLogicName = "EventQueryLogic";
    private final String queryName = "Something";
    private final String query = "FOO == BAR";
    Date beginDate = new Date();
    Date endDate = beginDate;
    private final Date expirationDate = DateUtils.addDays(new Date(), 1);
    private final int pagesize = 10;
    private final QueryPersistence persist = QueryPersistence.TRANSIENT;
    
    // need to call the getQueryByName() method. Maybe a partial mock of QueryExecutorBean would be better
    // setup principal mock
    private final String userDN = "CN=Guy Some Other soguy, OU=MY_SUBDIVISION, OU=MY_DIVISION, O=ORG, C=US";
    private final String[] auths = new String[] {"PRIVATE", "PUBLIC"};
    
    private static final Logger log = Logger.getLogger(QueryExecutorBeanTest.class);
    
    // QueryExecutorBean dependencies
    private QueryCache cache;
    private ClosedQueryCache closedCache;
    private AccumuloConnectionRequestBean connectionRequestBean;
    private AccumuloConnectionFactory connectionFactory;
    private AuditBean auditor;
    private AuditService auditService;
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
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
        QueryTraceCache traceCache = new QueryTraceCache();
        Whitebox.invokeMethod(traceCache, "init");
        
        cache = new QueryCache();
        cache.init();
        
        closedCache = new ClosedQueryCache();
        
        bean = new QueryExecutorBean();
        
        connectionFactory = createStrictMock(AccumuloConnectionFactory.class);
        auditor = new AuditBean();
        auditService = createStrictMock(AuditService.class);
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
        setInternalState(auditor, AuditService.class, auditService);
        setInternalState(auditor, AuditParameterBuilder.class, new DefaultAuditParameterBuilder());
        setInternalState(connectionRequestBean, EJBContext.class, ctx);
        setInternalState(bean, QueryCache.class, cache);
        setInternalState(bean, ClosedQueryCache.class, closedCache);
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
    
    private QueryImpl createNewQuery() throws Exception {
        Set<QueryImpl.Parameter> parameters = new HashSet<>();
        
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
        q.setDnList(Collections.singletonList(userDN));
        q.setId(UUID.randomUUID());
        
        return q;
    }
    
    private MultivaluedMap createNewQueryParameterMap() throws Exception {
        MultivaluedMap<String,String> p = new MultivaluedMapImpl<>();
        p.putSingle(QueryParameters.QUERY_STRING, "foo == 'bar'");
        p.putSingle(QueryParameters.QUERY_NAME, "query name");
        p.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, StringUtils.join(auths, ","));
        p.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        p.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        p.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expirationDate));
        p.putSingle(QueryParameters.QUERY_NAME, queryName);
        p.putSingle(QueryParameters.QUERY_PAGESIZE, Integer.toString(pagesize));
        p.putSingle(QueryParameters.QUERY_STRING, query);
        p.putSingle(QueryParameters.QUERY_PERSISTENCE, persist.name());
        p.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, "PRIVATE|PUBLIC");
        
        return p;
    }
    
    private MultivaluedMap createNewQueryParameters(QueryImpl q, MultivaluedMap p) {
        QueryParameters qp = new QueryParametersImpl();
        MultivaluedMap<String,String> optionalParameters = new MultivaluedMapImpl<>();
        optionalParameters.putAll(qp.getUnknownParameters(p));
        optionalParameters.putSingle(PrivateAuditConstants.USER_DN, userDN.toLowerCase());
        optionalParameters.putSingle(PrivateAuditConstants.COLUMN_VISIBILITY, "PRIVATE|PUBLIC");
        optionalParameters.putSingle(PrivateAuditConstants.LOGIC_CLASS, q.getQueryLogicName());
        
        return optionalParameters;
    }
    
    private void defineTestRunner(QueryImpl q, MultivaluedMap p) throws Exception {
        
        MultivaluedMap<String,String> optionalParameters = createNewQueryParameters(q, p);
        
        @SuppressWarnings("rawtypes")
        QueryLogic logic = createMock(BaseQueryLogic.class);
        
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
        EasyMock.expect(logic.containsDNWithAccess(dnList)).andReturn(true);
        EasyMock.expect(logic.getMaxPageSize()).andReturn(0);
        EasyMock.expect(logic.getCollectQueryMetrics()).andReturn(Boolean.FALSE);
        EasyMock.expect(logic.getResultLimit(q.getDnList())).andReturn(-1L);
        EasyMock.expect(logic.getMaxResults()).andReturn(-1L);
        PowerMock.replayAll();
        
        bean.defineQuery(queryLogicName, p);
        
        PowerMock.verifyAll();
        
        Object cachedRunningQuery = cache.get(q.getId().toString());
        Assert.assertNotNull(cachedRunningQuery);
        RunningQuery rq2 = (RunningQuery) cachedRunningQuery;
        Assert.assertEquals(q, rq2.getSettings());
        
    }
    
    @SuppressWarnings("unchecked")
    @Test(expected = DatawaveWebApplicationException.class)
    public void testCreateWithNoSelectedAuths() throws Exception {
        String queryLogicName = "EventQueryLogic";
        String queryName = "Something";
        String query = "FOO == BAR";
        Date beginDate = new Date();
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
        q.setEndDate(beginDate);
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
        p.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, "");
        p.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        p.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(beginDate));
        p.putSingle(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expirationDate));
        p.putSingle(QueryParameters.QUERY_NAME, queryName);
        p.putSingle(QueryParameters.QUERY_PAGESIZE, Integer.toString(pagesize));
        p.putSingle(QueryParameters.QUERY_STRING, query);
        p.putSingle(QueryParameters.QUERY_PERSISTENCE, persist.name());
        p.putSingle(ColumnVisibilitySecurityMarking.VISIBILITY_MARKING, "PRIVATE|PUBLIC");
        
        InMemoryInstance instance = new InMemoryInstance();
        Connector c = instance.getConnector("root", new PasswordToken(""));
        
        QueryParameters qp = new QueryParametersImpl();
        MultivaluedMap<String,String> optionalParameters = new MultivaluedMapImpl<>();
        optionalParameters.putAll(qp.getUnknownParameters(p));
        
        DatawaveUser user = new DatawaveUser(SubjectIssuerDNPair.of(userDN, "<CN=MY_CA, OU=MY_SUBDIVISION, OU=MY_DIVISION, O=ORG, C=US>"), UserType.USER,
                        Arrays.asList(auths), null, null, 0L);
        DatawavePrincipal principal = new DatawavePrincipal(Collections.singletonList(user));
        String[] dns = principal.getDNs();
        Arrays.sort(dns);
        List<String> dnList = Arrays.asList(dns);
        
        PowerMock.resetAll();
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal).anyTimes();
        suppress(constructor(QueryParametersImpl.class));
        EasyMock.expect(persister.create(userDN, dnList, (SecurityMarking) Whitebox.getField(bean.getClass(), "marking").get(bean), queryLogicName,
                        (QueryParameters) Whitebox.getField(bean.getClass(), "qp").get(bean), optionalParameters)).andReturn(q);
        
        EasyMock.expect(queryLogicFactory.getQueryLogic(queryLogicName, principal)).andReturn(logic);
        EasyMock.expect(logic.getRequiredQueryParameters()).andReturn(Collections.EMPTY_SET);
        EasyMock.expect(logic.containsDNWithAccess(dnList)).andReturn(true);
        EasyMock.expect(logic.getMaxPageSize()).andReturn(0);
        EasyMock.expect(logic.getAuditType(EasyMock.<Query> anyObject())).andReturn(AuditType.ACTIVE).anyTimes();
        EasyMock.expect(logic.getSelectors(anyObject())).andReturn(null);
        Map<String,String> auditParams = new HashMap<>();
        auditParams.put(QueryParameters.QUERY_STRING, p.getFirst(QueryParameters.QUERY_STRING));
        auditParams.put(AuditParameters.USER_DN, userDN);
        auditParams.put(AuditParameters.QUERY_SECURITY_MARKING_COLVIZ, "PRIVATE|PUBLIC");
        auditParams.put(AuditParameters.QUERY_AUDIT_TYPE, AuditType.ACTIVE.name());
        auditParams.put(AuditParameters.QUERY_LOGIC_CLASS, "EventQueryLogic");
        EasyMock.expect(auditService.audit(eq(auditParams))).andReturn(null);
        logic.close();
        EasyMock.expectLastCall();
        persister.remove(anyObject(Query.class));
        PowerMock.replayAll();
        
        bean.createQuery(queryLogicName, p);
        
        PowerMock.verifyAll();
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testDefine() throws Exception {
        QueryImpl q = createNewQuery();
        MultivaluedMap p = createNewQueryParameterMap();
        p.putSingle(QueryParameters.QUERY_LOGIC_NAME, "EventQueryLogic");
        
        defineTestRunner(q, p);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testPredict() throws Exception {
        QueryImpl q = createNewQuery();
        MultivaluedMap p = createNewQueryParameterMap();
        p.putSingle(QueryParameters.QUERY_LOGIC_NAME, queryLogicName);
        
        MultivaluedMap<String,String> optionalParameters = createNewQueryParameters(q, p);
        
        @SuppressWarnings("rawtypes")
        QueryLogic logic = createMock(BaseQueryLogic.class);
        
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
        EasyMock.expect(logic.containsDNWithAccess(dnList)).andReturn(true);
        EasyMock.expect(logic.getMaxPageSize()).andReturn(0);
        
        BaseQueryMetric metric = new QueryMetricFactoryImpl().createMetric();
        metric.populate(q);
        metric.setQueryType(RunningQuery.class.getSimpleName());
        
        QueryMetric testMetric = new QueryMetric((QueryMetric) metric) {
            @Override
            public boolean equals(Object o) {
                // test for equality except for the create date
                if (null == o) {
                    return false;
                }
                if (this == o) {
                    return true;
                }
                if (o instanceof QueryMetric) {
                    QueryMetric other = (QueryMetric) o;
                    return new EqualsBuilder().append(this.getQueryId(), other.getQueryId()).append(this.getQueryType(), other.getQueryType())
                                    .append(this.getQueryAuthorizations(), other.getQueryAuthorizations())
                                    .append(this.getColumnVisibility(), other.getColumnVisibility()).append(this.getBeginDate(), other.getBeginDate())
                                    .append(this.getEndDate(), other.getEndDate()).append(this.getCreateDate(), other.getCreateDate())
                                    .append(this.getSetupTime(), other.getSetupTime()).append(this.getUser(), other.getUser())
                                    .append(this.getUserDN(), other.getUserDN()).append(this.getQuery(), other.getQuery())
                                    .append(this.getQueryLogic(), other.getQueryLogic()).append(this.getQueryName(), other.getQueryName())
                                    .append(this.getParameters(), other.getParameters()).append(this.getHost(), other.getHost())
                                    .append(this.getPageTimes(), other.getPageTimes()).append(this.getProxyServers(), other.getProxyServers())
                                    .append(this.getLifecycle(), other.getLifecycle()).append(this.getErrorMessage(), other.getErrorMessage())
                                    .append(this.getErrorCode(), other.getErrorCode()).append(this.getSourceCount(), other.getSourceCount())
                                    .append(this.getNextCount(), other.getNextCount()).append(this.getSeekCount(), other.getSeekCount())
                                    .append(this.getYieldCount(), other.getYieldCount()).append(this.getDocRanges(), other.getDocRanges())
                                    .append(this.getFiRanges(), other.getFiRanges()).append(this.getPlan(), other.getPlan())
                                    .append(this.getVersion(), other.getVersion()).append(this.getLoginTime(), other.getLoginTime())
                                    .append(this.getPredictions(), other.getPredictions()).isEquals();
                } else {
                    return false;
                }
                
            }
        };
        
        Set<Prediction> predictions = new HashSet<>();
        predictions.add(new Prediction("source", 1));
        EasyMock.expect(predictor.predict(EasyMock.eq(testMetric))).andReturn(predictions);
        
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
    public void testListWithNoName() throws URISyntaxException {
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
    
    private void nullDateTestRunner(boolean nullStart, boolean nullEnd) throws Exception {
        QueryImpl q = createNewQuery();
        MultivaluedMap p = createNewQueryParameterMap();
        
        if (nullStart) {
            q.setBeginDate(null);
            p.remove(QueryParameters.QUERY_BEGIN);
            p.putSingle(QueryParameters.QUERY_BEGIN, null);
        }
        
        if (nullEnd) {
            q.setEndDate(null);
            p.remove(QueryParameters.QUERY_END);
            p.putSingle(QueryParameters.QUERY_END, null);
        }
        
        defineTestRunner(q, p);
    }
    
    @Test
    public void testBothDatesNull() throws Exception {
        nullDateTestRunner(true, true);
    }
    
    @Test
    public void testStartDateNull() throws Exception {
        nullDateTestRunner(true, false);
    }
    
    @Test
    public void testEndDateNull() throws Exception {
        nullDateTestRunner(false, true);
    }
    
    @Test
    public void testBeginDateAfterEndDate() throws Exception {
        final Date beginDate = new Date(2018, 1, 2);
        final Date endDate = new Date(2018, 1, 1);
        
        final MultivaluedMap<String,String> queryParameters = createNewQueryParameterMap();
        queryParameters.remove(QueryParameters.QUERY_BEGIN);
        queryParameters.remove(QueryParameters.QUERY_END);
        queryParameters.putSingle(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate));
        queryParameters.putSingle(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate));
        
        try {
            queryParameters.putSingle(QueryParameters.QUERY_LOGIC_NAME, "EventQueryLogic");
            bean.createQuery("EventQueryLogic", queryParameters);
            fail(); // If doesn't throw exception, should fail
        } catch (BadRequestException e) {
            assertEquals(DatawaveErrorCode.BEGIN_DATE_AFTER_END_DATE.toString(), e.getCause().getMessage());
        }
    }
    
    @SuppressWarnings("unchecked")
    @Test(timeout = 5000)
    public void testCloseActuallyCloses() throws Exception {
        QueryImpl q = createNewQuery();
        
        final MultivaluedMap<String,String> queryParameters = createNewQueryParameterMap();
        queryParameters.putSingle(QueryParameters.QUERY_LOGIC_NAME, "EventQueryLogic");
        
        final Thread createQuery = new Thread(() -> {
            try {
                bean.createQuery("EventQueryLogic", queryParameters);
            } catch (Exception e) {
                // ok if we fail the call
                        log.debug("createQuery terminated with " + e);
                    }
                });
        
        final Throwable[] createQueryException = {null};
        createQuery.setUncaughtExceptionHandler((t, e) -> createQueryException[0] = e);
        
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
        
        MultivaluedMap<String,String> optionalParameters = createNewQueryParameters(q, queryParameters);
        
        PowerMock.resetAll();
        EasyMock.expect(ctx.getCallerPrincipal()).andReturn(principal).anyTimes();
        EasyMock.expect(logic.getAuditType(null)).andReturn(AuditType.NONE);
        EasyMock.expect(persister.create(principal.getUserDN().subjectDN(), dnList, Whitebox.getInternalState(bean, SecurityMarking.class), queryLogicName,
                        Whitebox.getInternalState(bean, QueryParameters.class), optionalParameters)).andReturn(q);
        EasyMock.expect(persister.findById(EasyMock.anyString())).andReturn(null).anyTimes();
        EasyMock.expect(connectionFactory.getTrackingMap(anyObject())).andReturn(Maps.newHashMap()).anyTimes();
        
        BaseQueryMetric metric = new QueryMetricFactoryImpl().createMetric();
        metric.populate(q);
        EasyMock.expectLastCall();
        metric.setQueryType(RunningQuery.class.getSimpleName());
        metric.setLifecycle(Lifecycle.DEFINED);
        System.out.println(metric);
        
        Set<Prediction> predictions = new HashSet<>();
        predictions.add(new Prediction("source", 1));
        EasyMock.expect(predictor.predict(metric)).andReturn(predictions);
        
        connectionRequestBean.requestBegin(q.getId().toString());
        EasyMock.expectLastCall();
        EasyMock.expect(connectionFactory.getConnection(eq("connPool1"), anyObject(), anyObject())).andReturn(c).anyTimes();
        connectionRequestBean.requestEnd(q.getId().toString());
        EasyMock.expectLastCall();
        connectionFactory.returnConnection(c);
        EasyMock.expectLastCall();
        
        EasyMock.expect(queryLogicFactory.getQueryLogic(queryLogicName, principal)).andReturn(logic);
        EasyMock.expect(logic.getRequiredQueryParameters()).andReturn(Collections.emptySet());
        EasyMock.expect(logic.getConnectionPriority()).andReturn(AccumuloConnectionFactory.Priority.NORMAL).atLeastOnce();
        EasyMock.expect(logic.containsDNWithAccess(dnList)).andReturn(true);
        EasyMock.expect(logic.getMaxPageSize()).andReturn(0);
        EasyMock.expect(logic.getAuditType(q)).andReturn(AuditType.NONE);
        EasyMock.expect(logic.getConnPoolName()).andReturn("connPool1");
        EasyMock.expect(logic.getResultLimit(eq(q.getDnList()))).andReturn(-1L).anyTimes();
        EasyMock.expect(logic.getMaxResults()).andReturn(-1L).anyTimes();
        
        EasyMock.expect(connectionRequestBean.cancelConnectionRequest(q.getId().toString(), principal)).andReturn(false).anyTimes();
        connectionFactory.returnConnection(EasyMock.isA(Connector.class));
        
        final AtomicBoolean initializeLooping = new AtomicBoolean(false);
        
        // During initialize, mark that we get here, and then sleep
        final IAnswer<GenericQueryConfiguration> initializeAnswer = () -> {
            initializeLooping.set(true);
            try {
                while (true) {
                    Thread.sleep(1000);
                    log.debug("Initialize: woke up");
                }
            } catch (InterruptedException e) {
                throw new QueryException("EXPECTED EXCEPTION: initialize interrupted");
            }
        };
        
        EasyMock.expect(logic.initialize(anyObject(Connector.class), anyObject(Query.class), anyObject(Set.class))).andAnswer(initializeAnswer);
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
        EasyMock.expectLastCall().andAnswer(closeAnswer).anyTimes();
        
        // Make the QueryLogic mock not threadsafe, otherwise it will be blocked infinitely
        // trying to get the lock on the infinite loop
        EasyMock.makeThreadSafe(logic, false);
        
        metrics.updateMetric(EasyMock.isA(QueryMetric.class));
        
        PowerMock.replayAll();
        try {
            createQuery.start();
            
            // Wait for the create call to get to initialize
            while (!initializeLooping.get()) {
                if (!createQuery.isAlive() && !initializeLooping.get()) {
                    Assert.fail("createQuery thread died before reaching initialize: " + createQueryException[0]);
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
