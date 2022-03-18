package datawave.webservice.query.interceptor;

import com.google.common.io.CountingOutputStream;
import datawave.microservice.querymetric.QueryMetric;
import datawave.microservice.querymetric.BaseQueryMetric.PageMetric;
import datawave.security.util.DnUtils.NpeUtils;
import datawave.webservice.query.annotation.EnrichQueryMetrics;
import datawave.webservice.query.cache.QueryCache;
import datawave.webservice.query.interceptor.QueryMetricsEnrichmentInterceptor.QueryCall;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.result.BaseQueryResponse;
import org.easymock.Capture;
import org.easymock.IAnswer;
import org.jboss.resteasy.core.interception.ContainerResponseContextImpl;
import org.jboss.resteasy.core.interception.PreMatchContainerRequestContext;
import org.jboss.resteasy.specimpl.BuiltResponse;
import org.jboss.resteasy.spi.HttpRequest;
import org.jboss.resteasy.spi.ResteasyUriInfo;
import org.jboss.resteasy.util.FindAnnotation;
import org.jboss.resteasy.util.HttpResponseCodes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.WriterInterceptorContext;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.UUID;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.geq;
import static org.easymock.EasyMock.gt;
import static org.easymock.EasyMock.isA;
import static org.powermock.reflect.Whitebox.setInternalState;

@RunWith(PowerMockRunner.class)
@PrepareForTest(FindAnnotation.class)
public class QueryMetricsEnrichmentInterceptorTest {
    
    @Mock
    private BaseQueryResponse baseQueryResponse;
    
    @Mock
    private MultivaluedMap<String,String> decodedFormParameters;
    
    @Mock
    private EnrichQueryMetrics enrichQueryMetrics;
    
    @Mock
    private PageMetric pageTime;
    
    @Mock
    private HttpRequest httpRequest;
    
    @Mock
    private PreMatchContainerRequestContext requestContext;
    
    @Mock
    private ContainerResponseContextImpl responseContext;
    
    @Mock
    private InitialContext initialContext;
    
    @Mock
    private OutputStream outputStream;
    
    @Mock
    private QueryCache queryCache;
    
    @Mock
    private QueryMetric queryMetric;
    
    @Mock
    private QueryMetricsBean queryMetrics;
    
    private MultivaluedMap<String,String> requestHeaders;
    
    @Mock
    private RunningQuery runningQuery;
    
    @Mock
    private ResteasyUriInfo uriInfo;
    
    @Mock
    private BaseQueryLogic queryLogic;
    
    private MultivaluedMap<String,Object> writeHeaders;
    
    @Mock
    private BuiltResponse jaxrsResponse;
    
    @Mock
    private WriterInterceptorContext writerContext;
    
    @Before
    public void setup() {
        System.setProperty(NpeUtils.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
        
        // noinspection unchecked
        requestHeaders = PowerMock.createStrictMock(MultivaluedMap.class);
        // noinspection unchecked
        writeHeaders = PowerMock.createStrictMock(MultivaluedMap.class);
    }
    
    @Test
    public void testPreProcess_HappyPath() throws Exception {
        QueryMetricsEnrichmentInterceptor subject = new QueryMetricsEnrichmentInterceptor();
        
        // Assign local variables
        URI requestUri = new URI("http://localhost/test");
        
        // Set expectations
        expect(requestContext.getUriInfo()).andReturn(uriInfo);
        expect(uriInfo.getRequestUri()).andReturn(requestUri);
        expect(requestContext.getMethod()).andReturn(null);
        expect(requestContext.getHeaders()).andReturn(requestHeaders);
        expect(requestHeaders.keySet()).andReturn(new HashSet<>());
        expect(requestContext.getMediaType()).andReturn(MediaType.APPLICATION_FORM_URLENCODED_TYPE);
        expect(requestContext.getHttpRequest()).andReturn(httpRequest);
        expect(httpRequest.getDecodedFormParameters()).andReturn(decodedFormParameters);
        expect(decodedFormParameters.keySet()).andReturn(new HashSet<>());
        requestContext.setProperty(eq((String) Whitebox.getInternalState(subject, "REQUEST_STATS_NAME")), anyObject());
        
        // Run the test
        PowerMock.replayAll();
        subject.filter(requestContext);
        
        // Verify results
        PowerMock.verifyAll();
    }
    
    @Test
    public void testPostProcess_BaseQueryResponse() throws Exception {
        QueryMetricsEnrichmentInterceptor subject = new QueryMetricsEnrichmentInterceptor();
        
        // Set expectations
        expect(responseContext.getHeaders()).andReturn(writeHeaders);
        expect(writeHeaders.keySet()).andReturn(new HashSet<>());
        expect(responseContext.getStatus()).andReturn(HttpResponseCodes.SC_OK);
        expect(responseContext.getJaxrsResponse()).andReturn(jaxrsResponse);
        expect(jaxrsResponse.getAnnotations()).andReturn(new Annotation[] {enrichQueryMetrics});
        PowerMock.mockStaticPartial(FindAnnotation.class, "findAnnotation");
        expect(FindAnnotation.findAnnotation(isA(Annotation[].class), eq(EnrichQueryMetrics.class))).andReturn(this.enrichQueryMetrics);
        expect(responseContext.getEntity()).andReturn(baseQueryResponse);
        expect(enrichQueryMetrics.methodType()).andReturn(EnrichQueryMetrics.MethodType.CREATE);
        expect(baseQueryResponse.getQueryId()).andReturn(UUID.randomUUID().toString());
        requestContext.setProperty(eq((String) Whitebox.getInternalState(subject, "RESPONSE_STATS_NAME")), anyObject());
        requestContext.setProperty(eq(QueryCall.class.getName()), isA(QueryCall.class));
        
        // Run the test
        PowerMock.replayAll();
        subject.filter(requestContext, responseContext);
        PowerMock.verifyAll();
    }
    
    @Test
    public void testWrite_UncheckedException() throws Exception {
        QueryMetricsEnrichmentInterceptor subject = new QueryMetricsEnrichmentInterceptor();
        
        // Simulate the initial context
        TestInitialContextFactory.INITIAL_CONTEXT = this.initialContext;
        
        final Capture<QueryCall> qcCapture = Capture.newInstance();
        
        // Set expectations for the postProcess
        expect(responseContext.getHeaders()).andReturn(writeHeaders);
        expect(writeHeaders.keySet()).andReturn(new HashSet<>());
        expect(responseContext.getStatus()).andReturn(HttpResponseCodes.SC_OK);
        expect(responseContext.getJaxrsResponse()).andReturn(jaxrsResponse);
        requestContext.setProperty(eq((String) Whitebox.getInternalState(subject, "RESPONSE_STATS_NAME")), anyObject());
        expect(jaxrsResponse.getAnnotations()).andReturn(new Annotation[] {enrichQueryMetrics});
        PowerMock.mockStaticPartial(FindAnnotation.class, "findAnnotation");
        expect(FindAnnotation.findAnnotation(isA(Annotation[].class), eq(EnrichQueryMetrics.class))).andReturn(this.enrichQueryMetrics);
        expect(responseContext.getEntity()).andReturn(baseQueryResponse);
        expect(enrichQueryMetrics.methodType()).andReturn(EnrichQueryMetrics.MethodType.CREATE);
        expect(baseQueryResponse.getQueryId()).andReturn(UUID.randomUUID().toString());
        requestContext.setProperty(eq(QueryCall.class.getName()), capture(qcCapture));
        
        // Set expectations for the write
        expect(writerContext.getOutputStream()).andReturn(outputStream);
        writerContext.setOutputStream(isA(CountingOutputStream.class));
        writerContext.setOutputStream(outputStream);
        expect(writerContext.getHeaders()).andReturn(writeHeaders);
        expect(writeHeaders.entrySet()).andReturn(new HashSet<>());
        writerContext.proceed();
        expect(writerContext.getProperty(eq((String) Whitebox.getInternalState(subject, "RESPONSE_STATS_NAME")))).andReturn(null);
        expect(writerContext.getProperty(eq((String) Whitebox.getInternalState(subject, "REQUEST_STATS_NAME")))).andReturn(null);
        expect(writerContext.getProperty(QueryCall.class.getName())).andAnswer((IAnswer<QueryCall>) qcCapture::getValue);
        expect(queryCache.get(isA(String.class))).andReturn(runningQuery);
        expect(runningQuery.getLogic()).andReturn(queryLogic);
        expect(queryLogic.getCollectQueryMetrics()).andReturn(true);
        expect(runningQuery.getMetric()).andThrow(new IllegalStateException("INTENTIONALLY THROWN UNCHECKED TEST EXCEPTION"));
        
        // Run the test
        PowerMock.replayAll();
        
        try {
            // Set the initial context factory
            System.setProperty(InitialContext.INITIAL_CONTEXT_FACTORY, TestInitialContextFactory.class.getName());
            
            // Create and test the test subject
            setInternalState(subject, QueryCache.class, queryCache);
            setInternalState(subject, QueryMetricsBean.class, queryMetrics);
            subject.filter(requestContext, responseContext);
            subject.aroundWriteTo(writerContext);
        } finally {
            // Remove the initial context factory
            System.clearProperty(InitialContext.INITIAL_CONTEXT_FACTORY);
        }
        PowerMock.verifyAll();
    }
    
    @Test
    public void testWrite_CreateQidResponse() throws Exception {
        QueryMetricsEnrichmentInterceptor subject = new QueryMetricsEnrichmentInterceptor();
        
        // Simulate the initial context
        TestInitialContextFactory.INITIAL_CONTEXT = this.initialContext;
        
        final Capture<QueryCall> qcCapture = Capture.newInstance();
        
        // Set expectations for the postProcess
        expect(responseContext.getHeaders()).andReturn(writeHeaders);
        expect(writeHeaders.keySet()).andReturn(new HashSet<>());
        expect(responseContext.getStatus()).andReturn(HttpResponseCodes.SC_OK);
        expect(responseContext.getJaxrsResponse()).andReturn(jaxrsResponse);
        requestContext.setProperty(eq((String) Whitebox.getInternalState(subject, "RESPONSE_STATS_NAME")), anyObject());
        expect(jaxrsResponse.getAnnotations()).andReturn(new Annotation[] {enrichQueryMetrics});
        PowerMock.mockStaticPartial(FindAnnotation.class, "findAnnotation");
        expect(FindAnnotation.findAnnotation(isA(Annotation[].class), eq(EnrichQueryMetrics.class))).andReturn(this.enrichQueryMetrics);
        expect(responseContext.getEntity()).andReturn(baseQueryResponse);
        expect(enrichQueryMetrics.methodType()).andReturn(EnrichQueryMetrics.MethodType.CREATE);
        expect(baseQueryResponse.getQueryId()).andReturn(UUID.randomUUID().toString());
        requestContext.setProperty(eq(QueryCall.class.getName()), capture(qcCapture));
        
        // Set expectations for the write
        expect(writerContext.getOutputStream()).andReturn(outputStream);
        writerContext.setOutputStream(isA(CountingOutputStream.class));
        writerContext.setOutputStream(outputStream);
        expect(writerContext.getHeaders()).andReturn(writeHeaders);
        expect(writeHeaders.entrySet()).andReturn(new HashSet<>());
        writerContext.proceed();
        expect(writerContext.getProperty(eq((String) Whitebox.getInternalState(subject, "RESPONSE_STATS_NAME")))).andReturn(null);
        expect(writerContext.getProperty(eq((String) Whitebox.getInternalState(subject, "REQUEST_STATS_NAME")))).andReturn(null);
        expect(writerContext.getProperty(QueryCall.class.getName())).andAnswer((IAnswer<QueryCall>) qcCapture::getValue);
        expect(queryCache.get(isA(String.class))).andReturn(runningQuery);
        expect(runningQuery.getLogic()).andReturn(queryLogic);
        expect(queryLogic.getCollectQueryMetrics()).andReturn(true);
        expect(runningQuery.getMetric()).andReturn(queryMetric);
        queryMetric.setCreateCallTime(gt(-2L));
        queryMetric.setLoginTime(-1L);
        queryMetrics.updateMetric(queryMetric);
        
        // Run the test
        PowerMock.replayAll();
        
        try {
            // Set the initial context factory
            System.setProperty(InitialContext.INITIAL_CONTEXT_FACTORY, TestInitialContextFactory.class.getName());
            
            // Create and test the test subject
            setInternalState(subject, QueryCache.class, queryCache);
            setInternalState(subject, QueryMetricsBean.class, queryMetrics);
            subject.filter(requestContext, responseContext);
            subject.aroundWriteTo(writerContext);
        } finally {
            // Remove the initial context factory
            System.clearProperty(InitialContext.INITIAL_CONTEXT_FACTORY);
        }
        PowerMock.verifyAll();
    }
    
    @Test
    public void testWrite_CreateAndNextQidResponse() throws Exception {
        QueryMetricsEnrichmentInterceptor subject = new QueryMetricsEnrichmentInterceptor();
        
        // Simulate the initial context
        TestInitialContextFactory.INITIAL_CONTEXT = this.initialContext;
        
        final Capture<QueryCall> qcCapture = Capture.newInstance();
        
        // Set expectations for the postProcess
        expect(responseContext.getHeaders()).andReturn(writeHeaders);
        expect(writeHeaders.keySet()).andReturn(new HashSet<>());
        expect(responseContext.getStatus()).andReturn(HttpResponseCodes.SC_OK);
        expect(responseContext.getJaxrsResponse()).andReturn(jaxrsResponse);
        requestContext.setProperty(eq((String) Whitebox.getInternalState(subject, "RESPONSE_STATS_NAME")), anyObject());
        expect(jaxrsResponse.getAnnotations()).andReturn(new Annotation[] {enrichQueryMetrics});
        PowerMock.mockStaticPartial(FindAnnotation.class, "findAnnotation");
        expect(FindAnnotation.findAnnotation(isA(Annotation[].class), eq(EnrichQueryMetrics.class))).andReturn(this.enrichQueryMetrics);
        expect(responseContext.getEntity()).andReturn(baseQueryResponse);
        expect(enrichQueryMetrics.methodType()).andReturn(EnrichQueryMetrics.MethodType.CREATE_AND_NEXT);
        expect(baseQueryResponse.getQueryId()).andReturn(UUID.randomUUID().toString());
        requestContext.setProperty(eq(QueryCall.class.getName()), capture(qcCapture));
        
        // Set expectations for the write
        expect(writerContext.getOutputStream()).andReturn(outputStream);
        writerContext.setOutputStream(isA(CountingOutputStream.class));
        writerContext.setOutputStream(outputStream);
        expect(writerContext.getHeaders()).andReturn(writeHeaders);
        expect(writeHeaders.entrySet()).andReturn(new HashSet<>());
        writerContext.proceed();
        expect(writerContext.getProperty(QueryCall.class.getName())).andAnswer((IAnswer<QueryCall>) qcCapture::getValue);
        expect(writerContext.getProperty(eq((String) Whitebox.getInternalState(subject, "RESPONSE_STATS_NAME")))).andReturn(null);
        expect(writerContext.getProperty(eq((String) Whitebox.getInternalState(subject, "REQUEST_STATS_NAME")))).andReturn(null);
        expect(queryCache.get(isA(String.class))).andReturn(runningQuery);
        expect(runningQuery.getLogic()).andReturn(queryLogic);
        expect(queryLogic.getCollectQueryMetrics()).andReturn(true);
        expect(runningQuery.getMetric()).andReturn(queryMetric);
        expect(queryMetric.getPageTimes()).andReturn(Arrays.asList(pageTime));
        queryMetric.setCreateCallTime(eq(-1L));
        queryMetric.setLoginTime(-1L);
        pageTime.setCallTime(-1L);
        pageTime.setLoginTime(-1L);
        pageTime.setSerializationTime(geq(0L));
        pageTime.setBytesWritten(0L);
        queryMetrics.updateMetric(queryMetric);
        
        // Run the test
        PowerMock.replayAll();
        
        try {
            // Set the initial context factory
            System.setProperty(InitialContext.INITIAL_CONTEXT_FACTORY, TestInitialContextFactory.class.getName());
            
            // Create and test the test subject
            setInternalState(subject, QueryCache.class, queryCache);
            setInternalState(subject, QueryMetricsBean.class, queryMetrics);
            subject.filter(requestContext, responseContext);
            subject.aroundWriteTo(writerContext);
        } finally {
            // Remove the initial context factory
            System.clearProperty(InitialContext.INITIAL_CONTEXT_FACTORY);
        }
        PowerMock.verifyAll();
    }
    
    @Test
    public void testWrite_NextQidResponse() throws Exception {
        QueryMetricsEnrichmentInterceptor subject = new QueryMetricsEnrichmentInterceptor();
        
        // Simulate the initial context
        TestInitialContextFactory.INITIAL_CONTEXT = this.initialContext;
        
        final Capture<QueryCall> qcCapture = Capture.newInstance();
        
        // Set expectations for the postProcess
        expect(responseContext.getHeaders()).andReturn(writeHeaders);
        expect(writeHeaders.keySet()).andReturn(new HashSet<>());
        expect(responseContext.getStatus()).andReturn(HttpResponseCodes.SC_OK);
        expect(responseContext.getJaxrsResponse()).andReturn(jaxrsResponse);
        requestContext.setProperty(eq((String) Whitebox.getInternalState(subject, "RESPONSE_STATS_NAME")), anyObject());
        expect(jaxrsResponse.getAnnotations()).andReturn(new Annotation[] {enrichQueryMetrics});
        PowerMock.mockStaticPartial(FindAnnotation.class, "findAnnotation");
        expect(FindAnnotation.findAnnotation(isA(Annotation[].class), eq(EnrichQueryMetrics.class))).andReturn(this.enrichQueryMetrics);
        expect(responseContext.getEntity()).andReturn(baseQueryResponse);
        expect(enrichQueryMetrics.methodType()).andReturn(EnrichQueryMetrics.MethodType.NEXT);
        expect(baseQueryResponse.getQueryId()).andReturn(UUID.randomUUID().toString());
        requestContext.setProperty(eq(QueryCall.class.getName()), capture(qcCapture));
        
        // Set expectations for the write
        expect(writerContext.getOutputStream()).andReturn(outputStream);
        writerContext.setOutputStream(isA(CountingOutputStream.class));
        writerContext.setOutputStream(outputStream);
        expect(writerContext.getHeaders()).andReturn(writeHeaders);
        expect(writeHeaders.entrySet()).andReturn(new HashSet<>());
        writerContext.proceed();
        expect(writerContext.getProperty(eq((String) Whitebox.getInternalState(subject, "RESPONSE_STATS_NAME")))).andReturn(null);
        expect(writerContext.getProperty(eq((String) Whitebox.getInternalState(subject, "REQUEST_STATS_NAME")))).andReturn(null);
        expect(writerContext.getProperty(QueryCall.class.getName())).andAnswer((IAnswer<QueryCall>) qcCapture::getValue);
        expect(queryCache.get(isA(String.class))).andReturn(runningQuery);
        expect(runningQuery.getLogic()).andReturn(queryLogic);
        expect(queryLogic.getCollectQueryMetrics()).andReturn(true);
        expect(runningQuery.getMetric()).andReturn(queryMetric);
        expect(queryMetric.getPageTimes()).andReturn(Arrays.asList(pageTime));
        pageTime.setCallTime(-1L);
        pageTime.setLoginTime(-1L);
        pageTime.setSerializationTime(geq(0L));
        pageTime.setBytesWritten(0L);
        queryMetrics.updateMetric(queryMetric);
        
        // Run the test
        PowerMock.replayAll();
        
        try {
            // Set the initial context factory
            System.setProperty(InitialContext.INITIAL_CONTEXT_FACTORY, TestInitialContextFactory.class.getName());
            
            // Create and test the test subject
            setInternalState(subject, QueryCache.class, queryCache);
            setInternalState(subject, QueryMetricsBean.class, queryMetrics);
            subject.filter(requestContext, responseContext);
            subject.aroundWriteTo(writerContext);
        } finally {
            // Remove the initial context factory
            System.clearProperty(InitialContext.INITIAL_CONTEXT_FACTORY);
        }
        PowerMock.verifyAll();
    }
    
    public static class TestInitialContextFactory implements InitialContextFactory {
        static InitialContext INITIAL_CONTEXT;
        
        @Override
        public Context getInitialContext(Hashtable<?,?> arg0) throws NamingException {
            return INITIAL_CONTEXT;
        }
    }
}
