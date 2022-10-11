package datawave.webservice.query.interceptor;

import com.google.common.io.CountingOutputStream;
import datawave.microservice.querymetric.BaseQueryMetric.PageMetric;
import datawave.microservice.querymetric.QueryMetric;
import datawave.security.util.DnUtils.NpeUtils;
import datawave.webservice.query.annotation.EnrichQueryMetrics;
import datawave.webservice.query.cache.QueryCache;
import datawave.webservice.query.interceptor.QueryMetricsEnrichmentInterceptor.QueryCall;
import datawave.webservice.query.logic.BaseQueryLogic;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.result.BaseQueryResponse;
import org.jboss.resteasy.core.interception.ContainerResponseContextImpl;
import org.jboss.resteasy.core.interception.PreMatchContainerRequestContext;
import org.jboss.resteasy.specimpl.BuiltResponse;
import org.jboss.resteasy.spi.HttpRequest;
import org.jboss.resteasy.spi.ResteasyUriInfo;
import org.jboss.resteasy.util.FindAnnotation;
import org.jboss.resteasy.util.HttpResponseCodes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

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
import java.util.HashSet;
import java.util.Hashtable;
import java.util.UUID;

import static org.mockito.AdditionalMatchers.geq;
import static org.mockito.AdditionalMatchers.gt;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
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
    
    @Mock
    private MultivaluedMap<String,String> requestHeaders;
    
    @Mock
    private RunningQuery runningQuery;
    
    @Mock
    private ResteasyUriInfo uriInfo;
    
    @Mock
    private BaseQueryLogic queryLogic;
    
    @Mock
    private MultivaluedMap<String,Object> writeHeaders;
    
    @Mock
    private BuiltResponse jaxrsResponse;
    
    @Mock
    private WriterInterceptorContext writerContext;
    
    @BeforeEach
    public void setup() {
        System.setProperty(NpeUtils.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
    }
    
    @AfterEach
    public void tearDown() {
        System.clearProperty(InitialContext.INITIAL_CONTEXT_FACTORY);
    }
    
    @Test
    public void testPreProcess_HappyPath() throws Exception {
        QueryMetricsEnrichmentInterceptor subject = new QueryMetricsEnrichmentInterceptor();
        
        // Assign local variables
        URI requestUri = new URI("http://localhost/test");
        
        // Set expectations
        when(requestContext.getUriInfo()).thenReturn(uriInfo);
        when(uriInfo.getRequestUri()).thenReturn(requestUri);
        when(requestContext.getMethod()).thenReturn(null);
        when(requestContext.getHeaders()).thenReturn(requestHeaders);
        when(requestHeaders.keySet()).thenReturn(new HashSet<>());
        when(requestContext.getMediaType()).thenReturn(MediaType.APPLICATION_FORM_URLENCODED_TYPE);
        when(requestContext.getHttpRequest()).thenReturn(httpRequest);
        when(httpRequest.getDecodedFormParameters()).thenReturn(decodedFormParameters);
        when(decodedFormParameters.keySet()).thenReturn(new HashSet<>());
        requestContext.setProperty(eq((String) ReflectionTestUtils.getField(subject, "REQUEST_STATS_NAME")), any());
        
        // Run the test
        subject.filter(requestContext);
        
    }
    
    @Test
    public void testPostProcess_BaseQueryResponse() throws Exception {
        try (MockedStatic<FindAnnotation> findAnnotationMock = Mockito.mockStatic(FindAnnotation.class)) {
            findAnnotationMock.when(() -> FindAnnotation.findAnnotation(isA(Annotation[].class), eq(EnrichQueryMetrics.class))).thenReturn(
                            this.enrichQueryMetrics);
            QueryMetricsEnrichmentInterceptor subject = new QueryMetricsEnrichmentInterceptor();
            
            // Set expectations
            when(responseContext.getHeaders()).thenReturn(writeHeaders);
            when(writeHeaders.keySet()).thenReturn(new HashSet<>());
            when(responseContext.getStatus()).thenReturn(HttpResponseCodes.SC_OK);
            when(responseContext.getJaxrsResponse()).thenReturn(jaxrsResponse);
            when(jaxrsResponse.getAnnotations()).thenReturn(new Annotation[] {enrichQueryMetrics});
            when(responseContext.getEntity()).thenReturn(baseQueryResponse);
            when(enrichQueryMetrics.methodType()).thenReturn(EnrichQueryMetrics.MethodType.CREATE);
            when(baseQueryResponse.getQueryId()).thenReturn(UUID.randomUUID().toString());
            requestContext.setProperty(eq((String) ReflectionTestUtils.getField(subject, "RESPONSE_STATS_NAME")), any());
            requestContext.setProperty(eq(QueryCall.class.getName()), isA(QueryCall.class));
            
            // Run the test
            subject.filter(requestContext, responseContext);
        }
    }
    
    @Test
    public void testWrite_UncheckedException() throws Exception {
        try (MockedStatic<FindAnnotation> findAnnotationMock = Mockito.mockStatic(FindAnnotation.class)) {
            findAnnotationMock.when(() -> FindAnnotation.findAnnotation(isA(Annotation[].class), eq(EnrichQueryMetrics.class))).thenReturn(
                            this.enrichQueryMetrics);
            QueryMetricsEnrichmentInterceptor subject = new QueryMetricsEnrichmentInterceptor();
            
            // Simulate the initial context
            TestInitialContextFactory.INITIAL_CONTEXT = this.initialContext;
            
            // final Capture<QueryCall> qcCapture = Capture.newInstance();
            
            // Set expectations for the postProcess
            when(responseContext.getHeaders()).thenReturn(writeHeaders);
            when(writeHeaders.keySet()).thenReturn(new HashSet<>());
            when(responseContext.getStatus()).thenReturn(HttpResponseCodes.SC_OK);
            when(responseContext.getJaxrsResponse()).thenReturn(jaxrsResponse);
            requestContext.setProperty(eq((String) ReflectionTestUtils.getField(subject, "RESPONSE_STATS_NAME")), any());
            when(jaxrsResponse.getAnnotations()).thenReturn(new Annotation[] {enrichQueryMetrics});
            when(responseContext.getEntity()).thenReturn(baseQueryResponse);
            when(enrichQueryMetrics.methodType()).thenReturn(EnrichQueryMetrics.MethodType.CREATE);
            when(baseQueryResponse.getQueryId()).thenReturn(UUID.randomUUID().toString());
            // requestContext.setProperty(eq(QueryCall.class.getName()), capture(qcCapture));
            
            // Set expectations for the write
            when(writerContext.getOutputStream()).thenReturn(outputStream);
            writerContext.setOutputStream(isA(CountingOutputStream.class));
            writerContext.setOutputStream(outputStream);
            when(writerContext.getHeaders()).thenReturn(writeHeaders);
            when(writeHeaders.entrySet()).thenReturn(new HashSet<>());
            writerContext.proceed();
            when(writerContext.getProperty(eq((String) ReflectionTestUtils.getField(subject, "RESPONSE_STATS_NAME")))).thenReturn(null);
            when(writerContext.getProperty(eq((String) ReflectionTestUtils.getField(subject, "REQUEST_STATS_NAME")))).thenReturn(null);
            
            // Run the test
            // Set the initial context factory
            System.setProperty(InitialContext.INITIAL_CONTEXT_FACTORY, TestInitialContextFactory.class.getName());
            
            // Create and test the test subject
            ReflectionTestUtils.setField(subject, "queryCache", queryCache);
            ReflectionTestUtils.setField(subject, "queryMetricsBean", queryMetrics);
            subject.filter(requestContext, responseContext);
            subject.aroundWriteTo(writerContext);
        }
    }
    
    @Test
    public void testWrite_CreateQidResponse() throws Exception {
        try (MockedStatic<FindAnnotation> findAnnotationMock = Mockito.mockStatic(FindAnnotation.class)) {
            findAnnotationMock.when(() -> FindAnnotation.findAnnotation(isA(Annotation[].class), eq(EnrichQueryMetrics.class))).thenReturn(
                            this.enrichQueryMetrics);
            QueryMetricsEnrichmentInterceptor subject = new QueryMetricsEnrichmentInterceptor();
            
            // Simulate the initial context
            TestInitialContextFactory.INITIAL_CONTEXT = this.initialContext;
            
            // final Capture<QueryCall> qcCapture = Capture.newInstance();
            
            // Set expectations for the postProcess
            when(responseContext.getHeaders()).thenReturn(writeHeaders);
            when(writeHeaders.keySet()).thenReturn(new HashSet<>());
            when(responseContext.getStatus()).thenReturn(HttpResponseCodes.SC_OK);
            when(responseContext.getJaxrsResponse()).thenReturn(jaxrsResponse);
            requestContext.setProperty(eq((String) ReflectionTestUtils.getField(subject, "RESPONSE_STATS_NAME")), any());
            when(jaxrsResponse.getAnnotations()).thenReturn(new Annotation[] {enrichQueryMetrics});
            when(responseContext.getEntity()).thenReturn(baseQueryResponse);
            when(enrichQueryMetrics.methodType()).thenReturn(EnrichQueryMetrics.MethodType.CREATE);
            when(baseQueryResponse.getQueryId()).thenReturn(UUID.randomUUID().toString());
            
            // Set expectations for the write
            when(writerContext.getOutputStream()).thenReturn(outputStream);
            writerContext.setOutputStream(isA(CountingOutputStream.class));
            writerContext.setOutputStream(outputStream);
            when(writerContext.getHeaders()).thenReturn(writeHeaders);
            when(writeHeaders.entrySet()).thenReturn(new HashSet<>());
            writerContext.proceed();
            when(writerContext.getProperty(eq((String) ReflectionTestUtils.getField(subject, "RESPONSE_STATS_NAME")))).thenReturn(null);
            when(writerContext.getProperty(eq((String) ReflectionTestUtils.getField(subject, "REQUEST_STATS_NAME")))).thenReturn(null);
            queryMetric.setCreateCallTime(gt(-2L));
            queryMetric.setLoginTime(-1L);
            queryMetrics.updateMetric(queryMetric);
            
            // Run the test
            // Set the initial context factory
            System.setProperty(InitialContext.INITIAL_CONTEXT_FACTORY, TestInitialContextFactory.class.getName());
            
            // Create and test the test subject
            ReflectionTestUtils.setField(subject, "queryCache", queryCache);
            ReflectionTestUtils.setField(subject, "queryMetricsBean", queryMetrics);
            subject.filter(requestContext, responseContext);
            subject.aroundWriteTo(writerContext);
        }
    }
    
    @Test
    public void testWrite_CreateAndNextQidResponse() throws Exception {
        try (MockedStatic<FindAnnotation> findAnnotationMock = Mockito.mockStatic(FindAnnotation.class)) {
            findAnnotationMock.when(() -> FindAnnotation.findAnnotation(isA(Annotation[].class), eq(EnrichQueryMetrics.class))).thenReturn(
                            this.enrichQueryMetrics);
            QueryMetricsEnrichmentInterceptor subject = new QueryMetricsEnrichmentInterceptor();
            
            // Simulate the initial context
            TestInitialContextFactory.INITIAL_CONTEXT = this.initialContext;
            
            final ArgumentCaptor<QueryCall> qcCapture;
            
            // Set expectations for the postProcess
            when(responseContext.getHeaders()).thenReturn(writeHeaders);
            when(writeHeaders.keySet()).thenReturn(new HashSet<>());
            when(responseContext.getStatus()).thenReturn(HttpResponseCodes.SC_OK);
            when(responseContext.getJaxrsResponse()).thenReturn(jaxrsResponse);
            requestContext.setProperty(eq((String) ReflectionTestUtils.getField(subject, "RESPONSE_STATS_NAME")), any());
            when(jaxrsResponse.getAnnotations()).thenReturn(new Annotation[] {enrichQueryMetrics});
            when(responseContext.getEntity()).thenReturn(baseQueryResponse);
            when(enrichQueryMetrics.methodType()).thenReturn(EnrichQueryMetrics.MethodType.CREATE_AND_NEXT);
            when(baseQueryResponse.getQueryId()).thenReturn(UUID.randomUUID().toString());
            // requestContext.setProperty(eq(QueryCall.class.getName()), capture(qcCapture));
            
            // Set expectations for the write
            when(writerContext.getOutputStream()).thenReturn(outputStream);
            writerContext.setOutputStream(isA(CountingOutputStream.class));
            writerContext.setOutputStream(outputStream);
            when(writerContext.getHeaders()).thenReturn(writeHeaders);
            when(writeHeaders.entrySet()).thenReturn(new HashSet<>());
            writerContext.proceed();
            // when(writerContext.getProperty(QueryCall.class.getName())).thenAnswer((Answer<QueryCall>) qcCapture::getValue);
            when(writerContext.getProperty(eq((String) ReflectionTestUtils.getField(subject, "RESPONSE_STATS_NAME")))).thenReturn(null);
            when(writerContext.getProperty(eq((String) ReflectionTestUtils.getField(subject, "REQUEST_STATS_NAME")))).thenReturn(null);
            queryMetric.setCreateCallTime(eq(-1L));
            queryMetric.setLoginTime(-1L);
            pageTime.setCallTime(-1L);
            pageTime.setLoginTime(-1L);
            pageTime.setSerializationTime(geq(0L));
            pageTime.setBytesWritten(0L);
            queryMetrics.updateMetric(queryMetric);
            
            // Run the test
            // Set the initial context factory
            System.setProperty(InitialContext.INITIAL_CONTEXT_FACTORY, TestInitialContextFactory.class.getName());
            
            // Create and test the test subject
            ReflectionTestUtils.setField(subject, "queryCache", queryCache);
            ReflectionTestUtils.setField(subject, "queryMetricsBean", queryMetrics);
            subject.filter(requestContext, responseContext);
            subject.aroundWriteTo(writerContext);
        }
    }
    
    @Test
    public void testWrite_NextQidResponse() throws Exception {
        try (MockedStatic<FindAnnotation> findAnnotationMock = Mockito.mockStatic(FindAnnotation.class)) {
            findAnnotationMock.when(() -> FindAnnotation.findAnnotation(isA(Annotation[].class), eq(EnrichQueryMetrics.class))).thenReturn(
                            this.enrichQueryMetrics);
            QueryMetricsEnrichmentInterceptor subject = new QueryMetricsEnrichmentInterceptor();
            
            // Simulate the initial context
            TestInitialContextFactory.INITIAL_CONTEXT = this.initialContext;
            
            // final Capture<QueryCall> qcCapture = Capture.newInstance();
            
            // Set expectations for the postProcess
            when(responseContext.getHeaders()).thenReturn(writeHeaders);
            when(writeHeaders.keySet()).thenReturn(new HashSet<>());
            when(responseContext.getStatus()).thenReturn(HttpResponseCodes.SC_OK);
            when(responseContext.getJaxrsResponse()).thenReturn(jaxrsResponse);
            requestContext.setProperty(eq((String) ReflectionTestUtils.getField(subject, "RESPONSE_STATS_NAME")), any());
            when(jaxrsResponse.getAnnotations()).thenReturn(new Annotation[] {enrichQueryMetrics});
            when(responseContext.getEntity()).thenReturn(baseQueryResponse);
            when(enrichQueryMetrics.methodType()).thenReturn(EnrichQueryMetrics.MethodType.NEXT);
            when(baseQueryResponse.getQueryId()).thenReturn(UUID.randomUUID().toString());
            // requestContext.setProperty(eq(QueryCall.class.getName()), capture(qcCapture));
            
            // Set expectations for the write
            when(writerContext.getOutputStream()).thenReturn(outputStream);
            writerContext.setOutputStream(isA(CountingOutputStream.class));
            writerContext.setOutputStream(outputStream);
            when(writerContext.getHeaders()).thenReturn(writeHeaders);
            when(writeHeaders.entrySet()).thenReturn(new HashSet<>());
            writerContext.proceed();
            when(writerContext.getProperty(eq((String) ReflectionTestUtils.getField(subject, "RESPONSE_STATS_NAME")))).thenReturn(null);
            when(writerContext.getProperty(eq((String) ReflectionTestUtils.getField(subject, "REQUEST_STATS_NAME")))).thenReturn(null);
            pageTime.setCallTime(-1L);
            pageTime.setLoginTime(-1L);
            pageTime.setSerializationTime(geq(0L));
            pageTime.setBytesWritten(0L);
            queryMetrics.updateMetric(queryMetric);
            
            // Run the test
            
            // Set the initial context factory
            System.setProperty(InitialContext.INITIAL_CONTEXT_FACTORY, TestInitialContextFactory.class.getName());
            
            // Create and test the test subject
            ReflectionTestUtils.setField(subject, "queryCache", queryCache);
            ReflectionTestUtils.setField(subject, "queryMetricsBean", queryMetrics);
            subject.filter(requestContext, responseContext);
            subject.aroundWriteTo(writerContext);
        }
    }
    
    public static class TestInitialContextFactory implements InitialContextFactory {
        static InitialContext INITIAL_CONTEXT;
        
        @Override
        public Context getInitialContext(Hashtable<?,?> arg0) throws NamingException {
            return INITIAL_CONTEXT;
        }
    }
}
