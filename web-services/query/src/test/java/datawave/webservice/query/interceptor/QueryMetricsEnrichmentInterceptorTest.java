package datawave.webservice.query.interceptor;

import static org.mockito.AdditionalMatchers.geq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.WriterInterceptorContext;

import org.jboss.resteasy.core.interception.jaxrs.ContainerResponseContextImpl;
import org.jboss.resteasy.core.interception.jaxrs.PreMatchContainerRequestContext;
import org.jboss.resteasy.specimpl.BuiltResponse;
import org.jboss.resteasy.specimpl.ResteasyUriInfo;
import org.jboss.resteasy.spi.HttpRequest;
import org.jboss.resteasy.spi.HttpResponseCodes;
import org.jboss.resteasy.spi.util.FindAnnotation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import datawave.core.query.logic.BaseQueryLogic;
import datawave.microservice.querymetric.BaseQueryMetric.PageMetric;
import datawave.microservice.querymetric.QueryMetric;
import datawave.security.util.DnProperties;
import datawave.webservice.query.annotation.EnrichQueryMetrics;
import datawave.webservice.query.cache.QueryCache;
import datawave.webservice.query.interceptor.QueryMetricsEnrichmentInterceptor.QueryCall;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.result.BaseQueryResponse;

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
        System.setProperty(DnProperties.NPE_OU_PROPERTY, "iamnotaperson");
        System.setProperty("dw.metadatahelper.all.auths", "A,B,C,D");
    }

    @Test
    public void testPreProcess_HappyPath() throws Exception {
        QueryMetricsEnrichmentInterceptor subject = new QueryMetricsEnrichmentInterceptor();
        URI requestUri = new URI("http://localhost/test");
        String requestStatsName = (String) ReflectionTestUtils.getField(subject, "REQUEST_STATS_NAME");

        when(requestContext.getUriInfo()).thenReturn(uriInfo);
        when(uriInfo.getRequestUri()).thenReturn(requestUri);
        when(requestContext.getMethod()).thenReturn(null);
        when(requestContext.getHeaders()).thenReturn(requestHeaders);
        when(requestHeaders.keySet()).thenReturn(new HashSet<>());
        when(requestContext.getMediaType()).thenReturn(MediaType.APPLICATION_FORM_URLENCODED_TYPE);
        when(requestContext.getHttpRequest()).thenReturn(httpRequest);
        when(httpRequest.getDecodedFormParameters()).thenReturn(decodedFormParameters);
        when(decodedFormParameters.keySet()).thenReturn(new HashSet<>());

        subject.filter(requestContext);

        verify(requestContext).setProperty(eq(requestStatsName), any());
    }

    @Test
    public void testPostProcess_BaseQueryResponse() throws Exception {
        QueryMetricsEnrichmentInterceptor subject = new QueryMetricsEnrichmentInterceptor();
        String responseStatsName = (String) ReflectionTestUtils.getField(subject, "RESPONSE_STATS_NAME");

        when(responseContext.getHeaders()).thenReturn(writeHeaders);
        when(writeHeaders.keySet()).thenReturn(new HashSet<>());
        when(responseContext.getStatus()).thenReturn(HttpResponseCodes.SC_OK);
        when(responseContext.getJaxrsResponse()).thenReturn(jaxrsResponse);
        when(jaxrsResponse.getAnnotations()).thenReturn(new Annotation[] {enrichQueryMetrics});
        when(responseContext.getEntity()).thenReturn(baseQueryResponse);
        when(enrichQueryMetrics.methodType()).thenReturn(EnrichQueryMetrics.MethodType.CREATE);
        when(baseQueryResponse.getQueryId()).thenReturn(UUID.randomUUID().toString());

        try (MockedStatic<FindAnnotation> findAnnotationMock = mockStatic(FindAnnotation.class)) {
            findAnnotationMock.when(() -> FindAnnotation.findAnnotation(any(Annotation[].class), eq(EnrichQueryMetrics.class))).thenReturn(enrichQueryMetrics);

            subject.filter(requestContext, responseContext);
        }

        verify(requestContext).setProperty(eq(responseStatsName), any());
        verify(requestContext).setProperty(eq(QueryCall.class.getName()), isA(QueryCall.class));
    }

    @Test
    public void testWrite_UncheckedException() throws Exception {
        QueryMetricsEnrichmentInterceptor subject = new QueryMetricsEnrichmentInterceptor();
        TestInitialContextFactory.INITIAL_CONTEXT = this.initialContext;

        String responseStatsName = (String) ReflectionTestUtils.getField(subject, "RESPONSE_STATS_NAME");
        String requestStatsName = (String) ReflectionTestUtils.getField(subject, "REQUEST_STATS_NAME");

        AtomicReference<QueryCall> capturedQueryCall = new AtomicReference<>();
        lenient().doAnswer(inv -> {
            capturedQueryCall.set((QueryCall) inv.getArgument(1));
            return null;
        }).when(requestContext).setProperty(eq(QueryCall.class.getName()), any());

        when(responseContext.getHeaders()).thenReturn(writeHeaders);
        when(responseContext.getStatus()).thenReturn(HttpResponseCodes.SC_OK);
        when(responseContext.getJaxrsResponse()).thenReturn(jaxrsResponse);
        when(jaxrsResponse.getAnnotations()).thenReturn(new Annotation[] {enrichQueryMetrics});
        when(responseContext.getEntity()).thenReturn(baseQueryResponse);
        when(enrichQueryMetrics.methodType()).thenReturn(EnrichQueryMetrics.MethodType.CREATE);
        when(baseQueryResponse.getQueryId()).thenReturn(UUID.randomUUID().toString());

        when(writerContext.getOutputStream()).thenReturn(outputStream);
        when(writerContext.getHeaders()).thenReturn(writeHeaders);
        when(writerContext.getProperty(eq(responseStatsName))).thenReturn(null);
        when(writerContext.getProperty(eq(requestStatsName))).thenReturn(null);
        when(writerContext.getProperty(QueryCall.class.getName())).thenAnswer(inv -> capturedQueryCall.get());
        when(queryCache.get(any(String.class))).thenReturn(runningQuery);
        when(runningQuery.getLogic()).thenReturn(queryLogic);
        when(queryLogic.getCollectQueryMetrics()).thenReturn(true);
        when(runningQuery.getMetric()).thenThrow(new IllegalStateException("INTENTIONALLY THROWN UNCHECKED TEST EXCEPTION"));

        try (MockedStatic<FindAnnotation> findAnnotationMock = mockStatic(FindAnnotation.class)) {
            findAnnotationMock.when(() -> FindAnnotation.findAnnotation(any(Annotation[].class), eq(EnrichQueryMetrics.class))).thenReturn(enrichQueryMetrics);

            System.setProperty(InitialContext.INITIAL_CONTEXT_FACTORY, TestInitialContextFactory.class.getName());
            try {
                ReflectionTestUtils.setField(subject, "queryCache", queryCache);
                ReflectionTestUtils.setField(subject, "queryMetricsBean", queryMetrics);
                subject.filter(requestContext, responseContext);
                subject.aroundWriteTo(writerContext);
            } finally {
                System.clearProperty(InitialContext.INITIAL_CONTEXT_FACTORY);
            }
        }
    }

    @Test
    public void testWrite_CreateQidResponse() throws Exception {
        QueryMetricsEnrichmentInterceptor subject = new QueryMetricsEnrichmentInterceptor();
        TestInitialContextFactory.INITIAL_CONTEXT = this.initialContext;

        String responseStatsName = (String) ReflectionTestUtils.getField(subject, "RESPONSE_STATS_NAME");
        String requestStatsName = (String) ReflectionTestUtils.getField(subject, "REQUEST_STATS_NAME");

        AtomicReference<QueryCall> capturedQueryCall = new AtomicReference<>();
        lenient().doAnswer(inv -> {
            capturedQueryCall.set((QueryCall) inv.getArgument(1));
            return null;
        }).when(requestContext).setProperty(eq(QueryCall.class.getName()), any());

        when(responseContext.getHeaders()).thenReturn(writeHeaders);
        when(responseContext.getStatus()).thenReturn(HttpResponseCodes.SC_OK);
        when(responseContext.getJaxrsResponse()).thenReturn(jaxrsResponse);
        when(jaxrsResponse.getAnnotations()).thenReturn(new Annotation[] {enrichQueryMetrics});
        when(responseContext.getEntity()).thenReturn(baseQueryResponse);
        when(enrichQueryMetrics.methodType()).thenReturn(EnrichQueryMetrics.MethodType.CREATE);
        when(baseQueryResponse.getQueryId()).thenReturn(UUID.randomUUID().toString());

        when(writerContext.getOutputStream()).thenReturn(outputStream);
        when(writerContext.getHeaders()).thenReturn(writeHeaders);
        when(writerContext.getProperty(eq(responseStatsName))).thenReturn(null);
        when(writerContext.getProperty(eq(requestStatsName))).thenReturn(null);
        when(writerContext.getProperty(QueryCall.class.getName())).thenAnswer(inv -> capturedQueryCall.get());
        when(queryCache.get(any(String.class))).thenReturn(runningQuery);
        when(runningQuery.getLogic()).thenReturn(queryLogic);
        when(queryLogic.getCollectQueryMetrics()).thenReturn(true);
        when(runningQuery.getMetric()).thenReturn(queryMetric);

        try (MockedStatic<FindAnnotation> findAnnotationMock = mockStatic(FindAnnotation.class)) {
            findAnnotationMock.when(() -> FindAnnotation.findAnnotation(any(Annotation[].class), eq(EnrichQueryMetrics.class))).thenReturn(enrichQueryMetrics);

            System.setProperty(InitialContext.INITIAL_CONTEXT_FACTORY, TestInitialContextFactory.class.getName());
            try {
                ReflectionTestUtils.setField(subject, "queryCache", queryCache);
                ReflectionTestUtils.setField(subject, "queryMetricsBean", queryMetrics);
                subject.filter(requestContext, responseContext);
                subject.aroundWriteTo(writerContext);
            } finally {
                System.clearProperty(InitialContext.INITIAL_CONTEXT_FACTORY);
            }
        }

        verify(queryMetric).setCreateCallTime(-1L);
        verify(queryMetric).setLoginTime(-1L);
        verify(queryMetrics).updateMetric(queryMetric);
    }

    @Test
    public void testWrite_CreateAndNextQidResponse() throws Exception {
        QueryMetricsEnrichmentInterceptor subject = new QueryMetricsEnrichmentInterceptor();
        TestInitialContextFactory.INITIAL_CONTEXT = this.initialContext;

        String responseStatsName = (String) ReflectionTestUtils.getField(subject, "RESPONSE_STATS_NAME");
        String requestStatsName = (String) ReflectionTestUtils.getField(subject, "REQUEST_STATS_NAME");

        AtomicReference<QueryCall> capturedQueryCall = new AtomicReference<>();
        lenient().doAnswer(inv -> {
            capturedQueryCall.set((QueryCall) inv.getArgument(1));
            return null;
        }).when(requestContext).setProperty(eq(QueryCall.class.getName()), any());

        when(responseContext.getHeaders()).thenReturn(writeHeaders);
        when(responseContext.getStatus()).thenReturn(HttpResponseCodes.SC_OK);
        when(responseContext.getJaxrsResponse()).thenReturn(jaxrsResponse);
        when(jaxrsResponse.getAnnotations()).thenReturn(new Annotation[] {enrichQueryMetrics});
        when(responseContext.getEntity()).thenReturn(baseQueryResponse);
        when(enrichQueryMetrics.methodType()).thenReturn(EnrichQueryMetrics.MethodType.CREATE_AND_NEXT);
        when(baseQueryResponse.getQueryId()).thenReturn(UUID.randomUUID().toString());

        when(writerContext.getOutputStream()).thenReturn(outputStream);
        when(writerContext.getHeaders()).thenReturn(writeHeaders);
        when(writerContext.getProperty(eq(responseStatsName))).thenReturn(null);
        when(writerContext.getProperty(eq(requestStatsName))).thenReturn(null);
        when(writerContext.getProperty(QueryCall.class.getName())).thenAnswer(inv -> capturedQueryCall.get());
        when(queryCache.get(any(String.class))).thenReturn(runningQuery);
        when(runningQuery.getLogic()).thenReturn(queryLogic);
        when(queryLogic.getCollectQueryMetrics()).thenReturn(true);
        when(runningQuery.getMetric()).thenReturn(queryMetric);
        when(queryMetric.getPageTimes()).thenReturn(Arrays.asList(pageTime));

        try (MockedStatic<FindAnnotation> findAnnotationMock = mockStatic(FindAnnotation.class)) {
            findAnnotationMock.when(() -> FindAnnotation.findAnnotation(any(Annotation[].class), eq(EnrichQueryMetrics.class))).thenReturn(enrichQueryMetrics);

            System.setProperty(InitialContext.INITIAL_CONTEXT_FACTORY, TestInitialContextFactory.class.getName());
            try {
                ReflectionTestUtils.setField(subject, "queryCache", queryCache);
                ReflectionTestUtils.setField(subject, "queryMetricsBean", queryMetrics);
                subject.filter(requestContext, responseContext);
                subject.aroundWriteTo(writerContext);
            } finally {
                System.clearProperty(InitialContext.INITIAL_CONTEXT_FACTORY);
            }
        }

        verify(queryMetric).setCreateCallTime(-1L);
        verify(queryMetric).setLoginTime(-1L);
        verify(pageTime).setCallTime(-1L);
        verify(pageTime).setLoginTime(-1L);
        verify(pageTime).setSerializationTime(geq(0L));
        verify(pageTime).setBytesWritten(0L);
        verify(queryMetrics).updateMetric(queryMetric);
    }

    @Test
    public void testWrite_NextQidResponse() throws Exception {
        QueryMetricsEnrichmentInterceptor subject = new QueryMetricsEnrichmentInterceptor();
        TestInitialContextFactory.INITIAL_CONTEXT = this.initialContext;

        String responseStatsName = (String) ReflectionTestUtils.getField(subject, "RESPONSE_STATS_NAME");
        String requestStatsName = (String) ReflectionTestUtils.getField(subject, "REQUEST_STATS_NAME");

        AtomicReference<QueryCall> capturedQueryCall = new AtomicReference<>();
        lenient().doAnswer(inv -> {
            capturedQueryCall.set((QueryCall) inv.getArgument(1));
            return null;
        }).when(requestContext).setProperty(eq(QueryCall.class.getName()), any());

        when(responseContext.getHeaders()).thenReturn(writeHeaders);
        when(responseContext.getStatus()).thenReturn(HttpResponseCodes.SC_OK);
        when(responseContext.getJaxrsResponse()).thenReturn(jaxrsResponse);
        when(jaxrsResponse.getAnnotations()).thenReturn(new Annotation[] {enrichQueryMetrics});
        when(responseContext.getEntity()).thenReturn(baseQueryResponse);
        when(enrichQueryMetrics.methodType()).thenReturn(EnrichQueryMetrics.MethodType.NEXT);
        when(baseQueryResponse.getQueryId()).thenReturn(UUID.randomUUID().toString());

        when(writerContext.getOutputStream()).thenReturn(outputStream);
        when(writerContext.getHeaders()).thenReturn(writeHeaders);
        when(writerContext.getProperty(eq(responseStatsName))).thenReturn(null);
        when(writerContext.getProperty(eq(requestStatsName))).thenReturn(null);
        when(writerContext.getProperty(QueryCall.class.getName())).thenAnswer(inv -> capturedQueryCall.get());
        when(queryCache.get(any(String.class))).thenReturn(runningQuery);
        when(runningQuery.getLogic()).thenReturn(queryLogic);
        when(queryLogic.getCollectQueryMetrics()).thenReturn(true);
        when(runningQuery.getMetric()).thenReturn(queryMetric);
        when(queryMetric.getPageTimes()).thenReturn(Arrays.asList(pageTime));

        try (MockedStatic<FindAnnotation> findAnnotationMock = mockStatic(FindAnnotation.class)) {
            findAnnotationMock.when(() -> FindAnnotation.findAnnotation(any(Annotation[].class), eq(EnrichQueryMetrics.class))).thenReturn(enrichQueryMetrics);

            System.setProperty(InitialContext.INITIAL_CONTEXT_FACTORY, TestInitialContextFactory.class.getName());
            try {
                ReflectionTestUtils.setField(subject, "queryCache", queryCache);
                ReflectionTestUtils.setField(subject, "queryMetricsBean", queryMetrics);
                subject.filter(requestContext, responseContext);
                subject.aroundWriteTo(writerContext);
            } finally {
                System.clearProperty(InitialContext.INITIAL_CONTEXT_FACTORY);
            }
        }

        verify(pageTime).setCallTime(-1L);
        verify(pageTime).setLoginTime(-1L);
        verify(pageTime).setSerializationTime(geq(0L));
        verify(pageTime).setBytesWritten(0L);
        verify(queryMetrics).updateMetric(queryMetric);
    }

    public static class TestInitialContextFactory implements InitialContextFactory {
        static InitialContext INITIAL_CONTEXT;

        @Override
        public Context getInitialContext(Hashtable<?,?> arg0) throws NamingException {
            return INITIAL_CONTEXT;
        }
    }
}
