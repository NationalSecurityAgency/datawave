package datawave.resteasy.interceptor;

import static datawave.webservice.metrics.Constants.REQUEST_LOGIN_TIME_HEADER;
import static datawave.webservice.metrics.Constants.REQUEST_START_TIME_HEADER;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.WriterInterceptor;
import javax.ws.rs.ext.WriterInterceptorContext;

import org.apache.log4j.Logger;
import org.jboss.resteasy.core.interception.PreMatchContainerRequestContext;
import org.jboss.resteasy.specimpl.MultivaluedTreeMap;
import org.jboss.resteasy.spi.Failure;
import org.jboss.resteasy.util.CaseInsensitiveMap;

import com.google.common.io.CountingOutputStream;

public abstract class BaseMethodStatsInterceptor implements ContainerRequestFilter, ContainerResponseFilter, WriterInterceptor {
    protected static final Logger log = Logger.getLogger(BaseMethodStatsInterceptor.class);

    protected static class RequestMethodStats {

        private String uri;
        private String method;
        private long loginTime = -1;
        private long callStartTime;
        private MultivaluedMap<String,String> requestHeaders = new CaseInsensitiveMap<>();
        private MultivaluedMap<String,String> formParameters = new CaseInsensitiveMap<>();

        public String getUri() {
            return uri;
        }

        public String getMethod() {
            return method;
        }

        public long getLoginTime() {
            return loginTime;
        }

        public long getCallStartTime() {
            return callStartTime;
        }

        public MultivaluedMap<String,String> getRequestHeaders() {
            return requestHeaders;
        }

        public MultivaluedMap<String,String> getFormParameters() {
            return formParameters;
        }
    }

    protected static class ResponseMethodStats {
        private int statusCode = -1;
        private long loginTime = -1;
        private long callTime = -1;
        private long serializationTime = -1;
        private long bytesWritten = -1;
        private MultivaluedMap<String,Object> responseHeaders = new CaseInsensitiveMap<>();

        public int getStatusCode() {
            return statusCode;
        }

        public long getLoginTime() {
            return loginTime;
        }

        public long getCallTime() {
            return callTime;
        }

        public long getSerializationTime() {
            return serializationTime;
        }

        public long getBytesWritten() {
            return bytesWritten;
        }

        public MultivaluedMap<String,Object> getResponseHeaders() {
            return responseHeaders;
        }

    }

    @Context
    protected HttpHeaders httpHeaders;

    private final String REQUEST_STATS_NAME = getClass().getName() + "." + RequestMethodStats.class.getName();
    private final String RESPONSE_STATS_NAME = getClass().getName() + "." + ResponseMethodStats.class.getName();

    protected ResponseMethodStats doWrite(WriterInterceptorContext context) throws IOException, WebApplicationException {
        ResponseMethodStats stats;
        long start = System.nanoTime();
        OutputStream originalOutputStream = context.getOutputStream();
        CountingOutputStream countingStream = new CountingOutputStream(originalOutputStream);
        context.setOutputStream(countingStream);
        try {
            context.proceed();
        } finally {
            long stop = System.nanoTime();
            long time = TimeUnit.NANOSECONDS.toMillis(stop - start);

            context.setOutputStream(originalOutputStream);

            stats = (ResponseMethodStats) context.getProperty(RESPONSE_STATS_NAME);
            if (stats == null) {
                log.warn("No response stats found for " + getClass() + ". Using default.");
                stats = new ResponseMethodStats();
            }

            RequestMethodStats requestStats = (RequestMethodStats) context.getProperty(REQUEST_STATS_NAME);
            if (requestStats == null) {
                log.warn("No request method stats found for " + getClass() + ". Using default.");
                requestStats = new RequestMethodStats();
                requestStats.callStartTime = stop + TimeUnit.MILLISECONDS.toNanos(1);
            }

            stats.serializationTime = time;
            stats.loginTime = requestStats.getLoginTime();
            stats.callTime = TimeUnit.NANOSECONDS.toMillis(stop - requestStats.getCallStartTime());
            stats.bytesWritten = countingStream.getCount();
            // Merge in the headers we saved in the postProcess call, if any.
            putNew(stats.responseHeaders, context.getHeaders());
        }

        return stats;
    }

    // Response filter interceptor
    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext response) throws IOException {
        // Copy the headers because they get committed before the message body writer context is
        // called, and when they are committed, the contents of the map is modified.
        ResponseMethodStats stats = new ResponseMethodStats();
        MultivaluedTreeMap.addAll(response.getHeaders(), stats.responseHeaders);
        stats.statusCode = response.getStatus();
        request.setProperty(RESPONSE_STATS_NAME, stats);
    }

    protected RequestMethodStats doPreProcess(PreMatchContainerRequestContext request) throws Failure, WebApplicationException {
        RequestMethodStats stats = new RequestMethodStats();
        stats.uri = request.getUriInfo().getRequestUri().toString();
        stats.method = request.getMethod();
        stats.callStartTime = System.nanoTime();
        try {
            stats.callStartTime = Long.parseLong(httpHeaders.getHeaderString(REQUEST_START_TIME_HEADER));
        } catch (Exception e) {
            // ignore -- we just won't set call start time properly
        }
        try {
            stats.loginTime = Long.parseLong(httpHeaders.getHeaderString(REQUEST_LOGIN_TIME_HEADER));
        } catch (Exception e) {
            // ignore -- we just won't set login time.
        }
        MultivaluedTreeMap.addAll(request.getHeaders(), stats.requestHeaders);
        MediaType mediaType = request.getMediaType();
        if (mediaType != null && mediaType.isCompatible(MediaType.APPLICATION_FORM_URLENCODED_TYPE)) {
            MultivaluedMap<String,String> formParameters = request.getHttpRequest().getDecodedFormParameters();
            if (formParameters != null)
                MultivaluedTreeMap.addAll(formParameters, stats.formParameters);
        }
        request.setProperty(REQUEST_STATS_NAME, stats);
        return stats;
    }

    /**
     * Puts all entries in {@code newMap} into {@code existingMap}, unless there is already an existing entry in {@code existingMap}.
     *
     * @param newMap
     *            - a newmap
     * @param existingMap
     *            - existingmap
     * @param <K>
     *            - key type
     * @param <V>
     *            - value type
     */
    private static <K,V> void putNew(MultivaluedMap<K,V> existingMap, MultivaluedMap<K,V> newMap) {
        for (Entry<K,List<V>> entry : newMap.entrySet()) {
            if (!existingMap.containsKey(entry.getKey()))
                existingMap.put(entry.getKey(), entry.getValue());
        }
    }
}
