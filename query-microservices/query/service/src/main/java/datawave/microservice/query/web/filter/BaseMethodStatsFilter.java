package datawave.microservice.query.web.filter;

import org.springframework.http.MediaType;
import org.springframework.lang.NonNull;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletResponse;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static datawave.microservice.config.web.Constants.REQUEST_LOGIN_TIME_ATTRIBUTE;
import static datawave.microservice.config.web.Constants.REQUEST_START_TIME_NS_ATTRIBUTE;

public abstract class BaseMethodStatsFilter extends OncePerRequestFilter {
    
    private static final String START_NS_ATTRIBUTE = "STATS_START_NS";
    private static final String STOP_NS_ATTRIBUTE = "STATS_STOP_NS";
    
    protected static class RequestMethodStats {
        private String uri;
        private String method;
        private long loginTime = -1;
        private long callStartTime;
        private MultiValueMap<String,String> requestHeaders = new LinkedMultiValueMap<>();
        private MultiValueMap<String,String> formParameters = new LinkedMultiValueMap<>();
        
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
        
        public MultiValueMap<String,String> getRequestHeaders() {
            return requestHeaders;
        }
        
        public MultiValueMap<String,String> getFormParameters() {
            return formParameters;
        }
    }
    
    protected static class ResponseMethodStats {
        private int statusCode = -1;
        private long loginTime = -1;
        private long callTime = -1;
        private long serializationTime = -1;
        private long bytesWritten = -1;
        private MultiValueMap<String,Object> responseHeaders = new LinkedMultiValueMap<>();
        
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
        
        public MultiValueMap<String,Object> getResponseHeaders() {
            return responseHeaders;
        }
        
    }
    
    @Override
    public void doFilterInternal(@NonNull HttpServletRequest request, @NonNull HttpServletResponse response, @NonNull FilterChain chain)
                    throws IOException, ServletException {
        
        preProcess(request, response);
        
        if (!(response instanceof CountingHttpServletResponseWrapper)) {
            response = new CountingHttpServletResponseWrapper(response);
        }
        
        chain.doFilter(request, response);
        postProcess(request, response);
    }
    
    public void preProcess(HttpServletRequest request, HttpServletResponse response) {
        if (BaseMethodStatsContext.getRequestStats() == null) {
            BaseMethodStatsContext.setRequestStats(createRequestMethodStats(request, response));
            
            long start = System.nanoTime();
            request.setAttribute(START_NS_ATTRIBUTE, start);
        }
        preProcess(BaseMethodStatsContext.getRequestStats());
    }
    
    public void preProcess(RequestMethodStats requestStats) {
        // do nothing
    }
    
    public void postProcess(HttpServletRequest request, HttpServletResponse response) {
        if (BaseMethodStatsContext.getResponseStats() == null) {
            long stop = System.nanoTime();
            request.setAttribute(STOP_NS_ATTRIBUTE, stop);
            
            BaseMethodStatsContext.setResponseStats(createResponseMethodStats(request, response));
        }
        postProcess(BaseMethodStatsContext.getResponseStats());
    }
    
    public void postProcess(ResponseMethodStats responseStats) {
        // do nothing
    }
    
    protected RequestMethodStats createRequestMethodStats(HttpServletRequest request, HttpServletResponse response) {
        RequestMethodStats requestStats = new RequestMethodStats();
        
        requestStats.uri = request.getRequestURI();
        requestStats.method = request.getMethod();
        
        requestStats.callStartTime = System.nanoTime();
        try {
            requestStats.callStartTime = (long) request.getAttribute(REQUEST_START_TIME_NS_ATTRIBUTE);
        } catch (Exception e) {
            // do nothing
        }
        try {
            requestStats.loginTime = (long) request.getAttribute(REQUEST_LOGIN_TIME_ATTRIBUTE);
        } catch (Exception e) {
            // do nothing
        }
        
        for (Enumeration<String> headerNames = request.getHeaderNames(); headerNames.hasMoreElements();) {
            String header = headerNames.nextElement();
            for (Enumeration<String> headerValues = request.getHeaders(header); headerValues.hasMoreElements();) {
                requestStats.requestHeaders.add(header, headerValues.nextElement());
            }
        }
        
        if (request.getContentType() != null && MediaType.parseMediaType(request.getContentType()).isCompatibleWith(MediaType.APPLICATION_FORM_URLENCODED)) {
            Map<String,String[]> formParameters = request.getParameterMap();
            if (formParameters != null) {
                formParameters.forEach((k, v) -> requestStats.formParameters.addAll(k, Arrays.asList(v)));
            }
        }
        
        return requestStats;
    }
    
    private ResponseMethodStats createResponseMethodStats(HttpServletRequest request, HttpServletResponse response) {
        ResponseMethodStats responseStats = new ResponseMethodStats();
        
        long start = System.nanoTime();
        try {
            start = (long) request.getAttribute(START_NS_ATTRIBUTE);
        } catch (Exception e) {
            // do nothing
        }
        
        long stop = System.nanoTime();
        try {
            stop = (long) request.getAttribute(STOP_NS_ATTRIBUTE);
        } catch (Exception e) {
            // do nothing
        }
        
        responseStats.serializationTime = TimeUnit.NANOSECONDS.toMillis(stop - start);
        responseStats.loginTime = BaseMethodStatsContext.getRequestStats().getLoginTime();
        responseStats.callTime = TimeUnit.NANOSECONDS.toMillis(stop - BaseMethodStatsContext.getRequestStats().getCallStartTime());
        
        if (response instanceof CountingHttpServletResponseWrapper) {
            responseStats.bytesWritten = ((CountingHttpServletResponseWrapper) response).getByteCount();
        }
        
        for (String header : response.getHeaderNames()) {
            responseStats.responseHeaders.add(header, response.getHeaders(header));
        }
        
        return responseStats;
    }
    
    @Override
    public void destroy() {
        BaseMethodStatsContext.remove();
    }
    
    private static class CountingHttpServletResponseWrapper extends HttpServletResponseWrapper {
        private final ServletResponse response;
        private CountingServletOutputStream cos;
        
        /**
         * Creates a ServletResponse adaptor wrapping the given response object.
         *
         * @param response
         *            the {@link ServletResponse} to be wrapped
         * @throws IllegalArgumentException
         *             if the response is null.
         */
        public CountingHttpServletResponseWrapper(HttpServletResponse response) {
            super(response);
            this.response = response;
        }
        
        @Override
        public ServletOutputStream getOutputStream() throws IOException {
            if (cos == null) {
                cos = new CountingServletOutputStream(response.getOutputStream());
            }
            return cos;
        }
        
        public long getByteCount() {
            return cos.getByteCount();
        }
    }
    
    private static class CountingServletOutputStream extends ServletOutputStream {
        private final ServletOutputStream outputStream;
        private long count = 0;
        
        public CountingServletOutputStream(ServletOutputStream outputStream) {
            this.outputStream = outputStream;
        }
        
        @Override
        public boolean isReady() {
            return outputStream.isReady();
        }
        
        @Override
        public void setWriteListener(WriteListener writeListener) {
            outputStream.setWriteListener(writeListener);
        }
        
        @Override
        public void write(int b) throws IOException {
            outputStream.write(b);
            count++;
        }
        
        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            outputStream.write(b, off, len);
            this.count += (long) len;
        }
        
        public long getByteCount() {
            return count;
        }
    }
    
    public static class BaseMethodStatsContext {
        
        private static final ThreadLocal<RequestMethodStats> requestStats = new ThreadLocal<>();
        private static final ThreadLocal<ResponseMethodStats> responseStats = new ThreadLocal<>();
        
        public static RequestMethodStats getRequestStats() {
            return requestStats.get();
        }
        
        public static void setRequestStats(RequestMethodStats requestStats) {
            BaseMethodStatsContext.requestStats.set(requestStats);
        }
        
        public static ResponseMethodStats getResponseStats() {
            return responseStats.get();
        }
        
        public static void setResponseStats(ResponseMethodStats responseStats) {
            BaseMethodStatsContext.responseStats.set(responseStats);
        }
        
        private static void remove() {
            requestStats.remove();
            responseStats.remove();
        }
    }
}
