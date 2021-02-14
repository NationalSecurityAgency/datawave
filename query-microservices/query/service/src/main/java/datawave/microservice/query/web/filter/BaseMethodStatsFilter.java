package datawave.microservice.query.web.filter;

import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
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
import java.util.Enumeration;

import static datawave.microservice.config.web.Constants.REQUEST_LOGIN_TIME_ATTRIBUTE;
import static datawave.microservice.config.web.Constants.REQUEST_START_TIME_NS_ATTRIBUTE;

@Component
public class BaseMethodStatsFilter extends OncePerRequestFilter {
    
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
    public void initFilterBean() throws ServletException {
        System.out.println("init");
    }
    
    @Override
    public void doFilterInternal(@NonNull HttpServletRequest request, @NonNull HttpServletResponse response, @NonNull FilterChain chain)
                    throws IOException, ServletException {
        RequestMethodStats stats = new RequestMethodStats();
        
        stats.uri = request.getRequestURI();
        stats.method = request.getMethod();
        
        stats.callStartTime = System.nanoTime();
        try {
            stats.callStartTime = (long) request.getAttribute(REQUEST_START_TIME_NS_ATTRIBUTE);
        } catch (Exception e) {
            // do nothing
        }
        try {
            stats.loginTime = (long) request.getAttribute(REQUEST_LOGIN_TIME_ATTRIBUTE);
        } catch (Exception e) {
            // do nothing
        }

        for (Enumeration<String> headerNames = request.getHeaderNames(); headerNames.hasMoreElements();) {
            String header = headerNames.nextElement();
            for (Enumeration<String> headerValues = request.getHeaders(header); headerValues.hasMoreElements();){
                stats.requestHeaders.add(header, headerValues.nextElement());
            }
        }

        // TODO: Finish this!
//         MediaType mediaType = request.getMediaType();
//         if (mediaType != null && mediaType.isCompatible(MediaType.APPLICATION_FORM_URLENCODED_TYPE)) {
//         MultivaluedMap<String,String> formParameters = request.getHttpRequest().getDecodedFormParameters();
//         if (formParameters != null)
//         MultivaluedTreeMap.addAll(formParameters, stats.formParameters);
//         }
        // request.setProperty(REQUEST_STATS_NAME, stats);
        // return stats;
        
        System.out.println("doFilter before");
        if (response instanceof HttpServletResponse) {
            chain.doFilter(request, new CountingHttpServletResponseWrapper((HttpServletResponse) response));
        } else {
            chain.doFilter(request, response);
        }
        System.out.println("doFilter after");
    }
    
    @Override
    public void destroy() {
        System.out.println("destroy");
    }
    
    private static class CountingHttpServletResponseWrapper extends HttpServletResponseWrapper {
        private ServletResponse response;
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
}
