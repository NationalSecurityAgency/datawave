package datawave.microservice.config.web.filter;

import static datawave.microservice.config.web.Constants.OPERATION_TIME_MS_HEADER;
import static datawave.microservice.config.web.Constants.REQUEST_START_TIME_NS_ATTRIBUTE;
import static datawave.microservice.config.web.Constants.RESPONSE_ORIGIN_HEADER;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.lang.NonNull;
import org.springframework.security.web.util.OnCommittedResponseWrapper;
import org.springframework.web.filter.OncePerRequestFilter;

public class ResponseHeaderServletFilter extends OncePerRequestFilter {
    private final String origin;
    
    public ResponseHeaderServletFilter(String systemName) {
        String origin;
        try {
            origin = systemName + " / " + InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            origin = systemName + " / UNKNOWN";
        }
        this.origin = origin;
    }
    
    @Override
    protected void doFilterInternal(@NonNull HttpServletRequest request, @NonNull HttpServletResponse response, @NonNull FilterChain filterChain)
                    throws ServletException, IOException {
        // The request start time should have been written in to the ServletRequest earlier.
        // However, if it wasn't, then do so here to guarantee we have at least some start time to calculate from.
        if (request.getAttribute(REQUEST_START_TIME_NS_ATTRIBUTE) == null) {
            request.setAttribute(REQUEST_START_TIME_NS_ATTRIBUTE, System.nanoTime());
        }
        
        HeaderWriterResponse headerWriterResponse = new HeaderWriterResponse(response, request, origin);
        try {
            filterChain.doFilter(request, headerWriterResponse);
        } finally {
            headerWriterResponse.writeHeaders();
        }
    }
    
    private static class HeaderWriterResponse extends OnCommittedResponseWrapper {
        private final HttpServletRequest request;
        private final String origin;
        
        private HeaderWriterResponse(HttpServletResponse response, HttpServletRequest request, String origin) {
            super(response);
            this.request = request;
            this.origin = origin;
        }
        
        @Override
        protected void onResponseCommitted() {
            writeHeaders();
            disableOnResponseCommitted();
        }
        
        protected void writeHeaders() {
            if (isDisableOnResponseCommitted()) {
                return;
            }
            
            getHttpResponse().setHeader(RESPONSE_ORIGIN_HEADER, origin);
            
            // Add the operation time if we didn't already do it when writing a response out. Even though this might be
            // a slightly more accurate operation time, we don't want it to disagree with the value that was written in
            // to the response.
            if (!getHttpResponse().containsHeader(OPERATION_TIME_MS_HEADER)) {
                long startTimeNanos = (long) request.getAttribute(REQUEST_START_TIME_NS_ATTRIBUTE);
                long operationTimeMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
                getHttpResponse().setHeader(OPERATION_TIME_MS_HEADER, Long.toString(operationTimeMillis));
            }
        }
        
        private HttpServletResponse getHttpResponse() {
            return (HttpServletResponse) getResponse();
        }
    }
}
