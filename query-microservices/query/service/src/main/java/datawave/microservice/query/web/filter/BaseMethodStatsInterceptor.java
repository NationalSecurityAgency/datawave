package datawave.microservice.query.web.filter;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Component
public class BaseMethodStatsInterceptor extends HandlerInterceptorAdapter {
    protected static final Logger log = Logger.getLogger(BaseMethodStatsInterceptor.class);
    
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
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        System.out.println("pre handle");
        return true;
    }
    
    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView) throws Exception {
        System.out.println("post handle");
    }
    
    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        System.out.println("after completion");
    }
}
