package datawave.microservice.query.web;

import datawave.Constants;
import datawave.microservice.query.web.annotation.GenerateQuerySessionId;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpResponse;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;

import javax.servlet.http.Cookie;
import java.util.Arrays;
import java.util.UUID;

// TODO: Update to enable based on properties
// TODO: Should this be in the API or in a starter?
@ControllerAdvice
public class QuerySessionIdAdvice implements ResponseBodyAdvice<Object> {
    private final Logger log = Logger.getLogger(QuerySessionIdAdvice.class);
    
    @Override
    public boolean supports(@NonNull MethodParameter returnType, @NonNull Class<? extends HttpMessageConverter<?>> converterType) {
        return Arrays.stream(returnType.getMethodAnnotations()).anyMatch(GenerateQuerySessionId.class::isInstance);
    }
    
    @Override
    public Object beforeBodyWrite(Object body, @NonNull MethodParameter returnType, @NonNull MediaType selectedContentType,
                    @NonNull Class<? extends HttpMessageConverter<?>> selectedConverterType, @NonNull ServerHttpRequest request,
                    @NonNull ServerHttpResponse response) {
        try {
            GenerateQuerySessionId annotation = (GenerateQuerySessionId) Arrays.stream(returnType.getMethodAnnotations())
                            .filter(GenerateQuerySessionId.class::isInstance).findAny().orElse(null);
            
            if (annotation != null) {
                ServletServerHttpResponse httpServletResponse = (ServletServerHttpResponse) response;
                String path = annotation.cookieBasePath();
                
                String id = "";
                boolean setCookie = true;
                switch (HttpStatus.Series.valueOf(httpServletResponse.getServletResponse().getStatus())) {
                    case SERVER_ERROR:
                    case CLIENT_ERROR:
                        // If we're sending an error response, then there's no need to set a cookie since
                        // there's no query "session" to stick to this server.
                        setCookie = false;
                        break;
                    
                    default:
                        if (StringUtils.isEmpty(QuerySessionIdContext.getQueryId())) {
                            log.error("queryId was not set.");
                        } else {
                            id = QuerySessionIdContext.getQueryId();
                        }
                        break;
                }
                
                if (setCookie) {
                    Cookie cookie = new Cookie(Constants.QUERY_COOKIE_NAME, generateCookieValue());
                    cookie.setPath(path + id);
                    httpServletResponse.getServletResponse().addCookie(cookie);
                }
            }
            
            return body;
        } finally {
            QuerySessionIdContext.removeQueryId();
        }
    }
    
    private static String generateCookieValue() {
        return Integer.toString(UUID.randomUUID().hashCode() & Integer.MAX_VALUE);
    }
    
    public static class QuerySessionIdContext {
        
        private static final ThreadLocal<String> queryId = new ThreadLocal<>();
        
        public static String getQueryId() {
            return (String) queryId.get();
        }
        
        public static void setQueryId(String queryId) {
            QuerySessionIdContext.queryId.set(queryId);
        }
        
        private static void removeQueryId() {
            queryId.remove();
        }
    }
}
