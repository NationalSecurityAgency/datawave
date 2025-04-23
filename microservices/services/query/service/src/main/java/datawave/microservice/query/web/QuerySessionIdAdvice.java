package datawave.microservice.query.web;

import java.util.Arrays;
import java.util.UUID;

import javax.servlet.http.Cookie;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpResponse;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.context.annotation.RequestScope;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;

import datawave.Constants;
import datawave.microservice.query.web.annotation.ClearQuerySessionId;
import datawave.microservice.query.web.annotation.GenerateQuerySessionId;
import datawave.microservice.query.web.filter.QueryMetricsEnrichmentFilterAdvice;

@ControllerAdvice
public class QuerySessionIdAdvice implements ResponseBodyAdvice<Object> {
    private final Logger log = Logger.getLogger(QuerySessionIdAdvice.class);
    
    // Note: QuerySessionIdContext needs to be request scoped
    private final QuerySessionIdContext querySessionIdContext;
    
    public QuerySessionIdAdvice(QuerySessionIdContext querySessionIdContext) {
        this.querySessionIdContext = querySessionIdContext;
    }
    
    @Override
    public boolean supports(@NonNull MethodParameter returnType, @NonNull Class<? extends HttpMessageConverter<?>> converterType) {
        return Arrays.stream(returnType.getMethodAnnotations())
                        .anyMatch(obj -> GenerateQuerySessionId.class.isInstance(obj) || ClearQuerySessionId.class.isInstance(obj));
    }
    
    @Override
    public Object beforeBodyWrite(Object body, @NonNull MethodParameter returnType, @NonNull MediaType selectedContentType,
                    @NonNull Class<? extends HttpMessageConverter<?>> selectedConverterType, @NonNull ServerHttpRequest request,
                    @NonNull ServerHttpResponse response) {
        GenerateQuerySessionId generateSessionIdAnnotation = (GenerateQuerySessionId) Arrays.stream(returnType.getMethodAnnotations())
                        .filter(GenerateQuerySessionId.class::isInstance).findAny().orElse(null);
        
        ClearQuerySessionId clearSessionIdAnnotation = (ClearQuerySessionId) Arrays.stream(returnType.getMethodAnnotations())
                        .filter(ClearQuerySessionId.class::isInstance).findAny().orElse(null);
        
        if (generateSessionIdAnnotation != null || clearSessionIdAnnotation != null) {
            ServletServerHttpResponse httpServletResponse = (ServletServerHttpResponse) response;
            
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
                    if (StringUtils.isEmpty(querySessionIdContext.getQueryId())) {
                        log.error("queryId was not set.");
                    } else {
                        id = querySessionIdContext.getQueryId();
                    }
                    break;
            }
            
            if (setCookie) {
                Cookie cookie = null;
                if (generateSessionIdAnnotation != null) {
                    String path = generateSessionIdAnnotation.cookieBasePath();
                    cookie = new Cookie(Constants.QUERY_COOKIE_NAME, generateCookieValue());
                    cookie.setPath(path + id);
                    cookie.setVersion(1);
                } else if (clearSessionIdAnnotation != null) {
                    cookie = new Cookie(Constants.QUERY_COOKIE_NAME, null);
                    cookie.setVersion(1);
                }
                if (cookie != null) {
                    httpServletResponse.getServletResponse().addCookie(cookie);
                }
            }
        }
        
        return body;
    }
    
    private static String generateCookieValue() {
        return Integer.toString(UUID.randomUUID().hashCode() & Integer.MAX_VALUE);
    }
    
    public static class QuerySessionIdContext {
        
        private String queryId;
        
        public String getQueryId() {
            return queryId;
        }
        
        public void setQueryId(String queryId) {
            this.queryId = queryId;
        }
    }
    
    @Configuration
    public static class QuerySessionIdAdviceConfig {
        @Bean
        @ConditionalOnMissingBean
        @RequestScope
        public QuerySessionIdAdvice.QuerySessionIdContext querySessionIdContext() {
            return new QuerySessionIdAdvice.QuerySessionIdContext();
        }
    }
}
