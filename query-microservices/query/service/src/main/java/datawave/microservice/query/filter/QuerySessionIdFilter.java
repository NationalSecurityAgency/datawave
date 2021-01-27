package datawave.microservice.query.filter;

import datawave.Constants;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;

@Component
public class QuerySessionIdFilter implements Filter {
    private final Logger log = Logger.getLogger(QuerySessionIdFilter.class);
    
    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        
        QuerySessionIdContext.servletRequest.set(servletRequest);
        
        filterChain.doFilter(servletRequest, servletResponse);
        
        String path = QuerySessionIdContext.getCookieBasePath();
        
        if (path != null) {
            HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
            
            String id = "";
            boolean setCookie = true;
            switch (HttpStatus.Series.valueOf(httpServletResponse.getStatus())) {
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
                httpServletResponse.addCookie(cookie);
            }
        }
    }
    
    private static String generateCookieValue() {
        return Integer.toString(UUID.randomUUID().hashCode() & Integer.MAX_VALUE);
    }
    
    @Override
    public void destroy() {
        QuerySessionIdContext.removeServletRequest();
    }
    
    public static class QuerySessionIdContext {
        private static final String COOKIE_BASE_PATH = "cookieBasePath";
        private static final String QUERY_ID = "queryId";
        
        private static final ThreadLocal<ServletRequest> servletRequest = new ThreadLocal<>();
        
        public static String getQueryId() {
            return (String) servletRequest.get().getAttribute(QUERY_ID);
        }
        
        public static void setQueryId(String queryId) {
            servletRequest.get().setAttribute(QUERY_ID, queryId);
        }
        
        public static String getCookieBasePath() {
            return (String) servletRequest.get().getAttribute(COOKIE_BASE_PATH);
        }
        
        public static void setCookieBasePath(String cookieBasePath) {
            servletRequest.get().setAttribute(COOKIE_BASE_PATH, cookieBasePath);
        }
        
        private static void removeServletRequest() {
            servletRequest.remove();
        }
    }
}
