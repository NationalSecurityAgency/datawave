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
public class GenerateQuerySessionIdFilter implements Filter {
    private final Logger log = Logger.getLogger(GenerateQuerySessionIdFilter.class);
    public static final ThreadLocal<String> QUERY_ID = new ThreadLocal<>();
    
    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        
        // before the request is serviced by the rest controller
        filterChain.doFilter(servletRequest, servletResponse);
        
        // after the request is serviced by the rest controller
        if (servletResponse instanceof HttpServletResponse) {
            HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
            
            String path = "TODO";
            String id = "";
            String cookieValue = generateCookieValue();
            boolean setCookie = true;
            switch (HttpStatus.Series.valueOf(httpServletResponse.getStatus())) {
                case SERVER_ERROR:
                case CLIENT_ERROR:
                    // If we're sending an error response, then there's no need to set a cookie since
                    // there's no query "session" to stick to this server.
                    setCookie = false;
                    QUERY_ID.set(null);
                    break;
                
                default:
                    if (StringUtils.isEmpty(QUERY_ID.get())) {
                        log.error("threadlocal QUERY_ID was not set.");
                    } else {
                        id = QUERY_ID.get();
                        QUERY_ID.set(null);
                    }
                    break;
            }
            
            if (setCookie) {
                Cookie cookie = new Cookie(Constants.QUERY_COOKIE_NAME, cookieValue);
                cookie.setPath(path + id);
                httpServletResponse.addCookie(cookie);
            }
        }
    }
    
    public static String generateCookieValue() {
        return Integer.toString(UUID.randomUUID().hashCode() & Integer.MAX_VALUE);
    }
}
