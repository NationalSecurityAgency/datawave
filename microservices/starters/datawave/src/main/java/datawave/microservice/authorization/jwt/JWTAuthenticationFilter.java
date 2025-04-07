package datawave.microservice.authorization.jwt;

import static datawave.microservice.config.web.Constants.REQUEST_LOGIN_TIME_ATTRIBUTE;
import static datawave.microservice.config.web.Constants.REQUEST_START_TIME_NS_ATTRIBUTE;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.http.HttpHeaders;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.InsufficientAuthenticationException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.WebAttributes;
import org.springframework.web.filter.GenericFilterBean;

/**
 * A security filter that expects to find an encoded JWT in the "Authorization" header in the request. The token is extracted from the header and passed along
 * (in a {@link JWTPreauthToken}) to the authorization manager.
 */
public class JWTAuthenticationFilter extends GenericFilterBean {
    private final boolean headerRequired;
    private final AuthenticationManager authenticationManager;
    private AuthenticationEntryPoint authenticationEntryPoint;
    
    public JWTAuthenticationFilter(AuthenticationManager authenticationManager, AuthenticationEntryPoint authenticationEntryPoint) {
        this(true, authenticationManager, authenticationEntryPoint);
    }
    
    public JWTAuthenticationFilter(boolean headerRequired, AuthenticationManager authenticationManager, AuthenticationEntryPoint authenticationEntryPoint) {
        this.headerRequired = headerRequired;
        this.authenticationManager = authenticationManager;
        this.authenticationEntryPoint = authenticationEntryPoint;
    }
    
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse res = (HttpServletResponse) response;
        
        try {
            // If there's an authorization exception left around from a prior filter first, abort.
            Object attribute = request.getAttribute(WebAttributes.AUTHENTICATION_EXCEPTION);
            if (attribute instanceof AuthenticationException) {
                throw (AuthenticationException) attribute;
            }
            
            String stringToken = req.getHeader(HttpHeaders.AUTHORIZATION);
            if (stringToken == null) {
                if (headerRequired) {
                    throw new InsufficientAuthenticationException("Authorization header is missing!");
                } else {
                    filterChain.doFilter(request, response);
                    return;
                }
            }
            
            String authorizationSchema = "Bearer";
            if (!stringToken.startsWith(authorizationSchema)) {
                throw new InsufficientAuthenticationException("Authorization schema (" + authorizationSchema + ") not present in supplied token.");
            }
            stringToken = stringToken.substring(authorizationSchema.length()).trim();
            
            JWTPreauthToken jwtToken = new JWTPreauthToken(stringToken);
            Authentication auth = authenticationManager.authenticate(jwtToken);
            SecurityContextHolder.getContext().setAuthentication(auth);
            
            setLoginTimeHeader(request);
            
            filterChain.doFilter(request, response);
        } catch (AuthenticationException e) {
            SecurityContextHolder.clearContext();
            request.setAttribute(WebAttributes.AUTHENTICATION_EXCEPTION, e);
            if (authenticationEntryPoint != null) {
                authenticationEntryPoint.commence(req, res, e);
            }
        }
    }
    
    private void setLoginTimeHeader(ServletRequest request) {
        if (request.getAttribute(REQUEST_START_TIME_NS_ATTRIBUTE) != null) {
            long loginTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - (long) request.getAttribute(REQUEST_START_TIME_NS_ATTRIBUTE));
            request.setAttribute(REQUEST_LOGIN_TIME_ATTRIBUTE, String.valueOf(loginTime));
        }
    }
}
