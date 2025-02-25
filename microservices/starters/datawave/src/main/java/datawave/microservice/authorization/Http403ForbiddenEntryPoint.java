package datawave.microservice.authorization;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;

/**
 * A variant of {@link org.springframework.security.web.authentication.Http403ForbiddenEntryPoint} that includes the message from the associated
 * {@link AuthenticationException} in the servlet response message.
 */
public class Http403ForbiddenEntryPoint implements AuthenticationEntryPoint {
    private static final Logger logger = LoggerFactory.getLogger(Http403ForbiddenEntryPoint.class);
    
    @Override
    public void commence(HttpServletRequest request, HttpServletResponse response, AuthenticationException authException) throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("Pre-authenticated entry point called. Rejecting access");
        }
        String message = (authException == null) ? null : authException.getMessage();
        if (message != null) {
            message = "Access denied: " + message;
        } else {
            message = "Access denied";
        }
        response.sendError(HttpServletResponse.SC_FORBIDDEN, message);
    }
}
