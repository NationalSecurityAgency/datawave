package datawave.microservice.authorization.jwt.exception;

import org.springframework.security.core.AuthenticationException;

/**
 * Thrown if an authentication request is rejected because the Authorization header cannot be parsed and converted to a JWT.
 */
@SuppressWarnings("unused")
public class InvalidTokenException extends AuthenticationException {
    public InvalidTokenException(String msg) {
        super(msg);
    }
    
    public InvalidTokenException(String msg, Throwable t) {
        super(msg, t);
    }
}
