package datawave.microservice.authorization.jwt.exception;

import org.springframework.security.core.AuthenticationException;

/**
 * Thrown if an authentication request is rejected because the JWT credentials represent a JWT that has expired.
 */
@SuppressWarnings("unused")
public class TokenExpiredException extends AuthenticationException {
    public TokenExpiredException(String msg) {
        super(msg);
    }
    
    public TokenExpiredException(String msg, Throwable t) {
        super(msg, t);
    }
}
