package datawave.microservice.authorization.jwt.exception;

import org.springframework.security.core.AuthenticationException;

/**
 * Thrown if an authentication request is rejected because the JWT credentials are invalid due to the signature check failing.
 */
@SuppressWarnings("unused")
public class InvalidSignatureException extends AuthenticationException {
    public InvalidSignatureException(String msg) {
        super(msg);
    }
    
    public InvalidSignatureException(String msg, Throwable t) {
        super(msg, t);
    }
}
