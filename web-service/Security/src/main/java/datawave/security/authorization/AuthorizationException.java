package datawave.security.authorization;

@SuppressWarnings("UnusedDeclaration")
public class AuthorizationException extends Exception {
    public AuthorizationException() {
        super();
    }
    
    public AuthorizationException(String message) {
        super(message);
    }
    
    public AuthorizationException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public AuthorizationException(Throwable cause) {
        super(cause);
    }
    
    protected AuthorizationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
