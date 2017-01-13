package nsa.datawave.security.cache;

@SuppressWarnings("UnusedDeclaration")
public class AuthorizationServiceException extends Exception {
    public AuthorizationServiceException() {
        super();
    }
    
    public AuthorizationServiceException(String message) {
        super(message);
    }
    
    public AuthorizationServiceException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public AuthorizationServiceException(Throwable cause) {
        super(cause);
    }
    
    protected AuthorizationServiceException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
