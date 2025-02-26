package datawave.webservice.query.exception;

public class UnauthorizedQueryException extends QueryException {
    
    private static final long serialVersionUID = -656664017234074155L;
    
    public UnauthorizedQueryException() {
        super();
    }
    
    public UnauthorizedQueryException(String message) {
        super(message);
    }
    
    public UnauthorizedQueryException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public UnauthorizedQueryException(Throwable cause, String errorCode) {
        super(cause, errorCode);
    }
    
    public UnauthorizedQueryException(DatawaveErrorCode code, Throwable cause) {
        super(code, cause);
    }
    
    public UnauthorizedQueryException(DatawaveErrorCode code, String debugMessage) {
        super(code, debugMessage);
    }
    
    public UnauthorizedQueryException(DatawaveErrorCode code, Throwable cause, String debugMessage) {
        super(code, cause, debugMessage);
    }
    
    public UnauthorizedQueryException(DatawaveErrorCode code) {
        super(code);
    }
    
    public UnauthorizedQueryException(String message, int httpCode) {
        super(message, httpCode);
    }
    
    public UnauthorizedQueryException(String message, Throwable cause, String errorCode) {
        super(message, cause, errorCode);
    }
    
    public UnauthorizedQueryException(String message, String errorCode) {
        super(message, errorCode);
    }
    
    public UnauthorizedQueryException(Throwable cause) {
        super(cause);
    }
}
