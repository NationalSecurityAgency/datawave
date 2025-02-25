package datawave.webservice.query.exception;

public class BadRequestQueryException extends QueryException {
    
    private static final long serialVersionUID = -65346401073845783L;
    
    public BadRequestQueryException() {
        super();
    }
    
    public BadRequestQueryException(String message) {
        super(message);
    }
    
    public BadRequestQueryException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public BadRequestQueryException(Throwable cause, String errorCode) {
        super(cause, errorCode);
    }
    
    public BadRequestQueryException(DatawaveErrorCode code, Throwable cause) {
        super(code, cause);
    }
    
    public BadRequestQueryException(DatawaveErrorCode code, String debugMessage) {
        super(code, debugMessage);
    }
    
    public BadRequestQueryException(DatawaveErrorCode code, Throwable cause, String debugMessage) {
        super(code, cause, debugMessage);
    }
    
    public BadRequestQueryException(DatawaveErrorCode code) {
        super(code);
    }
    
    public BadRequestQueryException(String message, int httpCode) {
        super(message, httpCode);
    }
    
    public BadRequestQueryException(String message, Throwable cause, String errorCode) {
        super(message, cause, errorCode);
    }
    
    public BadRequestQueryException(String message, String debugMessage, String errorCode) {
        super(message, debugMessage, errorCode);
    }
    
    public BadRequestQueryException(String message, String errorCode) {
        super(message, errorCode);
    }
    
    public BadRequestQueryException(Throwable cause) {
        super(cause);
    }
}
