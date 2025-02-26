package datawave.webservice.query.exception;

public class TimeoutQueryException extends QueryException {
    
    private static final long serialVersionUID = -7756640273640045775L;
    
    public TimeoutQueryException() {
        super();
    }
    
    public TimeoutQueryException(String message) {
        super(message);
    }
    
    public TimeoutQueryException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public TimeoutQueryException(Throwable cause, String errorCode) {
        super(cause, errorCode);
    }
    
    public TimeoutQueryException(DatawaveErrorCode code, Throwable cause) {
        super(code, cause);
    }
    
    public TimeoutQueryException(DatawaveErrorCode code, String debugMessage) {
        super(code, debugMessage);
    }
    
    public TimeoutQueryException(DatawaveErrorCode code, Throwable cause, String debugMessage) {
        super(code, cause, debugMessage);
    }
    
    public TimeoutQueryException(DatawaveErrorCode code) {
        super(code);
    }
    
    public TimeoutQueryException(String message, int httpCode) {
        super(message, httpCode);
    }
    
    public TimeoutQueryException(String message, Throwable cause, String errorCode) {
        super(message, cause, errorCode);
    }
    
    public TimeoutQueryException(String message, String errorCode) {
        super(message, errorCode);
    }
    
    public TimeoutQueryException(Throwable cause) {
        super(cause);
    }
}
