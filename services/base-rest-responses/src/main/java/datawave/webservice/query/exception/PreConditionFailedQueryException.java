package datawave.webservice.query.exception;

public class PreConditionFailedQueryException extends QueryException {
    
    private static final long serialVersionUID = -682794401380142802L;
    
    public PreConditionFailedQueryException() {
        super();
    }
    
    public PreConditionFailedQueryException(String message) {
        super(message);
    }
    
    public PreConditionFailedQueryException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public PreConditionFailedQueryException(Throwable cause, String errorCode) {
        super(cause, errorCode);
    }
    
    public PreConditionFailedQueryException(DatawaveErrorCode code, Throwable cause) {
        super(code, cause);
    }
    
    public PreConditionFailedQueryException(DatawaveErrorCode code, String debugMessage) {
        super(code, debugMessage);
    }
    
    public PreConditionFailedQueryException(DatawaveErrorCode code, Throwable cause, String debugMessage) {
        super(code, cause, debugMessage);
    }
    
    public PreConditionFailedQueryException(DatawaveErrorCode code) {
        super(code);
    }
    
    public PreConditionFailedQueryException(String message, int httpCode) {
        super(message, httpCode);
    }
    
    public PreConditionFailedQueryException(String message, Throwable cause, String errorCode) {
        super(message, cause, errorCode);
    }
    
    public PreConditionFailedQueryException(String message, String errorCode) {
        super(message, errorCode);
    }
    
    public PreConditionFailedQueryException(Throwable cause) {
        super(cause);
    }
}
