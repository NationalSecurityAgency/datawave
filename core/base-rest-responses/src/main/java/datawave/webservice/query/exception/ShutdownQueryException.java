package datawave.webservice.query.exception;

public class ShutdownQueryException extends QueryException {
    
    private static final long serialVersionUID = -8304923711360112605L;
    
    public ShutdownQueryException() {
        super();
    }
    
    public ShutdownQueryException(String message) {
        super(message);
    }
    
    public ShutdownQueryException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public ShutdownQueryException(Throwable cause, String errorCode) {
        super(cause, errorCode);
    }
    
    public ShutdownQueryException(DatawaveErrorCode code, Throwable cause) {
        super(code, cause);
    }
    
    public ShutdownQueryException(DatawaveErrorCode code, String debugMessage) {
        super(code, debugMessage);
    }
    
    public ShutdownQueryException(DatawaveErrorCode code, Throwable cause, String debugMessage) {
        super(code, cause, debugMessage);
    }
    
    public ShutdownQueryException(DatawaveErrorCode code) {
        super(code);
    }
    
    public ShutdownQueryException(String message, int httpCode) {
        super(message, httpCode);
    }
    
    public ShutdownQueryException(String message, Throwable cause, String errorCode) {
        super(message, cause, errorCode);
    }
    
    public ShutdownQueryException(String message, String errorCode) {
        super(message, errorCode);
    }
    
    public ShutdownQueryException(Throwable cause) {
        super(cause);
    }
}
