package nsa.datawave.webservice.query.exception;

import javax.ws.rs.core.Response;

public class NotFoundQueryException extends QueryException {
    
    private static final long serialVersionUID = -934464085734045755L;
    
    public NotFoundQueryException() {
        super();
    }
    
    public NotFoundQueryException(String message) {
        super(message);
    }
    
    public NotFoundQueryException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public NotFoundQueryException(Throwable cause, String errorCode) {
        super(cause, errorCode);
    }
    
    public NotFoundQueryException(DatawaveErrorCode code, Throwable cause) {
        super(code, cause);
    }
    
    public NotFoundQueryException(DatawaveErrorCode code, String debugMessage) {
        super(code, debugMessage);
    }
    
    public NotFoundQueryException(DatawaveErrorCode code, Throwable cause, String debugMessage) {
        super(code, cause, debugMessage);
    }
    
    public NotFoundQueryException(DatawaveErrorCode code) {
        super(code);
    }
    
    public NotFoundQueryException(String message, Response.Status status) {
        super(message, status);
    }
    
    public NotFoundQueryException(String message, Throwable cause, String errorCode) {
        super(message, cause, errorCode);
    }
    
    public NotFoundQueryException(String message, String errorCode) {
        super(message, errorCode);
    }
    
    public NotFoundQueryException(Throwable cause) {
        super(cause);
    }
}
