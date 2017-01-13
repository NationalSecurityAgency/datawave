package nsa.datawave.webservice.query.exception;

import javax.ws.rs.core.Response;

public class NoResultsQueryException extends QueryException {
    
    private static final long serialVersionUID = -45816465824091965L;
    
    public NoResultsQueryException() {
        super();
    }
    
    public NoResultsQueryException(String message) {
        super(message);
    }
    
    public NoResultsQueryException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public NoResultsQueryException(Throwable cause, String errorCode) {
        super(cause, errorCode);
    }
    
    public NoResultsQueryException(DatawaveErrorCode code, Throwable cause) {
        super(code, cause);
    }
    
    public NoResultsQueryException(DatawaveErrorCode code, String debugMessage) {
        super(code, debugMessage);
    }
    
    public NoResultsQueryException(DatawaveErrorCode code, Throwable cause, String debugMessage) {
        super(code, cause, debugMessage);
    }
    
    public NoResultsQueryException(DatawaveErrorCode code) {
        super(code);
    }
    
    public NoResultsQueryException(String message, Response.Status status) {
        super(message, status);
    }
    
    public NoResultsQueryException(String message, Throwable cause, String errorCode) {
        super(message, cause, errorCode);
    }
    
    public NoResultsQueryException(String message, String errorCode) {
        super(message, errorCode);
    }
    
    public NoResultsQueryException(Throwable cause) {
        super(cause);
    }
}
