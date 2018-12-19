package datawave.webservice.query.exception;

public class QueryCanceledQueryException extends QueryException {
    
    private static final long serialVersionUID = -45193868381091635L;
    
    public QueryCanceledQueryException() {
        super();
    }
    
    public QueryCanceledQueryException(String message) {
        super(message);
    }
    
    public QueryCanceledQueryException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public QueryCanceledQueryException(Throwable cause, String errorCode) {
        super(cause, errorCode);
    }
    
    public QueryCanceledQueryException(DatawaveErrorCode code, Throwable cause) {
        super(code, cause);
    }
    
    public QueryCanceledQueryException(DatawaveErrorCode code, String debugMessage) {
        super(code, debugMessage);
    }
    
    public QueryCanceledQueryException(DatawaveErrorCode code, Throwable cause, String debugMessage) {
        super(code, cause, debugMessage);
    }
    
    public QueryCanceledQueryException(DatawaveErrorCode code) {
        super(code);
    }
    
    public QueryCanceledQueryException(String message, int httpCode) {
        super(message, httpCode);
    }
    
    public QueryCanceledQueryException(String message, Throwable cause, String errorCode) {
        super(message, cause, errorCode);
    }
    
    public QueryCanceledQueryException(String message, String errorCode) {
        super(message, errorCode);
    }
    
    public QueryCanceledQueryException(Throwable cause) {
        super(cause);
    }
}
