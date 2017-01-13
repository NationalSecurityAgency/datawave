package nsa.datawave.webservice.exception;

import javax.ws.rs.core.Response;

public class AccumuloException extends Exception {
    
    private static final long serialVersionUID = 1L;
    
    private Response.Status statusCode = Response.Status.INTERNAL_SERVER_ERROR;
    
    public AccumuloException() {
        super();
    }
    
    public AccumuloException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public AccumuloException(String message) {
        super(message);
    }
    
    public AccumuloException(Throwable cause) {
        super(cause);
    }
    
    public AccumuloException(String message, Throwable cause, Response.Status status) {
        super(message, cause);
        this.statusCode = status;
    }
    
    public AccumuloException(String message, Response.Status status) {
        super(message);
        this.statusCode = status;
    }
    
    public AccumuloException(Throwable cause, Response.Status status) {
        super(cause);
        this.statusCode = status;
    }
    
    public Response.Status getStatusCode() {
        return statusCode;
    }
    
    public void setStatusCode(Response.Status statusCode) {
        this.statusCode = statusCode;
    }
    
}
