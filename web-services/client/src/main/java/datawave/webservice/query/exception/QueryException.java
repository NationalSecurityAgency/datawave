package datawave.webservice.query.exception;

import java.util.List;

import javax.ws.rs.core.Response;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Class to encapsulate Datawave Mapped exceptions
 *
 */
public class QueryException extends Exception {
    
    private static final long serialVersionUID = 1L;
    
    protected String errorCode = "500-1";
    
    public QueryException() {
        super();
    }
    
    public QueryException(String message) {
        super(message);
    }
    
    public QueryException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public QueryException(Throwable cause, String errorCode) {
        super(cause);
        this.errorCode = errorCode;
    }
    
    public QueryException(DatawaveErrorCode code, Throwable cause) {
        super(code.toString(), cause);
        this.errorCode = code.getErrorCode();
    }
    
    /**
     *
     * @param code
     *            - DatawaveErrorCode for message mapping purposes
     * @param debugMessage
     *            - Debug variables that may help troubleshooting the issue.
     */
    public QueryException(DatawaveErrorCode code, String debugMessage) {
        super(code.toString() + " " + debugMessage);
        this.errorCode = code.getErrorCode();
    }
    
    /**
     *
     * @param code
     *            - DatawaveErrorCode for message mapping purposes
     * @param cause
     *            - Exception caught that caused this error
     * @param debugMessage
     *            - Debug variables that may help troubleshooting the issue.
     */
    public QueryException(DatawaveErrorCode code, Throwable cause, String debugMessage) {
        super(code.toString() + " " + debugMessage, cause);
        this.errorCode = code.getErrorCode();
    }
    
    public QueryException(DatawaveErrorCode code) {
        super(code.toString());
        this.errorCode = code.getErrorCode();
    }
    
    public QueryException(String message, Response.Status status) {
        super(message);
        // We don't know how to map this so... just use the default stuff.
        this.errorCode = Integer.toString(status.getStatusCode());
    }
    
    public QueryException(String message, Throwable cause, String errorCode) {
        super(message, cause);
        this.errorCode = errorCode;
    }
    
    public QueryException(String message, String debugMessage, String errorCode) {
        super(message + " " + debugMessage);
        this.errorCode = errorCode;
    }
    
    public QueryException(String message, String errorCode) {
        super(message);
        this.errorCode = errorCode;
    }
    
    public QueryException(Throwable cause) {
        super(cause);
    }
    
    /**
     * Method to strip off our additional mapping nonsense and get the actual HTTP response code.
     *
     * @return
     */
    public int getStatusCode() {
        int status = 500;
        if (null != errorCode && !errorCode.equals("")) {
            int idx = errorCode.indexOf("-");
            if (idx > 0) {
                String httpCode = errorCode.substring(0, idx);
                status = Integer.parseInt(httpCode);
            } else {
                status = Integer.parseInt(errorCode);
            }
        }
        return status;
    }
    
    /**
     * Returns all QueryExceptions in the stack.
     *
     * @return A list of QueryException objects in the stack.
     */
    public List<QueryException> getQueryExceptionsInStack() {
        List<Throwable> throwables = ExceptionUtils.getThrowableList(this);
        Iterable<QueryException> queryExceptions = Iterables.filter(throwables, QueryException.class);
        List<QueryException> queryExceptionList = Lists.newArrayList(queryExceptions);
        return queryExceptionList;
    }
    
    /**
     * Returns the bottom-most QueryException in the stack.
     *
     * @return The bottom-most QueryException in the stack.
     */
    public QueryException getBottomQueryException() {
        List<QueryException> queryExceptionList = getQueryExceptionsInStack();
        return queryExceptionList.get(queryExceptionList.size() - 1);
    }
    
    public String getErrorCode() {
        return errorCode;
    }
    
    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }
}
