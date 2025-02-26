package datawave.microservice.config.web;

public interface Constants {
    /**
     * An attribute key name to be used on an {@link javax.servlet.ServletRequest} in order to store the {@link System#nanoTime()} when the current request was
     * initiated.
     */
    String REQUEST_START_TIME_NS_ATTRIBUTE = "RequestStartTimeNS";
    
    /**
     * An attribute key name to be used to store the time required to authenticate the user for the current request.
     */
    String REQUEST_LOGIN_TIME_ATTRIBUTE = "RequestLoginTimeMillis";
    
    /**
     * The response header which will be used to report the length in time (in ms) a request took.
     */
    String OPERATION_TIME_MS_HEADER = "X-OperationTimeInMS";
    
    /**
     * The response header which will be used to report the system/server name of the server that processed the request.
     */
    String RESPONSE_ORIGIN_HEADER = "X-Response-Origin";
    
    /**
     * The name of the response header that contains the error code when there is a datawave-specific exception.
     */
    String ERROR_CODE_HEADER = "X-ErrorCode";
}
