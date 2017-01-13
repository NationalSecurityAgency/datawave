package nsa.datawave;

public interface Constants {
    /**
     * The name of the cookie created by Query create and CachedResults create methods.
     */
    public static final String QUERY_COOKIE_NAME = "query-session-id";
    
    /**
     * The name of the response header returned by calls to next() to tell caller the page number that is being returned
     */
    public static final String PAGE_NUMBER = "X-query-page-number";
    
    /**
     * The name of the response header returned by calls to next to tell caller that there are no more pages
     */
    public static final String IS_LAST_PAGE = "X-query-last-page";
    
    /**
     * The name of the response header that conveys server side operation time
     */
    public static final String OPERATION_TIME = "X-OperationTimeInMS";
    
    /**
     * The name of the response header that conveys server side operation start time
     */
    public static final String OPERATION_START_TIME = "X-OperationStartTimeInMS";
    
    /**
     * The name of the response header that conveys server side serialization time
     */
    public static final String SERIALIZATION_TIME = "X-SerializationTimeInMS";
    
    /**
     * The name of the cluster and web server node
     */
    public static final String RESPONSE_ORIGIN = "X-Response-Origin";
    
    /**
     * True if the query response does not have a full page of results
     */
    public static final String PARTIAL_RESULTS = "X-Partial-Results";
    
}
