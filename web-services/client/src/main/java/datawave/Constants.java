package datawave;

public interface Constants {
    /**
     * The name of the cookie created by Query create and CachedResults create methods.
     */
    String QUERY_COOKIE_NAME = "query-session-id";

    /**
     * The name of the response header returned by calls to next() to tell caller the page number that is being returned
     */
    String PAGE_NUMBER = "X-query-page-number";

    /**
     * The name of the response header returned by calls to next to tell caller that there are no more pages
     */
    String IS_LAST_PAGE = "X-query-last-page";

    /**
     * The name of the response header that conveys server side operation time
     */
    String OPERATION_TIME = "X-OperationTimeInMS";

    /**
     * The name of the response header that conveys server side operation start time
     */
    String OPERATION_START_TIME = "X-OperationStartTimeInMS";

    /**
     * The name of the response header that conveys server side serialization time
     */
    String SERIALIZATION_TIME = "X-SerializationTimeInMS";

    /**
     * The name of the cluster and web server node
     */
    String RESPONSE_ORIGIN = "X-Response-Origin";

    /**
     * True if the query response does not have a full page of results
     */
    String PARTIAL_RESULTS = "X-Partial-Results";

    /**
     * The name of the response header that contains the error code when there is a datawave-specific exception.
     */
    String ERROR_CODE = "X-ErrorCode";

}
