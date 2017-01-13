package nsa.datawave.webservice.metrics;

public interface Constants {
    /**
     * An internal header used to store the start time of the HTTP request, as retrieved from the web container (e.g., Undertow).
     */
    String REQUEST_START_TIME_HEADER = "X-Internal-RequestStartTimeNanos";
    /**
     * An internal header used to store the time required to authenticate the user for the current request.
     */
    String REQUEST_LOGIN_TIME_HEADER = "X-Internal-RequestLoginTimeMillis";
}
