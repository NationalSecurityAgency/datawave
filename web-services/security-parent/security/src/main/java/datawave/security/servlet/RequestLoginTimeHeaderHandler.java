package datawave.security.servlet;

import java.util.concurrent.TimeUnit;

import datawave.security.util.SecurityConstants;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.HttpString;

/**
 * A {@link HttpHandler} that will add a {@value SecurityConstants#REQUEST_LOGIN_TIME_HEADER} request header with the delta of the request start time stored in
 * the header {@link SecurityConstants#REQUEST_START_TIME_HEADER} and the current system time in milliseconds. This handler is expected to be configured to
 * execute after the calling user for the request is authenticated.
 */
public class RequestLoginTimeHeaderHandler implements HttpHandler {

    private static final HttpString header = new HttpString(SecurityConstants.REQUEST_LOGIN_TIME_HEADER);

    private final HttpHandler next;

    /**
     * Create a new {@link RequestLoginTimeHeaderHandler} with a reference to the next handler that the incoming exchange should be passed to.
     *
     * @param next
     *            the next handler
     */
    public RequestLoginTimeHeaderHandler(HttpHandler next) {
        this.next = next;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        try {
            // Add a header with the current time as the login time.
            HeaderMap headers = exchange.getRequestHeaders();
            String startTimeValue = headers.getFirst(SecurityConstants.REQUEST_START_TIME_HEADER);
            if (startTimeValue != null) {
                long startTime = Long.parseLong(startTimeValue);
                long loginTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                headers.add(header, loginTime);
            }
        } finally {
            // Pass the exchange to the next handler.
            if (next != null) {
                next.handleRequest(exchange);
            }
        }
    }
}
