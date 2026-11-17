package datawave.security.servlet;

import datawave.security.util.SecurityConstants;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;

/**
 * A {@link HttpHandler} that will add a {@value SecurityConstants#REQUEST_START_TIME_HEADER} request header with the current system time in nanoseconds. This
 * handler is expected to be configured to execute before the calling user for the request is authenticated.
 */
public class RequestStartTimeHeaderHandler implements HttpHandler {

    private static final HttpString header = new HttpString(SecurityConstants.REQUEST_START_TIME_HEADER);

    private final HttpHandler next;

    /**
     * Create a new {@link RequestStartTimeHeaderHandler} with a reference to the next handler that the incoming exchange should be passed to.
     *
     * @param next
     *            the next handler
     */
    public RequestStartTimeHeaderHandler(HttpHandler next) {
        this.next = next;
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) throws Exception {
        try {
            // Add the header.
            long startTime = System.nanoTime();
            exchange.getRequestHeaders().add(header, startTime);
        } finally {
            // Pass the exchange to the next handler.
            if (this.next != null) {
                this.next.handleRequest(exchange);
            }
        }
    }
}
