package datawave.security.servlet;

import static io.smallrye.common.constraint.Assert.assertFalse;
import static io.smallrye.common.constraint.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import datawave.security.util.SecurityConstants;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.HeaderValues;
import io.undertow.util.HttpString;

class RequestLoginTimeHeaderHandlerTest {

    /**
     * Verify that when an http request is passed to {@link RequestLoginTimeHeaderHandler#handleRequest(HttpServerExchange)} with a request start time header,
     * that a request login time header is added with the delta in milliseconds.
     */
    @Test
    void testHandleRequestGivenStartTimeHeader() throws Exception {
        HttpServerExchange exchange = new HttpServerExchange(null);

        // Add a start time header with a time 5 seconds ago.
        HeaderMap headerMap = exchange.getRequestHeaders();
        long startTime = System.nanoTime() - (TimeUnit.SECONDS.toNanos(5));
        headerMap.put(new HttpString(SecurityConstants.REQUEST_START_TIME_HEADER), String.valueOf(startTime));

        RequestLoginTimeHeaderHandler handler = new RequestLoginTimeHeaderHandler(null);
        handler.handleRequest(exchange);

        // Verify that one value was added for the request start header.
        headerMap = exchange.getRequestHeaders();
        assertTrue(headerMap.contains(SecurityConstants.REQUEST_LOGIN_TIME_HEADER));
        HeaderValues values = headerMap.get(SecurityConstants.REQUEST_LOGIN_TIME_HEADER);
        assertEquals(1, values.size());

        // Verify that the value is approximately 5 seconds in milliseconds.
        long requestStartTime = Long.parseLong(values.iterator().next());
        long fiveSeconds = TimeUnit.SECONDS.toMillis(5);
        long sixSeconds = TimeUnit.SECONDS.toMillis(6);

        assertTrue(requestStartTime >= fiveSeconds);
        assertTrue(requestStartTime <= sixSeconds);
    }

    /**
     * Verify that when an http request is passed to {@link RequestLoginTimeHeaderHandler#handleRequest(HttpServerExchange)} without a request start time
     * header, that a request login time header is not added.
     */
    @Test
    void testHandleRequestGivenNoStartTimeHeader() throws Exception {
        HttpServerExchange exchange = new HttpServerExchange(null);

        RequestLoginTimeHeaderHandler handler = new RequestLoginTimeHeaderHandler(null);
        handler.handleRequest(exchange);

        HeaderMap headerMap = exchange.getRequestHeaders();
        // Verify that one value was added for the request start header.
        assertFalse(headerMap.contains(SecurityConstants.REQUEST_LOGIN_TIME_HEADER));
    }
}
