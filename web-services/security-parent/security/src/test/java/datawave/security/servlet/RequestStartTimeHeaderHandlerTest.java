package datawave.security.servlet;

import static io.smallrye.common.constraint.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import datawave.security.util.SecurityConstants;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.HeaderValues;

class RequestStartTimeHeaderHandlerTest {

    /**
     * Verify that when an http request is passed to {@link RequestStartTimeHeaderHandler#handleRequest(HttpServerExchange)}, that the request start time is
     * added with the system's current nano time.
     */
    @Test
    void testHandleRequest() throws Exception {
        HttpServerExchange exchange = new HttpServerExchange(null);

        long timeBeforeAddition = System.nanoTime();

        RequestStartTimeHeaderHandler handler = new RequestStartTimeHeaderHandler(null);
        handler.handleRequest(exchange);

        long timeAfterAddition = System.nanoTime();

        // Verify that one value was added for the request start header.
        HeaderMap headerMap = exchange.getRequestHeaders();
        assertTrue(headerMap.contains(SecurityConstants.REQUEST_START_TIME_HEADER));
        HeaderValues values = headerMap.get(SecurityConstants.REQUEST_START_TIME_HEADER);
        assertEquals(1, values.size());

        // Verify that the value was the current system time.
        long requestStartTime = Long.parseLong(values.iterator().next());
        assertTrue(requestStartTime >= timeBeforeAddition);
        assertTrue(requestStartTime <= timeAfterAddition);
    }
}
