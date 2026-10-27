package datawave.security.servlet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import datawave.security.util.SecurityConstants;
import io.undertow.security.api.SecurityContext;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.HttpString;

class ProxiedEntitiesAcceptedHeaderHandlerTest {

    /**
     * Verify that when proxied entities are present and the caller is authenticated, the accepted proxied entities response header is added.
     */
    @Test
    void testHandleRequestAuthenticatedWithProxiedEntitiesHeader() throws Exception {
        HttpServerExchange exchange = new HttpServerExchange(null);
        exchange.getRequestHeaders().put(new HttpString(SecurityConstants.PROXIED_ENTITIES_HEADER), "<CN=proxied user><CN=proxied server>");

        SecurityContext securityContext = mock(SecurityContext.class);
        when(securityContext.isAuthenticated()).thenReturn(true);
        exchange.setSecurityContext(securityContext);

        ProxiedEntitiesAcceptedHeaderHandler handler = new ProxiedEntitiesAcceptedHeaderHandler(null);
        handler.handleRequest(exchange);

        HeaderMap responseHeaders = exchange.getResponseHeaders();
        assertTrue(responseHeaders.contains(SecurityConstants.PROXIED_ENTITIES_ACCEPTED_HEADER));
        assertEquals(Boolean.TRUE.toString(), responseHeaders.getFirst(SecurityConstants.PROXIED_ENTITIES_ACCEPTED_HEADER));
    }

    /**
     * Verify that when proxied entities are present but the caller is not authenticated, no accepted proxied entities response header is added.
     */
    @Test
    void testHandleRequestUnauthenticatedWithProxiedEntitiesHeader() throws Exception {
        HttpServerExchange exchange = new HttpServerExchange(null);
        exchange.getRequestHeaders().put(new HttpString(SecurityConstants.PROXIED_ENTITIES_HEADER), "<CN=proxied user><CN=proxied server>");

        SecurityContext securityContext = mock(SecurityContext.class);
        when(securityContext.isAuthenticated()).thenReturn(false);
        exchange.setSecurityContext(securityContext);

        ProxiedEntitiesAcceptedHeaderHandler handler = new ProxiedEntitiesAcceptedHeaderHandler(null);
        handler.handleRequest(exchange);

        assertFalse(exchange.getResponseHeaders().contains(SecurityConstants.PROXIED_ENTITIES_ACCEPTED_HEADER));
    }

    /**
     * Verify that when the proxied entities header is absent, no accepted proxied entities response header is added.
     */
    @Test
    void testHandleRequestAuthenticatedWithoutProxiedEntitiesHeader() throws Exception {
        HttpServerExchange exchange = new HttpServerExchange(null);

        SecurityContext securityContext = mock(SecurityContext.class);
        when(securityContext.isAuthenticated()).thenReturn(true);
        exchange.setSecurityContext(securityContext);

        ProxiedEntitiesAcceptedHeaderHandler handler = new ProxiedEntitiesAcceptedHeaderHandler(null);
        handler.handleRequest(exchange);

        assertFalse(exchange.getResponseHeaders().contains(SecurityConstants.PROXIED_ENTITIES_ACCEPTED_HEADER));
    }
}
