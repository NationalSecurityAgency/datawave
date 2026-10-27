package datawave.security.servlet;

import datawave.security.util.SecurityConstants;
import io.undertow.security.api.SecurityContext;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;

/**
 * A {@link HttpHandler} that inspects requests for {@value SecurityConstants#PROXIED_ENTITIES_HEADER}. If it is present and the caller has been authenticated,
 * this handler adds {@value SecurityConstants#PROXIED_ENTITIES_ACCEPTED_HEADER} to the response with the value {@code true}.
 */
public class ProxiedEntitiesAcceptedHeaderHandler implements HttpHandler {

    private static final HttpString acceptedHeader = new HttpString(SecurityConstants.PROXIED_ENTITIES_ACCEPTED_HEADER);

    private final HttpHandler next;

    /**
     * Create a new {@link ProxiedEntitiesAcceptedHeaderHandler}.
     *
     * @param next
     *            the next handler
     */
    public ProxiedEntitiesAcceptedHeaderHandler(HttpHandler next) {
        this.next = next;
    }

    @Override
    public void handleRequest(HttpServerExchange exchange) throws Exception {
        try {
            String proxiedEntities = exchange.getRequestHeaders().getFirst(SecurityConstants.PROXIED_ENTITIES_HEADER);
            SecurityContext securityContext = exchange.getSecurityContext();
            if (proxiedEntities != null && securityContext != null && securityContext.isAuthenticated()) {
                exchange.getResponseHeaders().put(acceptedHeader, Boolean.TRUE.toString());
            }
        } finally {
            if (next != null) {
                next.handleRequest(exchange);
            }
        }
    }
}
