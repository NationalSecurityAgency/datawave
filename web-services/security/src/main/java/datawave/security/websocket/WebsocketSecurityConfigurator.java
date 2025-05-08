package datawave.security.websocket;

import static datawave.webservice.metrics.Constants.REQUEST_LOGIN_TIME_HEADER;

import java.util.List;
import java.util.Map;

import javax.websocket.HandshakeResponse;
import javax.websocket.server.HandshakeRequest;
import javax.websocket.server.ServerEndpointConfig;
import javax.websocket.server.ServerEndpointConfig.Configurator;

import org.jboss.logging.Logger;
import org.jboss.security.SecurityContextAssociation;

/**
 * A JBoss AS/Wildfly-specific {@link Configurator} that saves the incoming JAAS credentials into the user session so that WebSocket handler methods can be
 * invoked using this security context. This covers a hole in the specification that doesn't allow for the propagation of security credentials to the WebSocket
 * handlers. See <a href="https://java.net/jira/browse/WEBSOCKET_SPEC-238">WEBSOCKET_SPEC-238</a> for more details.
 */
public class WebsocketSecurityConfigurator extends Configurator {

    private static final Logger log = org.jboss.logging.Logger.getLogger(WebsocketSecurityConfigurator.class);

    @Override
    public void modifyHandshake(ServerEndpointConfig sec, HandshakeRequest request, HandshakeResponse response) {
        log.trace("enter: modifyHandshake(ServerEndpointConfig, HandshakeRequest, HandshakeResponse)");
        super.modifyHandshake(sec, request, response);

        if (log.isTraceEnabled()) {
            log.trace("setting session principal to " + request.getUserPrincipal());
            log.trace("setting session subject to " + SecurityContextAssociation.getSubject());
            log.trace("setting session credential to " + SecurityContextAssociation.getPrincipal());
        }
        sec.getUserProperties().put(WebsocketSecurityInterceptor.SESSION_PRINCIPAL, request.getUserPrincipal());
        sec.getUserProperties().put(WebsocketSecurityInterceptor.SESSION_SUBJECT, SecurityContextAssociation.getSubject());
        sec.getUserProperties().put(WebsocketSecurityInterceptor.SESSION_CREDENTIAL, SecurityContextAssociation.getPrincipal());
        Map<String,List<String>> headers = request.getHeaders();
        if (headers != null) {
            List<String> loginHeader = headers.get(REQUEST_LOGIN_TIME_HEADER);
            if (loginHeader != null && !loginHeader.isEmpty()) {
                sec.getUserProperties().put(REQUEST_LOGIN_TIME_HEADER, loginHeader.get(0));
            }
        }
        log.trace("exit: modifyHandshake(ServerEndpointConfig, HandshakeRequest, HandshakeResponse)");
    }
}
