package datawave.security.websocket;

import static datawave.security.util.SecurityConstants.REQUEST_LOGIN_TIME_HEADER;

import java.util.List;
import java.util.Map;

import javax.websocket.HandshakeResponse;
import javax.websocket.server.HandshakeRequest;
import javax.websocket.server.ServerEndpointConfig;
import javax.websocket.server.ServerEndpointConfig.Configurator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityIdentity;

/**
 * A JBOSS AS/Wildfly-specific {@link Configurator} that stores the invoking user's security identity and the request login time into the user session so that
 * websocket handler methods can be invoked via the security identity. This covers a hole in the specifications that does not allow for the propagation of
 * security credentials to websocket handlers. See <a href="https://github.com/jakartaee/websocket/issues/238">Jakarta EE #238</a> for more details.
 */
public class WebsocketSecurityConfigurator extends Configurator {

    public static final String SESSION_SECURITY_IDENTITY = "websocket.security.identity";

    private static final Logger log = LoggerFactory.getLogger(WebsocketSecurityConfigurator.class);

    @Override
    public void modifyHandshake(ServerEndpointConfig sec, HandshakeRequest request, HandshakeResponse response) {
        super.modifyHandshake(sec, request, response);

        // Store the current security identity in the user properties so that it will be accessible later. This method will be invoked when the http request is
        // upgraded to websocket after authentication has completed. The current security identity will represent the calling user who started the websocket
        // session.
        SecurityIdentity identity = SecurityDomain.getCurrent().getCurrentSecurityIdentity();
        Map<String,Object> userProperties = sec.getUserProperties();
        if (userProperties != null) {
            userProperties.put(SESSION_SECURITY_IDENTITY, identity);
            if (log.isTraceEnabled()) {
                log.trace("Stored security identity in user property {}", SESSION_SECURITY_IDENTITY);
            }
        }

        // Store the request login time in the user properties so that it will be accessible later.
        Map<String,List<String>> headers = request.getHeaders();
        if (headers != null) {
            List<String> loginHeader = headers.get(REQUEST_LOGIN_TIME_HEADER);
            if (loginHeader != null && !loginHeader.isEmpty()) {
                sec.getUserProperties().put(REQUEST_LOGIN_TIME_HEADER, loginHeader.get(0));
                if (log.isTraceEnabled()) {
                    log.trace("Stored request login time in user property {}", REQUEST_LOGIN_TIME_HEADER);
                }
            }
        }
    }
}
