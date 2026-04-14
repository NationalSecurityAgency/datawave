package datawave.security.servlet;

import javax.servlet.ServletContext;

import datawave.security.util.SecurityConstants;
import io.undertow.servlet.ServletExtension;
import io.undertow.servlet.api.DeploymentInfo;

/**
 * A {@link ServletExtension} that will enrich incoming requests by adding the following headers:
 * <ul>
 * <li>{@value SecurityConstants#REQUEST_START_TIME_HEADER}: This header will be added before authentication occurs.</li>
 * <li>{@value SecurityConstants#REQUEST_LOGIN_TIME_HEADER}: This header will be added after the authentication attempt.</li>
 * </ul>
 * To register this extension with Undertow, the fully qualified class name must be added to a file named {@code io.undertow.servlet.ServletExtension} in the
 * META-INF/services directory.
 */
public class HeaderEnrichmentServletExtension implements ServletExtension {

    @Override
    public void handleDeployment(final DeploymentInfo deploymentInfo, final ServletContext servletContext) {
        // Add the request start time header handler. This will run after the servlet request context has been set up, but before any other handlers.
        deploymentInfo.addOuterHandlerChainWrapper(RequestStartTimeHeaderHandler::new);

        // Add the request login time header handler. This will run after the security handlers, but before the request is dispatched to deployment code.
        deploymentInfo.addInnerHandlerChainWrapper(RequestLoginTimeHeaderHandler::new);
    }
}
