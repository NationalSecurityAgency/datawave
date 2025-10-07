package datawave.security.auth;

import javax.servlet.ServletContext;

import io.undertow.security.api.AuthenticationMechanismFactory;
import io.undertow.servlet.ServletExtension;
import io.undertow.servlet.api.DeploymentInfo;

/**
 * Datawave Servlet Extension that is here simply to register {@link DatawaveAuthenticationMechanism} as an acceptable authentication mechanism for use in
 * web.xml files.
 */
public class DatawaveServletExtension implements ServletExtension {
    @Override
    public void handleDeployment(DeploymentInfo deploymentInfo, ServletContext servletContext) {
        AuthenticationMechanismFactory factory = new DatawaveAuthenticationMechanism.Factory(deploymentInfo.getIdentityManager());
        deploymentInfo.addAuthenticationMechanism(DatawaveAuthenticationMechanism.MECHANISM_NAME, factory);
    }
}
