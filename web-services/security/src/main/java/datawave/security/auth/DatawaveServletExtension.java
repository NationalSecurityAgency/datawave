package datawave.security.auth;

import javax.servlet.ServletContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.undertow.security.api.AuthenticationMechanismFactory;
import io.undertow.servlet.ServletExtension;
import io.undertow.servlet.api.DeploymentInfo;

/**
 * Datawave Servlet Extension that is here simply to register {@link DatawaveAuthenticationMechanism} as an acceptable authentication mechanism for use in
 * web.xml files.
 */
public class DatawaveServletExtension implements ServletExtension {

    private static final Logger log = LoggerFactory.getLogger(DatawaveServletExtension.class);

    @Override
    public void handleDeployment(DeploymentInfo deploymentInfo, ServletContext servletContext) {
        log.trace("enter: handleDeployment(DeploymentInfo, ServletContext)");
        AuthenticationMechanismFactory factory = new DatawaveAuthenticationMechanism.Factory(deploymentInfo.getIdentityManager());
        deploymentInfo.addAuthenticationMechanism(DatawaveAuthenticationMechanism.MECHANISM_NAME, factory);
        log.trace("exit: handleDeployment(DeploymentInfo, ServletContext)");
    }
}
