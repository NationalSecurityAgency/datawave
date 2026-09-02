package datawave.webservice.examples;

import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.concurrent.Callable;

import javax.annotation.security.RunAs;
import javax.ejb.LocalBean;
import javax.ejb.Schedule;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityIdentity;

import datawave.security.cert.SSLStores;
import datawave.security.evidence.EvidenceFactory;
import datawave.security.evidence.X509CertificateEvidence;
import datawave.security.user.UserOperationsBean;
import datawave.user.AuthorizationsListBase;

/**
 * This class demonstrates an example of how to programmatically obtain a {@link SecurityIdentity} from the current {@link SecurityDomain} in order to execute
 * operations on secured EJBs with a target user's permissions.
 */
@Startup
@Singleton
@LocalBean
@RunAs("InternalUser")
public class ClientLoginExampleBean {

    private static final Logger log = LoggerFactory.getLogger(ClientLoginExampleBean.class);

    // Inject a secured EJB we need to call.
    @Inject
    private UserOperationsBean userOperationsBean;

    // This only works if our bean is inside the EJB container. That is, you can't do it from an arbitrary client, which would instead need to get its
    // certificate some other way.
    @Inject
    private SSLStores sslStores;

    // Execute this call every minute.
    @Schedule(hour = "*", minute = "*", persistent = false)
    public void executeCall() {
        log.info("Executing scheduled call");
        try {
            // Show that the security identity before authentication is anonymous.
            SecurityDomain securityDomain = SecurityDomain.getCurrent();
            SecurityIdentity unauthenticatedIdentity = securityDomain.getCurrentSecurityIdentity();
            log.info("Current security identity within unsecured context (should be anonymous): {}", unauthenticatedIdentity.getPrincipal());

            // Grab the server certificate from the keystore (assuming it's the first one).
            KeyStore keyStore = sslStores.getKeyStore();
            final X509Certificate certificate = (X509Certificate) keyStore.getCertificate(keyStore.aliases().nextElement());

            // Create a piece of evidence that will identify the server we want to authenticate as.
            X509CertificateEvidence evidence = EvidenceFactory.getDefault().createX509CertificateEvidence(certificate, null, null);
            log.info("Authenticating with evidence: {}", evidence);

            // Authenticate and fetch a security identity using the evidence we created.
            SecurityDomain domain = SecurityDomain.getCurrent();
            SecurityIdentity identity = domain.authenticate(evidence);
            log.info("Obtained identity with principal {} and roles {}", identity.getPrincipal(), identity.getRoles());

            // Using the identity, execute the operations with the permissions of the authenticated server.
            try {
                identity.runAs((Callable<Void>) () -> {
                    AuthorizationsListBase auths = userOperationsBean.listEffectiveAuthorizations(false);
                    log.info("Authorizations for current user: {}", auths);
                    return null;
                });
            } catch (Exception e) {
                log.error("Failed to fetch user from cached credentials", e);
            }
        } catch (Exception e) {
            log.error("Failed to execute scheduled call", e);
        }
    }
}
