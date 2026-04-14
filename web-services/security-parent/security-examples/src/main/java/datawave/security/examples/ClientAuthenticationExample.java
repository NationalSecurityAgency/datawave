package datawave.security.examples;

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

import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.cache.CredentialsCacheBean;
import datawave.security.evidence.EvidenceFactory;
import datawave.security.evidence.TrustedHeaderEvidence;

/**
 * This class demonstrates an example of how to programmatically obtain a {@link SecurityIdentity} from the current {@link SecurityDomain} in order to execute
 * operations on secured EJBs with a target user's permissions.
 */
@Singleton
@Startup
@LocalBean
@RunAs("InternalUser")
public class ClientAuthenticationExample {

    private static final Logger log = LoggerFactory.getLogger(ClientAuthenticationExample.class);

    // Inject a secured EJB we need to call.
    @Inject
    private CredentialsCacheBean credentialsCacheBean;

    @Schedule(hour = "*", minute = "*", persistent = false)
    public void executeCall() {
        log.info("Executing scheduled call");
        try {
            // Show that the security identity before authentication is anonymous.
            SecurityDomain securityDomain = SecurityDomain.getCurrent();
            SecurityIdentity unauthenticatedIdentity = securityDomain.getCurrentSecurityIdentity();
            log.info("Current security identity within unsecured context (should be anonymous): {}", unauthenticatedIdentity.getPrincipal());

            // Create a piece of evidence that will identify the user we want to authenticate as.
            String subjectDn = "cn=Test A. User, c=US, o=Example Corp, ou=Example Developers";
            String issuerDn = "cn=EXAMPLE CORP CA, c=US, o=Example Corp";
            TrustedHeaderEvidence evidence = EvidenceFactory.getDefault().createTrustedHeadersEvidence(subjectDn, issuerDn, null, null);

            // Authenticate and fetch a security identity using the evidence we created.
            SecurityDomain domain = SecurityDomain.getCurrent();
            SecurityIdentity identity = domain.authenticate(evidence);

            // Using the identity, execute the operations with the permissions of the authenticated user.
            try {
                identity.runAs((Callable<Void>) () -> {
                    DatawavePrincipal principal = (DatawavePrincipal) identity.getPrincipal();
                    log.info("Fetching user from credentials cache as user {}", principal.getName());
                    DatawaveUser user = credentialsCacheBean.list(principal.getName());
                    log.info("Fetched user from credentials cache: {}", user);
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
