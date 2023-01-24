package datawave.webservice.examples;

import java.security.KeyStore;
import java.security.cert.X509Certificate;

import javax.annotation.security.RunAs;
import javax.ejb.LocalBean;
import javax.ejb.Schedule;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.login.LoginContext;

import datawave.security.user.UserOperationsBean;
import datawave.security.util.DnUtils;
import datawave.user.AuthorizationsListBase;

import org.jboss.security.JSSESecurityDomain;
import org.jboss.security.auth.callback.ObjectCallback;

/**
 * An example timer bean that shows how one would call a secured EJB from an unsecured context, such as a message-driven bean callback.
 */
@Startup
@Singleton
@LocalBean
@RunAs("InternalUser")
public class ClientLoginExampleBean {
    
    // Inject a secured EJB that we need to call.
    @Inject
    private UserOperationsBean userOps;
    
    // This only works if our bean is inside the EJB container.
    // That is, you can't do it from an arbitrary client, which would instead need
    // to get its certificate some other way.
    @Inject
    private JSSESecurityDomain domain;
    
    @Schedule(hour = "*", minute = "*", second = "0", persistent = false)
    public void doScheduledEvent() {
        try {
            // Grab the server certificate from the keystore (we are assuming it is the first one).
            // This is the credential we'll set on the object callback.
            KeyStore keystore = domain.getKeyStore();
            final X509Certificate cert = (X509Certificate) keystore.getCertificate(keystore.aliases().nextElement());
            
            // Compute the username. This would either be just a user DN if you are using a user's client
            // certificate, or a server DN combined with a proxied user DN as we demonstrate here.
            String userDN = System.getenv("USER_DN"); // Normally a username would go here. Hack for local testing--query the sid running jboss.
            String userIssuerDN = System.getenv("ISSUER_DN"); // We need the issuer of the user's cert. This needs to be set in the environment for this test.
            String serverDN = cert.getSubjectX500Principal().getName();
            String serverIssuerDN = cert.getIssuerX500Principal().getName();
            final String dn = DnUtils.buildNormalizedProxyDN(serverDN, serverIssuerDN, userDN, userIssuerDN);
            
            // Handle the callback for authentication. We expect two callbacks, a NameCallback and an ObjectCallback.
            CallbackHandler cbh = new CallbackHandler() {
                @Override
                public void handle(Callback[] callbacks) {
                    NameCallback nc = (NameCallback) callbacks[0];
                    ObjectCallback oc = (ObjectCallback) callbacks[1];
                    nc.setName(dn);
                    oc.setCredential(cert);
                }
            };
            
            // Authenticate to the DATAWAVE client domain. This saves the credentials
            // we passed in the callback handler above, and passes them along to the server
            // when we attempt any calls that require a login on the server.
            LoginContext lc = new LoginContext("datawave-client", cbh);
            lc.login();
            
            // Call secured EJBs
            try {
                AuthorizationsListBase auths = userOps.listEffectiveAuthorizations(false);
                System.err.println("***** Auths for user " + dn + " are: " + auths);
            } finally {
                // Logout, which will restore previous credentials, if any.
                // Be sure to do this in a finally block.
                lc.logout();
            }
        } catch (Exception e) {
            System.err.println("Error doing login!");
            e.printStackTrace(System.err);
        }
    }
}
