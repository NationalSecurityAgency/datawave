package datawave.webservice.examples;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.login.LoginContext;

import datawave.security.user.UserOperationsBean;
import datawave.security.util.DnUtils;
import datawave.user.AuthorizationsListBase;

import org.jboss.security.auth.callback.ObjectCallback;

/**
 * An example showing a call to a secured EJB from a remote client. Note that currently, many of the remote EJB calls will not work outside the JBoss VM, so
 * this example is of limited utility.
 */
public class RemoteClientLoginExample {
    public static void main(String[] args) throws Exception {
        String jbossHost = args[0];
        String jbossPort = args[1];
        String keystoreLoc = args[2];
        String keystoreType = args[3];
        String keystorePass = args[4];
        String userDN = args[5];
        String userIssuerDN = args[6];
        
        Properties p = new Properties();
        p.put("java.naming.factory.initial", "org.jnp.interfaces.NamingContextFactory");
        p.put("java.naming.factory.url.pkgs", "org.jboss.naming:org.jnp.interfaces");
        p.put("java.naming.provider.url", jbossHost + ":" + jbossPort);
        
        InitialContext ctx = new InitialContext(p);
        UserOperationsBean userOps = (UserOperationsBean) ctx.lookup("/datawav/UserOperationsBean/remote");
        
        LoginContext lc = clientLogin(keystoreLoc, keystoreType, keystorePass, userDN, userIssuerDN);
        try {
            AuthorizationsListBase auths = userOps.listEffectiveAuthorizations();
            System.out.println("Retrieving authorizations for " + userDN);
            System.out.println("Effective authorizations: " + auths);
        } finally {
            lc.logout();
        }
    }
    
    private static LoginContext clientLogin(String keystoreLoc, String keystoreType, String keystorePass, String userDN, String userIssuerDN) throws Exception {
        KeyStore keystore = KeyStore.getInstance(keystoreType);
        FileInputStream fis = new FileInputStream(keystoreLoc);
        keystore.load(fis, keystorePass.toCharArray());
        
        final X509Certificate cert = (X509Certificate) keystore.getCertificate(keystore.aliases().nextElement());
        
        // Compute the username. This would either be just a user DN if you are using a user's client
        // certificate, or a server DN combined with a proxied user DN as we demonstrate here.
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
        
        return lc;
    }
}
