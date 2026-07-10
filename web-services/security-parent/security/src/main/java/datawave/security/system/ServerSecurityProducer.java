package datawave.security.system;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.X509Certificate;
import java.util.Collections;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.cert.SSLStores;
import datawave.security.user.UserOperationsBean;

/**
 * A producer class for generating server-security related artifacts. For one, we produce the server DN of the server that we are running inside of.
 */
@ApplicationScoped
public class ServerSecurityProducer {

    @Inject
    private SSLStores sslStores;

    @Inject
    private DatawaveUserService datawaveUserService;

    @Inject
    private UserOperationsBean userOperationsBean;

    /**
     * Produces a {@link DatawavePrincipal} that is {@link RequestScoped}. This is the principal of the calling user--that is, the principal that is available
     * from the {@link javax.ejb.EJBContext} of an EJB.
     *
     * @return the principal of the calling user
     */
    @Produces
    @CallerPrincipal
    @RequestScoped
    public DatawavePrincipal produceCallerPrincipal() {
        DatawavePrincipal dp = userOperationsBean.getCurrentPrincipal();
        return dp == null ? DatawavePrincipal.anonymousPrincipal() : dp;
    }

    /**
     * Produces a {@link DatawavePrincipal} that is {@link RequestScoped}. This is a principal that is filled in with the name and authorizations for the server
     * that is currently running DATAWAVE.
     *
     * @return a datawave principal
     * @throws Exception
     *             if there are issues
     */
    @Produces
    @ServerPrincipal
    @RequestScoped
    public DatawavePrincipal produceServerPrincipal() throws Exception {
        return new DatawavePrincipal(datawaveUserService.lookup(Collections.singleton(lookupServerDN())));
    }

    private SubjectIssuerDNPair lookupServerDN() throws KeyStoreException {
        if (sslStores == null) {
            throw new IllegalStateException("SSL Context not injected.");
        }

        KeyStore keystore = sslStores.getKeyStore();
        final X509Certificate cert = (X509Certificate) keystore.getCertificate(keystore.aliases().nextElement());
        final String serverDN = cert.getSubjectX500Principal().getName();
        final String serverIssuerDN = cert.getIssuerX500Principal().getName();
        return SubjectIssuerDNPair.of(serverDN, serverIssuerDN);
    }
}
