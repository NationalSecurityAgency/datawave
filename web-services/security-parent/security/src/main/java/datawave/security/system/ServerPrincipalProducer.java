package datawave.security.system;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.function.Supplier;

import javax.annotation.PostConstruct;
import javax.enterprise.context.RequestScoped;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.cert.SSLStores;

/**
 * A producer class for generating server-security related artifacts. For one, we produce the server DN of the server that we are running inside of.
 */
public class ServerPrincipalProducer {

    private SSLStores sslStores;
    private DatawaveUserService datawaveUserService;

    @PostConstruct
    public void init() {
        if (sslStores == null) {
            throw new IllegalStateException("SSL Context not injected.");
        }
        if (datawaveUserService == null) {
            throw new IllegalStateException("DatawaveUserService not injected.");
        }
    }

    /**
     * Produces a {@link DatawavePrincipal} that is {@link RequestScoped}. This is a principal that is filled in with the name and authorizations for the server
     * that is currently running DATAWAVE.
     *
     * @return a datawave principal
     * @throws Exception
     *             if there are issues
     */
    @Bean
    @Qualifier("serverPrincipal")
    public DatawavePrincipal serverPrincipal() throws Exception {
        return new DatawavePrincipal(datawaveUserService.lookup(Collections.singleton(lookupServerDN())));
    }

    @Bean
    @ConditionalOnMissingBean
    public Supplier<DatawavePrincipal> serverPrincipalSupplier() {
        return () -> {
            try {
                return serverPrincipal();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
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

    public void setSslStores(SSLStores sslStores) {
        this.sslStores = sslStores;
    }

    public void setDatawaveUserService(DatawaveUserService datawaveUserService) {
        this.datawaveUserService = datawaveUserService;
    }
}
