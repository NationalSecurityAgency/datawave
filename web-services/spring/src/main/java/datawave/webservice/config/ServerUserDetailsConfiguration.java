package datawave.webservice.config;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.cert.SSLStores;

@Configuration
public class ServerUserDetailsConfiguration {

    // This Supplier<ProxiedUserDetails> is necessary when Spring creates DefaultQueryLogicFactory.
    // The rest of the beans here are to support the creation of this DatawavePrincipal.
    @Bean
    @Qualifier("serverUserDetailsSupplier")
    public Supplier<ProxiedUserDetails> serverUserDetailsSupplier(@Qualifier("remoteDatawaveUserService") DatawaveUserService datawaveUserService,
                    SSLStores sslStores) {
        return new Supplier<>() {
            @Override
            public ProxiedUserDetails get() {
                try {
                    return new DatawavePrincipal(datawaveUserService.lookup(Collections.singleton(lookupServerDN(sslStores))));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    private SubjectIssuerDNPair lookupServerDN(SSLStores sslStores) throws KeyStoreException {
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
