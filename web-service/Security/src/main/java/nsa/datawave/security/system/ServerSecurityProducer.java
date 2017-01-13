package nsa.datawave.security.system;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.X509Certificate;

import nsa.datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import nsa.datawave.configuration.RefreshableScope;
import nsa.datawave.security.authorization.DatawavePrincipal;
import nsa.datawave.security.authorization.DatawavePrincipalLookupBean;
import nsa.datawave.security.util.DnUtils;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.jboss.security.JSSESecurityDomain;

import javax.annotation.Resource;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

/**
 * A producer class for generating server-security related artifacts. For one, we produce the server DN of the server that we are running inside of. We allso
 * produce the {@link JSSESecurityDomain} for our application. We use this rather than directly injecting at each site using {@link Resource} since the producer
 * allows us to use a plain {@link javax.inject.Inject} annotation versus having to specify the resource name each time we inject with {@link Resource}. This
 * way, we only name the resource once.
 */
@ApplicationScoped
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class ServerSecurityProducer {
    // Allow injection of JSSESecurityDomain without having to specify the JNDI name at each injection point.
    // Instead, users can simply do:
    // @Inject private JSSESecurityDomain jsseSecurityDomain
    // and the specification of the resource location is limited to this class.
    @Produces
    @Resource(name = "java:jboss/jaas/datawave/jsse")
    private JSSESecurityDomain domain;
    
    @Inject
    private DatawavePrincipalLookupBean datawavePrincipalLookupBean;
    
    /**
     * Produces a {@link DatawavePrincipal} that is {@link RequestScoped}. This is a principal that is filled in with the name and authorizations for the server
     * that is currently running DATAWAVE.
     */
    @Produces
    @ServerPrincipal
    @RequestScoped
    public DatawavePrincipal produceServerPrincipal() throws Exception {
        return datawavePrincipalLookupBean.lookupPrincipal(lookupServerDN());
    }
    
    private String lookupServerDN() throws KeyStoreException {
        if (domain == null) {
            throw new IllegalArgumentException("Unable to find security domain.");
        }
        
        KeyStore keystore = domain.getKeyStore();
        final X509Certificate cert = (X509Certificate) keystore.getCertificate(keystore.aliases().nextElement());
        final String serverDN = cert.getSubjectX500Principal().getName();
        final String serverIssuerDN = cert.getIssuerX500Principal().getName();
        return DnUtils.buildNormalizedProxyDN(serverDN, serverIssuerDN, null, null);
        
    }
}
