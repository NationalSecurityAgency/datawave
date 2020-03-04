package datawave.security.system;

import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Alternative;
import javax.enterprise.inject.Produces;
import javax.interceptor.Interceptor;

import datawave.security.authorization.DatawavePrincipal;
import org.infinispan.commons.util.Base64;

/**
 * Server principal producer supplied just for Embedded mode (e.g., inside of MapReduce jars). This archive should not be included for normal web applications.
 */
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
@ApplicationScoped
public class EmbeddedServerPrincipalProducer {
    private boolean initialized = false;
    private DatawavePrincipal serverPrincipal;
    
    @Produces
    @ServerPrincipal
    public DatawavePrincipal produceServerPrincipal() {
        if (!initialized) {
            initializeServerPrincipal();
            initialized = true;
        }
        return serverPrincipal;
    }
    
    private void initializeServerPrincipal() {
        String encodedServerPrincipal = System.getProperty("server.principal");
        if (encodedServerPrincipal == null)
            throw new IllegalStateException("System property server.principal must be set to a serialized, gzip'd, base64 encoded principal.");
        serverPrincipal = (DatawavePrincipal) Base64.decodeToObject(encodedServerPrincipal);
    }
}
