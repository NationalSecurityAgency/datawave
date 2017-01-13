package nsa.datawave.security.authorization;

import javax.annotation.Priority;
import javax.enterprise.inject.Alternative;
import javax.interceptor.Interceptor;

/**
 * Lookup bean implementation supplied just for Embedded mode (e.g., inside of MapReduce jars). This archive should not be included for normal web applications.
 */
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
public class EmbeddedPrincipalLookupBean implements DatawavePrincipalLookup {
    @Override
    public DatawavePrincipal lookupPrincipal(String... dns) throws Exception {
        throw new UnsupportedOperationException("Not supported in embedded mode.");
    }
    
    @Override
    public DatawavePrincipal lookupPrincipal(String dn) throws Exception {
        throw new UnsupportedOperationException("Not supported in embedded mode.");
    }
    
    @Override
    public DatawavePrincipal lookupPrincipal(CacheMode cacheMode, String... dns) throws Exception {
        throw new UnsupportedOperationException("Not supported in embedded mode.");
    }
    
    @Override
    public DatawavePrincipal getCurrentPrincipal() {
        throw new UnsupportedOperationException("Not supported in embedded mode.");
    }
}
