package datawave.security.authorization;

import javax.annotation.Priority;
import javax.enterprise.inject.Alternative;
import javax.interceptor.Interceptor;
import java.util.Collection;

/**
 * Lookup bean implementation supplied just for Embedded mode (e.g., inside of MapReduce jars). This archive should not be included for normal web applications.
 */
@Alternative
@Priority(Interceptor.Priority.APPLICATION)
public class EmbeddedPrincipalLookupBean implements DatawavePrincipalService {
    @Override
    public DatawavePrincipal lookupPrincipal(SubjectIssuerDNPair dn) throws Exception {
        throw new UnsupportedOperationException("Not supported in embedded mode.");
    }
    
    @Override
    public DatawavePrincipal lookupPrincipal(Collection<SubjectIssuerDNPair> dns) throws Exception {
        throw new UnsupportedOperationException("Not supported in embedded mode.");
    }
}
