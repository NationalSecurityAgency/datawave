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
public class EmbeddedDatawaveUserService implements DatawaveUserService {
    @Override
    public DatawaveUser lookup(SubjectIssuerDNPair dn) throws AuthorizationException {
        throw new UnsupportedOperationException("Not supported in embedded mode.");
    }
    
    @Override
    public Collection<DatawaveUser> lookup(Collection<SubjectIssuerDNPair> dns) throws AuthorizationException {
        throw new UnsupportedOperationException("Not supported in embedded mode.");
    }
}
