package datawave.security.authorization;

import java.util.Collection;

/**
 * A service which is responsible for retrieving a {@link DatawaveUser} given a {@link SubjectIssuerDNPair} or a list thereof.
 */
public interface DatawaveUserService {
    
    /**
     * Retrieves the {@link DatawaveUser}s that correspond to the {@link SubjectIssuerDNPair}s supplied.
     *
     * @param dns
     *            the list of DNs for which to retrieve user information
     * @return the {@link DatawaveUser}s for {@code dns}
     */
    Collection<DatawaveUser> lookup(Collection<SubjectIssuerDNPair> dns) throws AuthorizationException;
    
}
