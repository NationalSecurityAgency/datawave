package datawave.security.authorization;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A service which is responsible for retrieving a {@link DatawaveUser} given a {@link SubjectIssuerDNPair} or a list thereof.
 */
public interface DatawaveUserService {
    
    /**
     * Retrieves the {@link DatawaveUser} for the username {@code dn}.
     *
     * @param dn
     *            the subject and (optional) DN representing the user to retrieve
     * @return the {@link DatawaveUser} for {@code dn}
     */
    DatawaveUser lookup(SubjectIssuerDNPair dn) throws AuthorizationException;
    
    /**
     * Retrieves the {@link DatawaveUser}s that correspond to the {@link SubjectIssuerDNPair}s supplied.
     *
     * @param dns
     *            the list of DNs for which to retrieve user information
     * @return the {@link DatawaveUser}s for {@code dns}
     */
    default Collection<DatawaveUser> lookup(Collection<SubjectIssuerDNPair> dns) throws AuthorizationException {
        ArrayList<DatawaveUser> users = new ArrayList<>(dns.size());
        for (SubjectIssuerDNPair dn : dns) {
            users.add(lookup(dn));
        }
        return users;
    }
    
}
