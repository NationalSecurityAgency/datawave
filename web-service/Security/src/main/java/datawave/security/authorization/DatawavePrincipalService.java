package datawave.security.authorization;

import java.util.Collection;

/**
 * A service which is responsible for retrieving a {@link DatawavePrincipal} given a {@link SubjectIssuerDNPair}
 * or a list thereof.
 */
public interface DatawavePrincipalService {

    /**
     * Retrieves the {@link DatawavePrincipal} for the username {@code dn}.
     *
     * @param dn the subject and (optional) DN representing the user to retrieve
     * @return the {@link DatawavePrincipal} for {@code dn}
     */
    DatawavePrincipal lookupPrincipal(SubjectIssuerDNPair dn) throws Exception;

    /**
     * Retrieves the {@link DatawavePrincipal} for the proxied users in {@code dns}.
     * This retrieves a principal that represents a user who was proxied through
     * one or more servers.
     *
     * @param dns the subject and (optional) DNs representing the user and servers to retrieve
     * @return the {@link DatawavePrincipal} for {@code dns}
     */
    DatawavePrincipal lookupPrincipal(Collection<SubjectIssuerDNPair> dns) throws Exception;
    
}
