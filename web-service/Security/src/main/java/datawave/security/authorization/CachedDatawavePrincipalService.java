package datawave.security.authorization;

import datawave.security.cache.DnList;

/**
 * A version of {@link DatawavePrincipalService} that provides cache control.
 */
public interface CachedDatawavePrincipalService extends DatawavePrincipalService {
    /**
     * Lists the DNs for all entries in the cache.
     */
    DnList list();

    /**
     * Lists DNs for all entries in the cache where the DN contains the named {@code substring}
     */
    DnList listMatching(String substring) throws Exception;

    /**
     * Evicts from the cache all entries containing {@code dn}
     */
    boolean evict(SubjectIssuerDNPair dn) throws Exception;

    /**
     * Evicts all entries from the cache.
     */
    boolean evictAll() throws Exception;
}
