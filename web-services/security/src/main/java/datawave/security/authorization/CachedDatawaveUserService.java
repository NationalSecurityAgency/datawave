package datawave.security.authorization;

import java.util.Collection;

/**
 * A version of {@link DatawaveUserService} that provides cache control.
 */
public interface CachedDatawaveUserService extends DatawaveUserService {
    
    /**
     * Retrieves the {@link DatawaveUser}s that correspond to the {@link SubjectIssuerDNPair}s supplied. Unlike the {@link #lookup(Collection)} method, this
     * method guarantees that any caching is bypassed and users are reloaded from the back end service.
     *
     * @param dns
     *            the list of DNs for which to retrieve user information
     * @return the {@link DatawaveUser}s for {@code dns}
     */
    Collection<DatawaveUser> reload(Collection<SubjectIssuerDNPair> dns) throws AuthorizationException;
    
    /**
     * Returns the {@link DatawaveUser} having the specified name as its {@link DatawaveUser#getName()} value.
     */
    DatawaveUser list(String name);
    
    /**
     * Lists the DNs for all entries in the cache.
     */
    Collection<? extends DatawaveUserInfo> listAll();
    
    /**
     * Lists DNs for all entries in the cache where the {@link DatawaveUser#getName()} contains the supplied string.
     */
    Collection<? extends DatawaveUserInfo> listMatching(String substring);
    
    /**
     * Evicts from the cache all entries whose {@link DatawaveUser#getName()} equals the supplied name
     */
    String evict(String name);
    
    /**
     * Evicts from the cache all entries whose {@link DatawaveUser#getName()} contains the supplied substring
     */
    String evictMatching(String substring);
    
    /**
     * Evicts all entries from the cache.
     */
    String evictAll();
}
