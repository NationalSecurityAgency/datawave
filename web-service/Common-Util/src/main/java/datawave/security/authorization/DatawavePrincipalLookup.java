package datawave.security.authorization;

public interface DatawavePrincipalLookup {
    enum CacheMode {
        // No entries are cached -- the authorization service is queried for single entities, and a merged principal is computed every time.
        NONE,
        // Only single entries are cached -- a merged principal will be computed every time.
        SINGLE_ENTRIES,
        // All entries are cached -- single entries as well as merged principals.
        ALL
    }
    
    DatawavePrincipal lookupPrincipal(String... dns) throws Exception;
    
    DatawavePrincipal lookupPrincipal(String dn) throws Exception;
    
    DatawavePrincipal getCurrentPrincipal();
    
    DatawavePrincipal lookupPrincipal(CacheMode cacheMode, String... dns) throws Exception;
}
