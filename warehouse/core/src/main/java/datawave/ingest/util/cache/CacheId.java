package datawave.ingest.util.cache;

/**
 * This class is meant to be an identifier for the meta data caching mechanism.
 */
public class CacheId {

    protected String cacheId = null;

    public CacheId(String cacheId) {
        this.cacheId = cacheId;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CacheId) {
            CacheId otherCache = CacheId.class.cast(obj);

            return cacheId.equals(otherCache.cacheId);
        }
        return false;
    }

    @Override
    public String toString() {
        return cacheId;
    }

    @Override
    public int hashCode() {
        return 31 + cacheId.hashCode();
    }
}
