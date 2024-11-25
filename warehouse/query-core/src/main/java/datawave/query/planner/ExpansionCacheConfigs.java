package datawave.query.planner;

/**
 * Config object that controls if the {@link DefaultQueryPlanner} will use the {@link ExpansionCacheFactory}
 */
public class ExpansionCacheConfigs {

    private boolean cacheFieldNameLookup = false;
    private boolean cacheBoundedRangeLookup = false;
    private boolean cacheRegexLookup = false;

    public boolean isCacheFieldNameLookup() {
        return cacheFieldNameLookup;
    }

    public void setCacheFieldNameLookup(boolean cacheFieldNameLookup) {
        this.cacheFieldNameLookup = cacheFieldNameLookup;
    }

    public boolean isCacheBoundedRangeLookup() {
        return cacheBoundedRangeLookup;
    }

    public void setCacheBoundedRangeLookup(boolean cacheBoundedRangeLookup) {
        this.cacheBoundedRangeLookup = cacheBoundedRangeLookup;
    }

    public boolean isCacheRegexLookup() {
        return cacheRegexLookup;
    }

    public void setCacheRegexLookup(boolean cacheRegexLookup) {
        this.cacheRegexLookup = cacheRegexLookup;
    }
}
