package datawave.query.planner;

import datawave.query.jexl.lookups.cache.BoundedRangeLookupCache;
import datawave.query.jexl.lookups.cache.FieldNameLookupCache;
import datawave.query.jexl.lookups.cache.RegexLookupCache;

/**
 * A singleton bean used across instances of {@link DefaultQueryPlanner} to record the result of index expansions
 */
public class ExpansionCacheFactory {

    private int fieldNameCacheSize;
    private int fieldNameExpireAfterWriteMinutes;
    private int fieldNameExpireAfterAccessMinutes;

    private int boundedRangeCacheSize;
    private int boundedRangeExpireAfterWriteMinutes;
    private int boundedRangeExpireAfterAccessMinutes;

    private int regexCacheSize;
    private int regexExpireAfterWriteMinutes;
    private int regexExpireAfterAccessMinutes;

    private final FieldNameLookupCache fieldNameLookupCache;
    private final BoundedRangeLookupCache rangeLookupCache;
    private final RegexLookupCache regexLookupCache;

    private ExpansionCacheFactory() {
        fieldNameLookupCache = new FieldNameLookupCache(fieldNameCacheSize, fieldNameExpireAfterWriteMinutes, fieldNameExpireAfterAccessMinutes);
        rangeLookupCache = new BoundedRangeLookupCache(boundedRangeCacheSize, boundedRangeExpireAfterWriteMinutes, boundedRangeExpireAfterAccessMinutes);
        regexLookupCache = new RegexLookupCache(regexCacheSize, regexExpireAfterWriteMinutes, regexExpireAfterAccessMinutes);
    }

    public FieldNameLookupCache getFieldNameLookupCache() {
        return fieldNameLookupCache;
    }

    public BoundedRangeLookupCache getRangeLookupCache() {
        return rangeLookupCache;
    }

    public RegexLookupCache getRegexLookupCache() {
        return regexLookupCache;
    }

    public int getFieldNameCacheSize() {
        return fieldNameCacheSize;
    }

    public void setFieldNameCacheSize(int fieldNameCacheSize) {
        this.fieldNameCacheSize = fieldNameCacheSize;
    }

    public int getFieldNameExpireAfterWriteMinutes() {
        return fieldNameExpireAfterWriteMinutes;
    }

    public void setFieldNameExpireAfterWriteMinutes(int fieldNameExpireAfterWriteMinutes) {
        this.fieldNameExpireAfterWriteMinutes = fieldNameExpireAfterWriteMinutes;
    }

    public int getFieldNameExpireAfterAccessMinutes() {
        return fieldNameExpireAfterAccessMinutes;
    }

    public void setFieldNameExpireAfterAccessMinutes(int fieldNameExpireAfterAccessMinutes) {
        this.fieldNameExpireAfterAccessMinutes = fieldNameExpireAfterAccessMinutes;
    }

    public int getBoundedRangeCacheSize() {
        return boundedRangeCacheSize;
    }

    public void setBoundedRangeCacheSize(int boundedRangeCacheSize) {
        this.boundedRangeCacheSize = boundedRangeCacheSize;
    }

    public int getBoundedRangeExpireAfterWriteMinutes() {
        return boundedRangeExpireAfterWriteMinutes;
    }

    public void setBoundedRangeExpireAfterWriteMinutes(int boundedRangeExpireAfterWriteMinutes) {
        this.boundedRangeExpireAfterWriteMinutes = boundedRangeExpireAfterWriteMinutes;
    }

    public int getBoundedRangeExpireAfterAccessMinutes() {
        return boundedRangeExpireAfterAccessMinutes;
    }

    public void setBoundedRangeExpireAfterAccessMinutes(int boundedRangeExpireAfterAccessMinutes) {
        this.boundedRangeExpireAfterAccessMinutes = boundedRangeExpireAfterAccessMinutes;
    }

    public int getRegexCacheSize() {
        return regexCacheSize;
    }

    public void setRegexCacheSize(int regexCacheSize) {
        this.regexCacheSize = regexCacheSize;
    }

    public int getRegexExpireAfterWriteMinutes() {
        return regexExpireAfterWriteMinutes;
    }

    public void setRegexExpireAfterWriteMinutes(int regexExpireAfterWriteMinutes) {
        this.regexExpireAfterWriteMinutes = regexExpireAfterWriteMinutes;
    }

    public int getRegexExpireAfterAccessMinutes() {
        return regexExpireAfterAccessMinutes;
    }

    public void setRegexExpireAfterAccessMinutes(int regexExpireAfterAccessMinutes) {
        this.regexExpireAfterAccessMinutes = regexExpireAfterAccessMinutes;
    }
}
