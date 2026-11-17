package datawave.ingest.data.config;

public class FieldConfigHelperConstants {
    public static final String FIELD_CONFIG_FILE = ".data.category.field.config.file";

    /**
     * Prefix for the settings controlling the field lookup cache in {@link XMLFieldConfigHelper}. Both settings honor an {@code all} datatype fallback -- see
     * {@link FieldLookupCache}.
     */
    public static final String FIELD_CONFIG_CACHE = ".data.category.field.config.cache";

    /**
     * The maximum number of fields the cache may hold. When unset, the cache is unbounded -- see {@link FieldLookupCache}.
     */
    public static final String FIELD_CONFIG_CACHE_MAX_SIZE = FIELD_CONFIG_CACHE + FieldLookupCache.MAX_SIZE_SUFFIX;

    /** What a lookup does once the cache is full, one of {@link FieldLookupCache.OverflowPolicy}. */
    public static final String FIELD_CONFIG_CACHE_OVERFLOW_POLICY = FIELD_CONFIG_CACHE + FieldLookupCache.OVERFLOW_POLICY_SUFFIX;
}
