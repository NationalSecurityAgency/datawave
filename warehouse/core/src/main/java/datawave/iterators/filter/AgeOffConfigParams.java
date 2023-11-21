package datawave.iterators.filter;

public class AgeOffConfigParams {
    /**
     * {@code String } object containing the configuration option key for the default "Time to Live" (ttl) for a record in accumulo.
     */
    public static final String TTL = "ttl";

    /**
     * {@code String } object containing the configuration option key for the time units for the default "Time to Live" (ttl) for a record in accumulo. The
     * valid values for the units are {@code ms}("milliseconds"), {@code s} ("seconds"), {@code m} ("minutes"), {@code h} ("hours"), and {@code d} ("days")
     */
    public static final String TTL_UNITS = "ttlUnits";

    /**
     * {@code String } object containing the configuration option key for the "Time to Live" (ttl) short circuit for a record in accumulo. Keys that occur after
     * this the (now - ttlShortCircuit) will bypass the configured filters and will automatically be accepted.
     */
    public static final String TTL_SHORT_CIRCUIT = "ttlShortCircuit";

    /**
     * {@code String } object containing the REGEX pattern to be used by subclasses of {@code RegexFilterBase}.
     */
    public static final String MATCHPATTERN = "matchPattern";

    /**
     * {@code String } object containing the configuration option key that the configuration file for this filter is stored under. This should be a full URL
     * such as {@code file:///opt/accumulo/config/configFilter.xml} or {@code hdfs://config/filters/configFilter.xml}
     */
    public static final String FILTER_CONFIG = "configurationFilePath";

    /**
     * Object containing extended options in the event that the filter class requires them.
     */
    public static final String EXTENDED_OPTIONS = "extendedOptions";

    /**
     * A scan time parameter to indicate start the scan session. This avoids concurrency issues when scanning many tablet servers.
     */
    public static final String SCAN_START_TIMESTAMP = "scanStart";

    public static final String ONLY_ON_USER_COMPACTION = "onlyAgeOffOnUserMajc";

    /**
     * A flag indicating that the ageoff parameters are from the merging of two or more config files. Some filter parsing need special handling for this case.
     */
    public static final String IS_MERGE = "ismerge";

    /**
     * A flag that indicates that the table being aged off is an index table
     */
    public static final String IS_INDEX_TABLE = "isindextable";

    /**
     * Exclude schema components from age-off
     */
    public static final String EXCLUDE_DATA = "excludeData";
}
