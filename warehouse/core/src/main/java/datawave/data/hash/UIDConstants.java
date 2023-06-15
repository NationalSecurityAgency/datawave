package datawave.data.hash;

/**
 * Constants used for [internal] UIDs
 */
public interface UIDConstants {
    /**
     * Character used for separating various parts of a UID, such as hashes and "extra" strings
     */
    char DEFAULT_SEPARATOR = '.';

    /**
     * The index of the host
     */
    String HOST_INDEX_OPT = "hostIndex";

    /**
     * The number of milliseconds in a day
     */
    int MILLISECONDS_PER_DAY = (24 * 60 * 60 * 1000);

    /**
     * The index of the process/JVM
     */
    String PROCESS_INDEX_OPT = "processIndex";

    /**
     * The one-up index of the thread
     */
    String THREAD_INDEX_OPT = "threadIndex";

    /**
     * A delimiter used to optionally identify a timestamp-based component of a UID (particularly hash-based UIDs)
     */
    char TIME_SEPARATOR = '+';

    /**
     * The type of UID to generate (default is the traditional Murmur-hash based UID)
     */
    String UID_TYPE_OPT = "uidType";

    /**
     * The base name for UID properties, as applicable, in the Hadoop configuration
     */
    String CONFIG_BASE_KEY = UID.class.getName().toLowerCase();

    /**
     * The configuration key for the machine ID value <i>required</i> for generating {@link SnowflakeUID}s. See the {@link SnowflakeUID} documentation for
     * details about this value.
     */
    String CONFIG_MACHINE_ID_KEY = CONFIG_BASE_KEY + ".machineId";

    /**
     * The configuration key for the UID type. If this property is not specified, the {@code UID.builder()} methods will return a hash-based {@link UIDBuilder}
     * by default.
     */
    String CONFIG_UID_TYPE_KEY = CONFIG_BASE_KEY + '.' + UID_TYPE_OPT;
}
