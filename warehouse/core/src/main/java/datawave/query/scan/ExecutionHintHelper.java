package datawave.query.scan;

import java.util.Map;

import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class that bridges the execution hints and consistency levels configured in the ShardQueryLogic with a specific context.
 * <p>
 * The shard query logic is configured with multiple execution hint maps that are keyed to a context. The context is often a table name, however the context
 * could also be based on a stage of query planning, or even an entire class of query logic.
 * <p>
 * For example, scan resources may be reserved for a specific query logic.
 * <p>
 * In addition to execution hints there is also support for selecting a consistency level from a configuration map.
 */
public class ExecutionHintHelper {

    private static final Logger log = LoggerFactory.getLogger(ExecutionHintHelper.class);

    public static String SCAN_TYPE = "scan_type";
    public static String PRIORITY = "priority";

    private ExecutionHintHelper() {
        // enforce static access
    }

    /**
     * Get the execution hint map based on a key. If no execution hints are configured for the primary key, the secondary key is used.
     * <p>
     * The key is usually a table name, but in some cases may be tied to a particular query execution stage (e.g., index expansion).
     *
     * @param primary
     *            the primary key
     * @param secondary
     *            the secondary key, used if the primary key is not found
     * @param hintMap
     *            the map of execution hints, organized by key
     * @return the map of execution hints, or null no hint matches the primary or secondary key
     */
    public static Map<String,String> getExecutionHints(String primary, String secondary, Map<String,Map<String,String>> hintMap) {
        Map<String,String> hints = getExecutionHints(primary, hintMap);

        if (hints == null) {
            hints = getExecutionHints(secondary, hintMap);
        }

        return hints;
    }

    /**
     * Get the execution hint map for the provided key. If no execution hints are configured for the specified key a warning is logged.
     *
     * @param key
     *            the key
     * @param hintMap
     *            a map of execution hints
     * @return a map of execution hints, or null if no hint matches the provided key
     */
    public static Map<String,String> getExecutionHints(String key, Map<String,Map<String,String>> hintMap) {
        if (hintMap == null) {
            log.warn("Execution hints is null");
            return null;
        }
        Map<String,String> hints = hintMap.get(key);
        if (hints == null) {
            log.warn("No execution hints found for key {}", key);
        }
        return hints;
    }

    /**
     * Get the <code>scan_type</code> hint from the map of execution hints. A warning is logged if no scan type is found.
     *
     * @param executionHints
     *            the map of execution hints
     * @return the scan type, or null if no scan type is configured
     */
    public static String getScanType(Map<String,String> executionHints) {
        if (executionHints == null) {
            log.warn("Execution hints is null");
            return null;
        }

        String scanType = executionHints.get(SCAN_TYPE);
        if (scanType == null) {
            log.warn("No scan type found");
        }
        return scanType;
    }

    /**
     * Get the scan priority from the map of execution hints. A warning is logged if no priority is found
     *
     * @param executionHints
     *            the map of execution hints
     * @return the scan priority, or {@link Integer#MAX_VALUE} if no priority is configured
     */
    public static int getPriority(Map<String,String> executionHints) {
        if (executionHints == null) {
            log.warn("Execution hints is null");
            return Integer.MAX_VALUE;
        }

        String value = executionHints.get(PRIORITY);
        if (value == null) {
            log.warn("No priority found");
            return Integer.MAX_VALUE;
        }
        return Integer.parseInt(value);
    }

    /**
     * Get the consistency level for the provided key
     *
     * @param key
     *            the key
     * @param levels
     *            a map of consistency levels
     * @return the consistency level, or null if no configured consistency level exists
     */
    public static ConsistencyLevel getConsistencyLevel(String key, Map<String,ConsistencyLevel> levels) {
        if (levels == null) {
            log.warn("ConsistencyLevels is null");
            return null;
        }

        ConsistencyLevel consistencyLevel = levels.get(key);
        if (consistencyLevel == null) {
            log.warn("No consistency level found for key {}", key);
        }
        return consistencyLevel;
    }
}
