package datawave.webservice.query.limit;

import java.util.function.Supplier;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

/**
 * Utility functions and constants for the query limiter feature.
 */
public final class QueryLimiterUtils {

    /**
     * The shared namespace for nodes created by {@link ActiveQueryTracker} in Zookeeper.
     */
    public static final String ZOOKEEPER_NAMESPACE = "ActiveQueries";

    /**
     * The root path for distinct query logics in Zookeeper.
     */
    public static final String QUERY_LOGICS_ROOT_PATH = "/queryLogics";

    /**
     * The root path for active queries.
     */
    public static final String QUERIES_ROOT_PATH = "/queries";

    /**
     * Configuration files may specify a single asterisk to use as a wildcard pattern.
     */
    public static final String SIMPLE_WILDCARD = "*";

    /**
     * Matches against regex patterns that consist of {@code *} or any combination that results in a wildcard pattern.
     */

    public static final Pattern wildcardOnlyPattern = Pattern.compile("^\\*$|^(\\.\\*)+$");

    /**
     * Implies an infinite limit when used as a limit value.
     */
    public static final int NO_LIMIT = -1;

    /**
     * The default system value to use when a null or blank system is provided.
     */
    public static final String EMPTY_SYSTEM_FROM = "EMPTY_SYSTEM_FROM";

    /**
     * Return the userDn trimmed and cast to lower case, or null if the userDn is null.
     *
     * @param userDn
     *            the user DN
     * @return the normalized value
     */
    public static String normalizeUserDn(String userDn) {
        return userDn != null ? userDn.trim().toLowerCase() : null;
    }

    /**
     * Return the system trimmed if it is not blank, or the default system {@value #EMPTY_SYSTEM_FROM} if it is null or blank.
     *
     * @param system
     *            the system
     * @return the normalized value
     */
    public static String normalizeSystem(String system) {
        return (system == null || system.isBlank()) ? EMPTY_SYSTEM_FROM : system.trim();
    }

    /**
     * Return the query logic trimmed, or null if the query logic is null.
     *
     * @param queryLogic
     *            the query logic
     * @return the normalized value
     */
    public static String normalizeQueryLogic(String queryLogic) {
        return queryLogic != null ? queryLogic.trim() : null;
    }

    /**
     * Wait for the given condition to be true, with the specified timeout in milliseconds and polling interval in millseconds.
     *
     * @param timeoutMs
     *            the timeout (0 or greater)
     * @param pollIntervalMs
     *            the poll interval (0 or greater)
     * @param condition
     *            the condition
     * @return true if the condition evaluated to true within the timeout, or false otherwise
     * @throws InterruptedException
     *             if the thread is interrupted while waiting
     */
    public static boolean await(long timeoutMs, int pollIntervalMs, Supplier<Boolean> condition) throws InterruptedException {
        Preconditions.checkArgument(timeoutMs > 0, "timeoutMs must be 0 or greater");
        Preconditions.checkArgument(pollIntervalMs > 0, "pollIntervalMs must be 0 or greater");

        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.get()) {
            if (System.currentTimeMillis() >= deadline) {
                System.out.println("Timed out waiting for condition");
                return false;
            }
            System.out.println("Sleeping for " + pollIntervalMs + " ms");
            Thread.sleep(Math.min(pollIntervalMs, deadline - System.currentTimeMillis()));
        }
        return true;
    }

    /**
     * Do not allow this class to be instantiated.
     */
    private QueryLimiterUtils() {
        throw new UnsupportedOperationException();
    }
}
