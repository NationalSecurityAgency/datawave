package datawave.webservice.query.limit;

import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

/**
 * Useful constants for the query limit feature.
 */
public final class QueryLimiterUtils {

    /**
     * A simple asterisk may be used in configuration files as a wildcard only pattern.
     */
    public static final String ASTERISK = "*";

    /**
     * Matches against regex patterns that consist of '*' or any combination that results in a wildcard pattern.
     */
    public static final Pattern wildcardOnlyPattern = Pattern.compile("^\\*$|^(\\.\\*)+$");

    /**
     * Indicates no limit when used as a system query limit.
     */
    public static final int NO_LIMIT = -1;

    /**
     * The common namespace that Zookeeper nodes created as part of the query limit feature will be stored under.
     */
    public static final String ZOOKEEPER_NAMESPACE = "ActiveQueries";

    /**
     * The default system to use if a null or blank system is supplied by a user.
     */
    public static final String EMPTY_SYSTEM_FROM = "EMPTY_SYSTEM_FROM";

    public static String normalizeQueryId(String queryId) {
        Preconditions.checkArgument(queryId != null && !queryId.isBlank(), "queryId cannot be null or blank");
        return queryId.trim();
    }

    public static String normalizeUserDn(String userDn) {
        Preconditions.checkArgument(userDn != null && !userDn.isBlank(), "userDn cannot be null or blank");
        return userDn.trim().toLowerCase();
    }

    public static String normalizeSystem(String system) {
        return (system == null || system.isBlank()) ? EMPTY_SYSTEM_FROM : system.trim();
    }

    public static String normalizeQueryLogic(String queryLogic) {
        Preconditions.checkArgument(queryLogic != null && !queryLogic.isBlank(), "queryLogic cannot be null or blank");
        return queryLogic.trim();
    }

    private QueryLimiterUtils() {
        throw new UnsupportedOperationException();
    }
}
