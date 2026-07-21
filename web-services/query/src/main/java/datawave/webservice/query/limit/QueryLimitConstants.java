package datawave.webservice.query.limit;

import java.util.regex.Pattern;

/**
 * Useful constants for the query limit feature.
 */
public final class QueryLimitConstants {

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

    private QueryLimitConstants() {
        throw new UnsupportedOperationException();
    }
}
