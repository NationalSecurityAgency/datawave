package datawave.webservice.query.limit;

import java.util.regex.Pattern;

public class QueryLimitConstants {

    public static final String ASTERISK = "*";

    // Matches against regex patterns that consist of '*' or any combination that results in a wildcard pattern.
    public static final Pattern wildcardOnlyPattern = Pattern.compile("^\\*$|^(\\.\\*)+$");
}
