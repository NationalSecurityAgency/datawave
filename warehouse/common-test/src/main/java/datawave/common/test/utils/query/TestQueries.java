package datawave.common.test.utils.query;

public class TestQueries {
    public static final String SINGLE = "FOO == 'BAR'";
    public static final String SINGLE_MIXED_CASE = "FoO == 'nL'";
    public static final String SINGLE_AND = "FOO == 'BAR' AND BAZ == 'PLOVER'";
    public static final String SINGLE_OR = "FOO == 'BAR' OR BAZ == 'PLOVER'";
    public static final String AND_NOT = "FOO == 'BAR' AND NOT (BAZ == 'PLOVER')";
    public static final String REGEX = "FOO == 'BAR' AND BAZ =~ 'PLO.*'";
    public static final String REGEX2 = "FOO =~ '.*BAR.*'";
    public static final String NEG_REGEX = "FOO == 'BAR' and BAZ !~ '.*plov.*'";
    public static final String RANGE_QUERY = "FOO == 'BAR' and (RANGE >= '9.0' and RANGE <= '12.0')";
    public static final String RANGE_FUNCTION = "f:between(LATITUDE,9.0,12.0)";
    public static final String QUOTED_RANGE_FUNCTION = "f:between('LATITUDE',9.0,12.0)";
    public static final String NULL_QUERY = "FOO == 'BAR' or BAR == null";
    public static final String NOT_EQUALS = "FOO != 'BAR'";
    public static final String RANGE_QUERY2 = "RANGE >= '64' AND RANGE <= '65.5'";
    public static final String LEADING_WILDCARD = "FOO =~ '.*BAR'";
    public static final String TRAILING_WILDCARD = "FOO =~ 'BAR.*'";
    public static final String LEADING_MID_TRAILING_WILDCARD = "FOO =~ '.*B.*R.*'";
}
