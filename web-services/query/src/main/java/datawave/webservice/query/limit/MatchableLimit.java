package datawave.webservice.query.limit;

public class MatchableLimit implements Comparable<MatchableLimit> {

    private final String pattern;
    private final Matcher matcher;
    private final int queryLimit;

    public MatchableLimit(String pattern, int queryLimit, long maxCacheSize) {
        this.pattern = pattern;
        this.matcher = Matcher.getMatcher(pattern, maxCacheSize);
        this.queryLimit = queryLimit;
    }

    public String getPattern() {
        return pattern;
    }

    public Matcher getMatcher() {
        return matcher;
    }

    public int getQueryLimit() {
        return queryLimit;
    }

    @Override
    public int compareTo(MatchableLimit o) {
        // First sort by the matcher type, sorting in order EXACT, PARTIAL, then ALL.
        int comparison = matcher.getType().compareTo(o.matcher.getType());

        // Then sort by the query limit from lowest to highest.
        if (comparison == 0) {
            comparison = Integer.compare(queryLimit, o.queryLimit);
        }

        // Finally, compare the pattern string.
        if (comparison == 0) {
            comparison = pattern.compareTo(o.pattern);
        }

        return comparison;
    }
}
