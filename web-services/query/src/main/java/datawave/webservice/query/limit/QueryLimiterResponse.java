package datawave.webservice.query.limit;

import java.util.Objects;
import java.util.StringJoiner;

/**
 * Response originating from {@link QueryLimiter}.
 */
public class QueryLimiterResponse {

    private static final QueryLimiterResponse NOT_MET_LIMIT_INSTANCE = new QueryLimiterResponse(false, null);

    public static QueryLimiterResponse hasNotMetLimit() {
        return NOT_MET_LIMIT_INSTANCE;
    }

    public static QueryLimiterResponse metLimit(String message) {
        return new QueryLimiterResponse(true, message);
    }

    private final boolean metLimit;
    private final String message;

    public QueryLimiterResponse(boolean metLimit, String message) {
        this.metLimit = metLimit;
        this.message = message;
    }

    public boolean metLimit() {
        return metLimit;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryLimiterResponse that = (QueryLimiterResponse) o;
        return metLimit == that.metLimit && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metLimit, message);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", QueryLimiterResponse.class.getSimpleName() + "[", "]").add("metLimit=" + metLimit).add("message='" + message + "'")
                        .toString();
    }
}
