package datawave.webservice.query.limit;

public class QueryAlreadyTrackedException extends QueryLimiterException {

    public QueryAlreadyTrackedException(String queryId) {
        super("Query " + queryId + " is already being tracked");
    }
}
