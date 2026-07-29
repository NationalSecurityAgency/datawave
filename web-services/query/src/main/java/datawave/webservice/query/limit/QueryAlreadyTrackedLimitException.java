package datawave.webservice.query.limit;

public class QueryAlreadyTrackedLimitException extends QueryLimitException {

    public QueryAlreadyTrackedLimitException(String queryId) {
        super("Query " + queryId + " is already being tracked");
    }
}
