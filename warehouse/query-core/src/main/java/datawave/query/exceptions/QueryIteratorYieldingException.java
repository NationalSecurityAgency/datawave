package datawave.query.exceptions;

/**
 * Exception thrown if the query stack yields
 */
public class QueryIteratorYieldingException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public QueryIteratorYieldingException() {}

    public QueryIteratorYieldingException(String msg) {
        super(msg);
    }
}
