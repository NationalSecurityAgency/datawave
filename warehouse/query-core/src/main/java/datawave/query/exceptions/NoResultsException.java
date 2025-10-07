package datawave.query.exceptions;

/**
 * A named exception to catch when, while processing/transforming/permuting an ASTJexlScript, we decide that the query cannot possibly return any results in a
 * non-error case.
 *
 * For example, if we try to perform 'anyfield' expansion on a term via the global index, and find that there are no matching fields for the term: this is a
 * case where the NoResultsException should be thrown to signify to the caller what has happened.
 */
public class NoResultsException extends DatawaveQueryException {
    private static final long serialVersionUID = 3900818570885657184L;

    public NoResultsException() {
        super();
    }

    public NoResultsException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoResultsException(String message) {
        super(message);
    }

    public NoResultsException(Throwable cause) {
        super(cause);
    }
}
