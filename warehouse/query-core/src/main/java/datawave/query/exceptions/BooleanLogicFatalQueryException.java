package datawave.query.exceptions;

/**
 *
 *
 */
public class BooleanLogicFatalQueryException extends Exception {
    private static final long serialVersionUID = 318700258115575237L;

    private String query = "";
    private String reason = "";

    public BooleanLogicFatalQueryException(String query, String reason) {
        this.query = query;
        this.reason = reason;
    }

    @Override
    public String getMessage() {
        return super.getMessage() + " Query: " + query + "  Reason: " + reason;
    }

    @Override
    public String toString() {
        return super.toString() + " Query: " + query + "  Reason: " + reason;
    }

}
