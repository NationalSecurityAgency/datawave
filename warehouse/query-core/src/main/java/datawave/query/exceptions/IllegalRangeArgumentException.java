package datawave.query.exceptions;

/**
 *
 */
public class IllegalRangeArgumentException extends RuntimeException {

    private static final long serialVersionUID = 5913184625604622415L;

    public IllegalRangeArgumentException(Exception other) {
        super(other);
    }

}
