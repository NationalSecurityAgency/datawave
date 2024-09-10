package datawave.query.iterator.errors;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;

public class ErrorKey extends Key {

    protected static final String ERROR_KEY_ID = ErrorKey.class.getSimpleName();

    public String message = null;

    protected ErrorType errorType = null;

    public ErrorKey(ErrorType errorType) {
        super(ERROR_KEY_ID);

        setErrorType(errorType);
    }

    private ErrorKey(Key key) {
        super(key);

        errorType = ErrorType.valueOf(key.getColumnFamily().toString().trim());
        message = key.getColumnQualifier().toString().trim();

    }

    public ErrorType getErrorType() {
        if (null == errorType) {
            errorType = ErrorType.valueOf(getColumnFamily().toString());
        }

        return errorType;
    }

    public void setErrorType(ErrorType errorType) {
        this.errorType = errorType;
        colFamily = new Text(errorType.toString().trim()).getBytes();
    }

    public void setMessage(String message) {
        colQualifier = new Text(message).getBytes();
    }

    public String getMessage() {
        if (null == message) {
            message = getColumnQualifier().toString();
        }

        return message;
    }

    public static ErrorKey getErrorKey(Key key) {
        if (key.getRow().toString().equals(ERROR_KEY_ID)) {
            return new ErrorKey(key);
        } else
            return null;
    }
}
