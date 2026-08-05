package datawave.webservice.query.limit;

public class ConfigurationUpdateFailedException extends QueryLimitException {
    
    public ConfigurationUpdateFailedException() {
        super();
    }
    
    public ConfigurationUpdateFailedException(String message) {
        super(message);
    }
    
    public ConfigurationUpdateFailedException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public ConfigurationUpdateFailedException(Throwable cause) {
        super(cause);
    }
    
    protected ConfigurationUpdateFailedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
