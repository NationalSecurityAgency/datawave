package datawave.webservice.query.limit;

public class ConfigurationUpdateException extends QueryLimitException {
    
    public ConfigurationUpdateException() {
        super();
    }
    
    public ConfigurationUpdateException(String message) {
        super(message);
    }
    
    public ConfigurationUpdateException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public ConfigurationUpdateException(Throwable cause) {
        super(cause);
    }
    
    protected ConfigurationUpdateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
