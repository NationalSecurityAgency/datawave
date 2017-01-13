package nsa.datawave.poller.manager;

/**
 * 
 */
public class FileProcessingError extends Error {
    
    private static final long serialVersionUID = -6531919975060433697L;
    
    public FileProcessingError() {
        super();
    }
    
    public FileProcessingError(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
    
    public FileProcessingError(String message, Throwable cause) {
        super(message, cause);
    }
    
    public FileProcessingError(String message) {
        super(message);
    }
    
    public FileProcessingError(Throwable cause) {
        super(cause);
    }
    
}
