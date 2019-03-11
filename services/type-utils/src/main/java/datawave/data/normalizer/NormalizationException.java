package datawave.data.normalizer;

import java.io.Serializable;

public class NormalizationException extends Exception implements Serializable {
    
    private static final long serialVersionUID = -2700045630205135530L;
    
    public NormalizationException() {
        super();
    }
    
    public NormalizationException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public NormalizationException(String message) {
        super(message);
    }
    
    public NormalizationException(Throwable cause) {
        super(cause);
    }
    
}
