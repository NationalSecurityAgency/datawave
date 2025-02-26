package datawave.validation;

import java.util.List;
import java.util.Map;

public interface ParameterValidator {
    
    /**
     * 
     * @param parameters
     * @throws IllegalArgumentException
     */
    void validate(Map<String,List<String>> parameters) throws IllegalArgumentException;
}
