package datawave.validation;

import javax.ws.rs.core.MultivaluedMap;

public interface ParameterValidator {
    
    /**
     * 
     * @param parameters
     * @throws IllegalArgumentException
     */
    void validate(MultivaluedMap<String,String> parameters) throws IllegalArgumentException;
}
