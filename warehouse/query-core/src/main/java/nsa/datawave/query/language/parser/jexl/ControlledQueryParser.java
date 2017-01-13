package nsa.datawave.query.language.parser.jexl;

import java.util.Map;
import java.util.Set;

/**
 * 
 */
public interface ControlledQueryParser {
    
    public void setAllowedFields(Set<String> allowedSpcmaFields);
    
    public Set<String> getAllowedFields();
    
    public void setExcludedValues(Map<String,Set<String>> defeatedValues);
    
    public Map<String,Set<String>> getExcludedValues();
    
    public void setIncludedValues(Map<String,Set<String>> requiredValues);
    
    public Map<String,Set<String>> getIncludedValues();
    
}
