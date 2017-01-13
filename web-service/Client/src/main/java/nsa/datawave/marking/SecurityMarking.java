package nsa.datawave.marking;

import java.util.Map;

import nsa.datawave.validation.ParameterValidator;

import org.apache.accumulo.core.security.ColumnVisibility;

public interface SecurityMarking extends ParameterValidator {
    
    public ColumnVisibility toColumnVisibility() throws MarkingFunctions.Exception;
    
    public String toColumnVisibilityString();
    
    public Map<String,String> toMap();
    
    public void fromMap(Map<String,String> map);
    
    public String mapToString();
    
    public void fromString(String xmlString);
    
    public void clear();
}
