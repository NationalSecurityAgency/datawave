package datawave.marking;

import java.util.Map;

import datawave.validation.ParameterValidator;

import org.apache.accumulo.core.security.ColumnVisibility;

public interface SecurityMarking extends ParameterValidator {
    
    ColumnVisibility toColumnVisibility() throws MarkingFunctions.Exception;
    
    String toColumnVisibilityString();
    
    Map<String,String> toMap();
    
    void fromMap(Map<String,String> map);
    
    String mapToString();
    
    void fromString(String xmlString);
    
    void clear();
}
