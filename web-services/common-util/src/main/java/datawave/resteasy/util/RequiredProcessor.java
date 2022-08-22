package datawave.resteasy.util;

import datawave.annotation.Required;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.jboss.resteasy.spi.StringParameterUnmarshaller;
import org.jboss.resteasy.util.FindAnnotation;

import java.lang.annotation.Annotation;

public class RequiredProcessor implements StringParameterUnmarshaller<String> {
    
    String fieldName = null;
    
    String[] validValues = null;
    
    @Override
    public String fromString(String value) {
        if (StringUtils.isBlank(value))
            throw new IllegalArgumentException("Parameter " + fieldName + " is required.");
        
        if (!(ArrayUtils.isEmpty(validValues) || ArrayUtils.contains(validValues, value)))
            throw new IllegalArgumentException("Parameter " + fieldName + " is not set to one of the required values: " + ArrayUtils.toString(validValues));
        return value;
    }
    
    @Override
    public void setAnnotations(Annotation[] ann) {
        Required r = FindAnnotation.findAnnotation(ann, Required.class);
        this.fieldName = r.value();
        this.validValues = r.validValues();
    }
    
}
