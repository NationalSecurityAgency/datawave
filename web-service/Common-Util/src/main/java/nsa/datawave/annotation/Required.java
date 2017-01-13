package nsa.datawave.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import nsa.datawave.resteasy.util.RequiredProcessor;

import org.jboss.resteasy.annotations.StringParameterUnmarshallerBinder;

@Retention(RetentionPolicy.RUNTIME)
@StringParameterUnmarshallerBinder(RequiredProcessor.class)
@Documented
public @interface Required {
    
    String value();
    
    String[] validValues() default {};
    
}
