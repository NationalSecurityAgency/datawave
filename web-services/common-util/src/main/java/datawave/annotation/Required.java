package datawave.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import org.jboss.resteasy.annotations.StringParameterUnmarshallerBinder;

import datawave.resteasy.util.RequiredProcessor;

@Retention(RetentionPolicy.RUNTIME)
@StringParameterUnmarshallerBinder(RequiredProcessor.class)
@Documented
public @interface Required {

    String value();

    String[] validValues() default {};

}
