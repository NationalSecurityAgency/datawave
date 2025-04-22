package datawave.microservice.validator;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.validation.Constraint;
import javax.validation.Payload;
import javax.validation.ReportAsSingleViolation;
import javax.validation.constraints.NotNull;

/**
 * Validates that {@code notNullFieldName} is not null if {@code fieldName} is set to {@code fieldValue}.
 */
@Documented
@Constraint(validatedBy = {datawave.microservice.validator.NotBlankIfFieldEqualsValidator.class})
@Target({TYPE, ANNOTATION_TYPE})
@Retention(RUNTIME)
@ReportAsSingleViolation
@NotNull
public @interface NotBlankIfFieldEquals {
    String fieldName();
    
    String fieldValue();
    
    String notBlankFieldName();
    
    String message() default "{NotBlankIfFieldEquals.message}";
    
    Class<?>[] groups() default {};
    
    Class<? extends Payload>[] payload() default {};
    
    @Target({TYPE, ANNOTATION_TYPE})
    @Retention(RUNTIME)
    @Documented
    @interface List {
        NotBlankIfFieldEquals[] value();
    }
}
