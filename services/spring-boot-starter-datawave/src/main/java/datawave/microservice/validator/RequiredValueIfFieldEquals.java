package datawave.microservice.validator;

import javax.validation.Constraint;
import javax.validation.Payload;
import javax.validation.ReportAsSingleViolation;
import javax.validation.constraints.NotNull;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Validates that {@code requiredValueVieldName} is set to {@code requiredValueFieldValue} if {@code fieldName} is set to {@code fieldValue}.
 */
@Documented
@Constraint(validatedBy = {RequiredValueIfFieldEqualsValidator.class})
@Target({TYPE, ANNOTATION_TYPE})
@Retention(RUNTIME)
@ReportAsSingleViolation
@NotNull
public @interface RequiredValueIfFieldEquals {
    String fieldName();
    
    String fieldValue();
    
    String requiredValueFieldName();
    
    String requiredValueFieldValue();
    
    String message() default "{RequiredValueIfFieldEquals.message}";
    
    Class<?>[] groups() default {};
    
    Class<? extends Payload>[] payload() default {};
    
    @Target({TYPE, ANNOTATION_TYPE})
    @Retention(RUNTIME)
    @Documented
    @interface List {
        RequiredValueIfFieldEquals[] value();
    }
}
