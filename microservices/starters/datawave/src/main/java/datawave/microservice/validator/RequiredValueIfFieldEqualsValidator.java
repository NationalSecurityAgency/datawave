package datawave.microservice.validator;

import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import org.apache.commons.beanutils.BeanUtils;

/**
 * Implementation of {@link RequiredValueIfFieldEquals} validator.
 */
public class RequiredValueIfFieldEqualsValidator implements ConstraintValidator<RequiredValueIfFieldEquals,Object> {
    private String fieldName;
    private String fieldSetValue;
    private String requiredValueFieldName;
    private String requiredValueFieldValue;
    
    @Override
    public void initialize(RequiredValueIfFieldEquals constraintAnnotation) {
        fieldName = constraintAnnotation.fieldName();
        fieldSetValue = constraintAnnotation.fieldValue();
        requiredValueFieldName = constraintAnnotation.requiredValueFieldName();
        requiredValueFieldValue = constraintAnnotation.requiredValueFieldValue();
    }
    
    @Override
    public boolean isValid(Object value, ConstraintValidatorContext context) {
        if (value == null) {
            return true;
        }
        
        try {
            final String fieldValue = BeanUtils.getProperty(value, fieldName);
            final String setFieldValue = BeanUtils.getProperty(value, requiredValueFieldName);
            if (Objects.equals(fieldValue, fieldSetValue) && !Objects.equals(setFieldValue, requiredValueFieldValue)) {
                context.disableDefaultConstraintViolation();
                context.buildConstraintViolationWithTemplate(context.getDefaultConstraintMessageTemplate()).addPropertyNode(requiredValueFieldName)
                                .addConstraintViolation();
                return false;
            }
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        
        return true;
    }
}
