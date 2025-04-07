package datawave.microservice.validator;

import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import org.apache.commons.beanutils.BeanUtils;

/**
 * Implementation of {@link NotBlankIfFieldEquals} validator.
 */
public class NotBlankIfFieldEqualsValidator implements ConstraintValidator<NotBlankIfFieldEquals,Object> {
    private String fieldName;
    private String fieldSetValue;
    private String notBlankFieldName;
    
    @Override
    public void initialize(NotBlankIfFieldEquals constraintAnnotation) {
        fieldName = constraintAnnotation.fieldName();
        fieldSetValue = constraintAnnotation.fieldValue();
        notBlankFieldName = constraintAnnotation.notBlankFieldName();
    }
    
    @Override
    public boolean isValid(Object value, ConstraintValidatorContext context) {
        if (value == null) {
            return true;
        }
        
        try {
            final String fieldValue = BeanUtils.getProperty(value, fieldName);
            final String notBlankFieldValue = BeanUtils.getProperty(value, notBlankFieldName);
            if (Objects.equals(fieldValue, fieldSetValue) && (notBlankFieldValue == null || notBlankFieldValue.trim().length() == 0)) {
                context.disableDefaultConstraintViolation();
                context.buildConstraintViolationWithTemplate(context.getDefaultConstraintMessageTemplate()).addPropertyNode(notBlankFieldName)
                                .addConstraintViolation();
                return false;
            }
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        
        return true;
    }
}
