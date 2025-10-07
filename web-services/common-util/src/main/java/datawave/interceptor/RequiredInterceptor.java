package datawave.interceptor;

import java.lang.annotation.Annotation;

import javax.interceptor.AroundInvoke;
import javax.interceptor.InvocationContext;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import datawave.annotation.Required;

/**
 * Used to make sure that method parameters that are annotated with the Required.class annotation are not null
 */
public class RequiredInterceptor {

    private Logger log = Logger.getLogger(this.getClass());

    @AroundInvoke
    public Object checkRequiredParameters(InvocationContext ctx) throws Exception {
        // Check to see if any of the parameters are required
        Object[] methodParams = ctx.getParameters();
        if (null != methodParams && methodParams.length > 0) {
            Annotation[][] annotations = ctx.getMethod().getParameterAnnotations();
            if (null != annotations && annotations.length > 0) {
                for (int i = 0; i < annotations.length; i++) {
                    Object methodParameter = methodParams[i];
                    Annotation[] methodAnnotations = annotations[i];
                    if (null != methodAnnotations && methodAnnotations.length > 0) {
                        // Check to see if the Required annotation exists
                        boolean required = false;
                        String paramName = null;
                        String[] validValues = null;
                        for (Annotation a : methodAnnotations) {
                            if (a.annotationType().equals(Required.class)) {
                                Required r = (Required) a;
                                paramName = r.value();
                                validValues = r.validValues();
                                required = true;
                                break;
                            }
                        }
                        log.debug("Checking parameter: " + paramName + ", value: " + methodParameter);
                        // If required and the methodParameter is null then fail
                        boolean fail = false;
                        if (required) {
                            if (null == methodParameter)
                                fail = true;
                            else if (methodParameter instanceof String && StringUtils.isEmpty((String) methodParameter))
                                fail = true;
                            else if (!isValidValuesEmpty(validValues) && !ArrayUtils.contains(validValues, methodParameter))
                                fail = true;
                        }
                        if (fail) {
                            StringBuilder errMsg = new StringBuilder();
                            errMsg.append("Parameter: '").append(paramName).append("' is required when calling '").append(ctx.getMethod()).append("'.");
                            if (null != validValues)
                                errMsg.append(" Valid values: ").append(ArrayUtils.toString(validValues));
                            String error = errMsg.toString();
                            log.error(error);
                            throw new IllegalArgumentException(error);
                        }
                    }
                }
            }
        }
        return ctx.proceed();
    }

    private boolean isValidValuesEmpty(String[] validValues) {
        if (null == validValues)
            return true;
        if (0 == validValues.length)
            return true;
        for (String value : validValues) {
            if (StringUtils.isNotBlank(value))
                return false;
        }
        return true;
    }
}
