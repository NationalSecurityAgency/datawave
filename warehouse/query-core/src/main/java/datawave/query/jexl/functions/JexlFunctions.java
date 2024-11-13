package datawave.query.jexl.functions;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This is a class annotation that is used to determine the JexlFunctionArgumentDescriptorFactory for a set of jexl functions. This is being done this was to
 * avoid the function class itself to be dependent on other jars...
 *
 *
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface JexlFunctions {
    String descriptorFactory();
}
