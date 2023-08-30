package datawave.configuration.spring;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.enterprise.util.Nonbinding;
import javax.inject.Qualifier;

@Qualifier
@Documented
@Target({TYPE, FIELD, METHOD, PARAMETER})
@Retention(RUNTIME)
public @interface SpringBean {
    /**
     * The name of the Spring bean to inject. If left to its default value, then injection will be performed by type rather than bean name.
     *
     * @return the name of the spring bean
     */
    String name() default "";

    @Nonbinding
    boolean required() default true;

    @Nonbinding
    boolean refreshable() default false;
}
