package nsa.datawave.configuration;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.inject.Scope;

/**
 * <p>
 * Specifies that a bean is refreshable scoped.
 * </p>
 * <p>
 * The refreshable scope is active at all times. However, firing of the CDI event {@link RefreshEvent} causes immediate destruction of the existing context and
 * recreation of a new context.
 * </p>
 */
@Scope
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD})
public @interface RefreshableScope {}
