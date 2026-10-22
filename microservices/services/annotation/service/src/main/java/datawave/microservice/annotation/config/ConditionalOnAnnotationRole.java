package datawave.microservice.annotation.config;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;

import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.type.AnnotatedTypeMetadata;

@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Conditional(ConditionalOnAnnotationRole.RoleCondition.class)
public @interface ConditionalOnAnnotationRole {
    AnnotationRole[] value();

    class RoleCondition extends SpringBootCondition {
        @Override
        public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
            AnnotationRole role = AnnotationRole.from(context.getEnvironment());
            // The annotation requires value(), so its class and method metadata always contain a non-null enum array.
            // @formatter:off
            AnnotationRole[] allowedRoles = metadata.getAnnotations()
                    .get(ConditionalOnAnnotationRole.class)
                    .getEnumArray("value", AnnotationRole.class);
            // @formatter:on
            boolean matches = Arrays.asList(allowedRoles).contains(role);
            return new ConditionOutcome(matches, "annotation deployment profile is " + role.profile());
        }
    }
}
