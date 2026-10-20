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
import org.springframework.core.annotation.AnnotationAttributes;
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
            AnnotationAttributes attributes = AnnotationAttributes.fromMap(metadata.getAnnotationAttributes(ConditionalOnAnnotationRole.class.getName()));
            boolean matches = Arrays.asList((AnnotationRole[]) attributes.get("value")).contains(role);
            return new ConditionOutcome(matches, "annotation deployment profile is " + role.profile());
        }
    }
}
