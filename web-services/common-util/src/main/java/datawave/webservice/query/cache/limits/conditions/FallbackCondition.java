package datawave.webservice.query.cache.limits.conditions;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class FallbackCondition implements Condition {
    @Override
    public boolean matches(ConditionContext conditionContext, AnnotatedTypeMetadata annotatedTypeMetadata) {
        final String type = conditionContext.getEnvironment().getProperty("dnLimitStore.type");
        return type == null || type.isEmpty() || type.equalsIgnoreCase("memory")
                        || (!"infinispan".equalsIgnoreCase(type) && !"zookeeper".equalsIgnoreCase(type));
    }
}
