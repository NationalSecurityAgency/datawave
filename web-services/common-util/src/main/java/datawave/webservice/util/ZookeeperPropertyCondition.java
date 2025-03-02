package datawave.webservice.util;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class ZookeeperPropertyCondition implements Condition {
    @Override
    public boolean matches(ConditionContext conditionContext, AnnotatedTypeMetadata annotatedTypeMetadata) {
        final String zkProperty = conditionContext.getEnvironment().getProperty("dw.warehouse.zookeepers");
        return zkProperty != null && !zkProperty.isEmpty();
    }
}
