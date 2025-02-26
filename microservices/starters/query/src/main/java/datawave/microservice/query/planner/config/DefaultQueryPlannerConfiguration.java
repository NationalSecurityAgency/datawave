package datawave.microservice.query.planner.config;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.type.AnnotatedTypeMetadata;

import datawave.query.planner.rules.NodeTransformRule;
import datawave.query.planner.rules.RegexDotallTransformRule;
import datawave.query.planner.rules.RegexPushdownTransformRule;
import datawave.query.planner.rules.RegexSimplifierTransformRule;

@Configuration
public class DefaultQueryPlannerConfiguration {
    @Bean
    @ConfigurationProperties(prefix = DefaultQueryPlannerProperties.PREFIX)
    public DefaultQueryPlannerProperties defaultQueryPlannerProperties() {
        return new DefaultQueryPlannerProperties();
    }
    
    @Bean
    @ConfigurationProperties(prefix = "datawave.query.planner.transform-rules.regex-pushdown-transform-rule")
    public TransformRuleProperties regexPushdownTransformRuleProperties() {
        return new TransformRuleProperties();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    @ConditionalOnRequestedTransformRule("RegexPushdownTransformRule")
    public RegexPushdownTransformRule RegexPushdownTransformRule() {
        RegexPushdownTransformRule rule = new RegexPushdownTransformRule();
        rule.setRegexPatterns(regexPushdownTransformRuleProperties().getRegexPatterns());
        return rule;
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    @ConditionalOnRequestedTransformRule("RegexDotallTransformRule")
    public RegexDotallTransformRule RegexDotallTransformRule() {
        return new RegexDotallTransformRule();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    @ConditionalOnRequestedTransformRule("RegexSimplifierTransformRule")
    public RegexSimplifierTransformRule RegexSimplifierTransformRule() {
        return new RegexSimplifierTransformRule();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public List<NodeTransformRule> defaultQueryPlannerNodeTransformRules(Map<String,NodeTransformRule> nodeTransformRuleBeans) {
        List<NodeTransformRule> requestedNodeTransformRules = new ArrayList<>();
        for (String requestedTransformRule : defaultQueryPlannerProperties().getRequestedTransformRules()) {
            if (nodeTransformRuleBeans.containsKey(requestedTransformRule)) {
                requestedNodeTransformRules.add(nodeTransformRuleBeans.get(requestedTransformRule));
            }
        }
        return requestedNodeTransformRules;
    }
    
    @Target({ElementType.TYPE, ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    @Conditional(NodeTransformRuleCondition.class)
    public @interface ConditionalOnRequestedTransformRule {
        String value();
    }
    
    public static class NodeTransformRuleCondition implements Condition {
        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            Map<String,Object> attributes = metadata.getAnnotationAttributes(ConditionalOnRequestedTransformRule.class.getName());
            String value = (String) attributes.get("value");
            DefaultQueryPlannerProperties defaultQueryPlannerProperties = Binder.get(context.getEnvironment())
                            .bind(DefaultQueryPlannerProperties.PREFIX, DefaultQueryPlannerProperties.class).orElse(null);
            return value != null && defaultQueryPlannerProperties != null && defaultQueryPlannerProperties.getRequestedTransformRules().contains(value);
        }
    }
}
