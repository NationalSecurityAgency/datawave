package datawave.microservice.cached;

import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.autoconfigure.condition.ConditionMessage;
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.bind.RelaxedPropertyResolver;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.core.type.ClassMetadata;

/**
 * A cache condition that matches either when the cache type is automatic (in which case we'd choose Hazelcast since it is on the classpath) or when the cache
 * type is specifically set to Hazelcast. The purpose of this is to exclude Hazelcast client auto-configuration when an alternate cache type is selected by the
 * user.
 */
public class HazelcastCacheCondition extends SpringBootCondition {
    public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
        String sourceClass = "";
        if (metadata instanceof ClassMetadata) {
            sourceClass = ((ClassMetadata) metadata).getClassName();
        }
        ConditionMessage.Builder message = ConditionMessage.forCondition("Cache", sourceClass);
        RelaxedPropertyResolver resolver = new RelaxedPropertyResolver(context.getEnvironment(), "spring.cache.");
        ConditionOutcome outcome;
        if (!resolver.containsProperty("type")) {
            outcome = ConditionOutcome.match(message.because("automatic cache type"));
        } else {
            CacheType cacheType = CacheType.HAZELCAST;
            String value = resolver.getProperty("type").replace('-', '_').toUpperCase();
            if (value.equals(cacheType.name())) {
                outcome = ConditionOutcome.match(message.because(value + " cache type"));
            } else {
                outcome = ConditionOutcome.noMatch(message.because(value + " cache type"));
            }
        }
        return outcome;
    }
}
