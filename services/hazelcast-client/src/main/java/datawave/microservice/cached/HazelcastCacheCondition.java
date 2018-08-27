package datawave.microservice.cached;

import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.boot.autoconfigure.condition.ConditionMessage;
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.context.properties.bind.BindException;
import org.springframework.boot.context.properties.bind.BindResult;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
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
        Environment environment = context.getEnvironment();
        ConditionOutcome outcome;
        try {
            BindResult<CacheType> specified = Binder.get(environment).bind("spring.cache.type", CacheType.class);
            if (!specified.isBound()) {
                outcome = ConditionOutcome.match(message.because("automatic cache type"));
            } else {
                CacheType required = CacheType.HAZELCAST;
                if (specified.get() == required) {
                    outcome = ConditionOutcome.match(message.because(specified.get() + " cache type"));
                } else {
                    outcome = ConditionOutcome.noMatch(message.because(specified.get() + " cache type"));
                }
            }
        } catch (BindException ex) {
            outcome = ConditionOutcome.noMatch(message.because("unknown cache type"));
        }
        return outcome;
    }
}
