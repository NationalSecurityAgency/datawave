package datawave.security.system;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.jboss.security.AuthenticationManager;
import org.jboss.security.CacheableManager;
import org.jboss.security.JSSESecurityDomain;

import javax.annotation.Resource;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import java.security.Principal;

/**
 * A producer class for generating server-security related artifacts. For one, we produce the server DN of the server that we are running inside of. We allso
 * produce the {@link JSSESecurityDomain} for our application. We use this rather than directly injecting at each site using {@link Resource} since the producer
 * allows us to use a plain {@link Inject} annotation versus having to specify the resource name each time we inject with {@link Resource}. This way, we only
 * name the resource once.
 */
@ApplicationScoped
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class SecurityDomainProducer {
    // Allow injection of JSSESecurityDomain without having to specify the JNDI name at each injection point.
    // Instead, users can simply do:
    // @Inject private JSSESecurityDomain jsseSecurityDomain
    // and the specification of the resource location is limited to this class.
    @Produces
    @Resource(name = "java:jboss/jaas/datawave/jsse")
    private JSSESecurityDomain domain;

    @Resource(name = "java:jboss/jaas/datawave")
    private AuthenticationManager authenticationManager;

    @Produces
    @AuthorizationCache
    @SuppressWarnings("unchecked")
    public CacheableManager<Object,Principal> produceAuthManager() {
        return (authenticationManager instanceof CacheableManager) ? (CacheableManager<Object,Principal>) authenticationManager : null;
    }
}
