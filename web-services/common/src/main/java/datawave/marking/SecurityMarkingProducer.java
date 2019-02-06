package datawave.marking;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

/**
 * A CDI producer class whose purposes is to produce an implementation of {@link SecurityMarking}. The default implementation is not in a library that is marked
 * as a bean archive, so it must be explicitly produced.
 */
@ApplicationScoped
public class SecurityMarkingProducer {
    @Produces
    public SecurityMarking columnVisibilitySecurityMarking() {
        return new ColumnVisibilitySecurityMarking();
    }
}
