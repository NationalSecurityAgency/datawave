package datawave.security.system;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import datawave.security.authorization.DatawavePrincipal;
import datawave.security.user.UserOperationsBean;

/**
 * A producer class for generating server-security related artifacts. For one, we produce the server DN of the server that we are running inside of.
 */
@ApplicationScoped
public class CallerPrincipalProducer {

    @Inject
    private UserOperationsBean userOperationsBean;

    /**
     * Produces a {@link DatawavePrincipal} that is {@link RequestScoped}. This is the principal of the calling user--that is, the principal that is available
     * from the {@link javax.ejb.EJBContext} of an EJB.
     *
     * @return the principal of the calling user
     */
    @Produces
    @CallerPrincipal
    @RequestScoped
    public DatawavePrincipal produceCallerPrincipal() {
        DatawavePrincipal dp = userOperationsBean.getCurrentPrincipal();
        return dp == null ? DatawavePrincipal.anonymousPrincipal() : dp;
    }
}
