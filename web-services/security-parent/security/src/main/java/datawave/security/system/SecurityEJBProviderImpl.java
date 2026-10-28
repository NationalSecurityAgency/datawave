package datawave.security.system;

import javax.annotation.security.PermitAll;
import javax.ejb.Singleton;
import javax.inject.Inject;

import datawave.configuration.spring.SpringBean;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.cache.ElytronCacheManager;
import datawave.security.cert.SSLStores;

/**
 * Implementation of {@link SecurityEJBProvider} that will inject EJBs and make them accessible to the datawave elytron module. In order to allow methods on
 * this class to be invoked before authentication occurs in the elytron module, we must use {@link PermitAll}. It is expected that this singleton will be bound
 * to JNDI for lookup by the elytron module.
 */
@PermitAll
@Singleton
public class SecurityEJBProviderImpl implements SecurityEJBProvider {

    @Inject
    @SpringBean(name = "datawaveUserService")
    private DatawaveUserService datawaveUserService;

    @Inject
    @SpringBean
    private SSLStores sslStores;

    private ElytronCacheManager elytronCacheManager;

    @Override
    public DatawaveUserService getDatawaveUserService() {
        return datawaveUserService;
    }

    @Override
    public SSLStores getSSLStores() {
        return sslStores;
    }

    @Inject
    public void setElytronCacheRegister(ElytronCacheManager elytronCacheManager) {
        this.elytronCacheManager = elytronCacheManager;
    }

    @Override
    public ElytronCacheManager getElytronCacheManager() {
        return elytronCacheManager;
    }
}
