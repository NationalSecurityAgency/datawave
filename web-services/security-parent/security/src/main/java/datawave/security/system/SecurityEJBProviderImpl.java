package datawave.security.system;

import javax.annotation.security.PermitAll;
import javax.ejb.Singleton;
import javax.inject.Inject;

import datawave.configuration.spring.BeanProvider;
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

    private DatawaveUserService datawaveUserService;

    private SSLStores sslStores;

    private ElytronCacheManager elytronCacheManager;

    private boolean injectedBeans = false;

    @Inject
    public void setDatawaveUserService(DatawaveUserService datawaveUserService) {
        this.datawaveUserService = datawaveUserService;
    }

    @Override
    public DatawaveUserService getDatawaveUserService() {
        injectBeans();
        return datawaveUserService;
    }

    @Inject
    public void setSSLContextInfo(SSLStores sslStores) {
        this.sslStores = sslStores;
    }

    @Override
    public SSLStores getSSLStores() {
        injectBeans();
        return sslStores;
    }

    @Inject
    public void setElytronCacheRegister(ElytronCacheManager elytronCacheManager) {
        this.elytronCacheManager = elytronCacheManager;
    }

    @Override
    public ElytronCacheManager getElytronCacheManager() {
        injectBeans();
        return elytronCacheManager;
    }

    /**
     * Injects beans here if not yet injected. This is necessary when the getters are being invoked within the external Datawave Elytron module.
     */
    private void injectBeans() {
        if (!injectedBeans) {
            BeanProvider.injectFields(this);
            injectedBeans = true;
        }
    }
}
