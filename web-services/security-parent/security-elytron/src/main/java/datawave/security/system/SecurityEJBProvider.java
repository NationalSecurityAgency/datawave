package datawave.security.system;

import datawave.security.authorization.DatawaveUserService;
import datawave.security.cache.ElytronCacheManager;
import datawave.security.cert.SSLStores;

/**
 * Defines methods for providing EJBs that need to be accessed when performing authentication via Wildfly in the datawave elytron module. Wildfly does not
 * support EJB injection within security components such as elytron security realms. It is expected that an instance of this class will be implemented and bound
 * to JNDI for lookup in the datawave elytron module.
 */
public interface SecurityEJBProvider {

    /**
     * Return a {@link DatawaveUserService}
     *
     * @return the user service
     */
    DatawaveUserService getDatawaveUserService();

    /**
     * Return a {@link SSLStores}
     *
     * @return the SSL context
     */
    SSLStores getSSLStores();

    /**
     * Return a {@link ElytronCacheManager}
     *
     * @return the cache collection
     */
    ElytronCacheManager getElytronCacheManager();
}
