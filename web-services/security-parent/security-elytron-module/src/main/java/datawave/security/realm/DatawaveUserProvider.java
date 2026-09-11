package datawave.security.realm;

import java.security.Key;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.X509KeyManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wildfly.security.evidence.Evidence;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;

import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.JWTTokenHandler;
import datawave.security.cert.SSLStores;
import datawave.security.evidence.JWTEvidence;
import datawave.security.evidence.TrustedHeaderEvidence;
import datawave.security.evidence.X509CertificateEvidence;
import datawave.security.system.SecurityEJBProvider;

/**
 * This class is responsible for delegating the lookup up of {@link DatawaveUser} instances to a configured {@link DatawaveUserService}.
 */
public class DatawaveUserProvider {

    private static final Logger log = LoggerFactory.getLogger(DatawaveUserProvider.class);

    private static DatawaveUserProvider instance;

    /**
     * Return a static instance of {@link DatawaveUserProvider} that is configured to use the {@link DatawaveUserService} and {@link SSLStores} provided by
     * {@link SecurityEJBUtils}.
     *
     * @return the instance
     * @throws Exception
     *             if the instance could not be created
     */
    public static DatawaveUserProvider getInstance() throws Exception {
        // If an instance has not been created yet, do so.
        if (instance == null) {
            // Fetch the EJB provider.
            SecurityEJBProvider ejbProvider;
            try {
                ejbProvider = SecurityEJBUtils.getSecurityEJBProvider();
            } catch (Exception e) {
                log.error("Failed to fetch SecurityEJBProvider", e);
                throw e;
            }

            // Ensure the EJB provider has the datawave user service and SSL keystore and truststore.
            DatawaveUserService userService = ejbProvider.getDatawaveUserService();
            SSLStores sslStores = ejbProvider.getSSLStores();
            if (userService == null) {
                throw new IllegalStateException("EJBProvider returned null user service");
            }
            if (sslStores == null) {
                throw new IllegalStateException("EJBProvider returned null ssl context");
            }

            // Create the JWT Token handler.
            JWTTokenHandler tokenHandler;
            try {
                // @formatter:off
                ObjectMapper mapper = JsonMapper.builder()
                        .enable(MapperFeature.USE_WRAPPER_NAME_AS_PROPERTY_NAME)
                        .build()
                        .registerModules(new GuavaModule())
                        .registerModules(new JaxbAnnotationModule());
                // @formatter:on
                String alias = sslStores.getKeyStore().aliases().nextElement();
                X509KeyManager keyManager = (X509KeyManager) sslStores.getKeyManagers()[0];
                X509Certificate[] certs = keyManager.getCertificateChain(alias);
                Key signingKey = keyManager.getPrivateKey(alias);

                tokenHandler = new JWTTokenHandler(certs[0], signingKey, 24, TimeUnit.HOURS, JWTTokenHandler.TtlMode.RELATIVE_TO_CURRENT_TIME, mapper);
            } catch (Exception e) {
                log.error("Failed to create JWTTokenHandler", e);
                throw e;
            }

            // Create the user provider.
            instance = new DatawaveUserProvider(userService, tokenHandler);
        }
        return instance;
    }

    private final DatawaveUserService userService;
    private final JWTTokenHandler jwtTokenHandler;

    public DatawaveUserProvider(DatawaveUserService userService, JWTTokenHandler jwtTokenHandler) {
        this.userService = userService;
        this.jwtTokenHandler = jwtTokenHandler;
    }

    /**
     * Look up and return the set of {@link DatawaveUser} associated with the users represented by the given {@link Evidence}.
     *
     * @param evidence
     *            the evidence
     * @return the user set
     * @throws Exception
     *             if an error occurred while fetching the users
     */
    public Collection<DatawaveUser> getUsers(Evidence evidence) throws Exception {
        if (evidence instanceof JWTEvidence) {
            return jwtTokenHandler.createUsersFromToken(((JWTEvidence) evidence).getToken());
        }
        if (evidence instanceof TrustedHeaderEvidence) {
            return userService.lookup(((TrustedHeaderEvidence) evidence).getEntities());
        }
        if (evidence instanceof X509CertificateEvidence) {
            return userService.lookup(((X509CertificateEvidence) evidence).getEntities());
        }

        return Set.of();
    }
}
