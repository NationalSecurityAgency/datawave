package datawave.security.realm;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.security.system.SecurityEJBProvider;

/**
 * A utility class responsible for loading a {@link SecurityEJBProvider} instance from JNDI. Due to the constraints of Elytron, we cannot deploy the Elytron
 * custom components directly with the Datawave EAR deployment, and thus do not have injectable access to any beans defined therein. We will instead use JNDI as
 * a workaround to look up the {@link SecurityEJBProvider} bean.
 */
public final class SecurityEJBUtils {

    public static final String SECURITY_EJB_JNDI_SYSTEM_PROPERTY = "dw.security.ejb.provider.jndi";

    private static final Logger log = LoggerFactory.getLogger(SecurityEJBUtils.class);

    private static SecurityEJBProvider providerInstance;

    /**
     * Returns a {@link SecurityEJBProvider} that was loaded from the JNDI binding {@value SECURITY_EJB_JNDI_SYSTEM_PROPERTY}.
     *
     * @return the {@link SecurityEJBProvider} instance
     */
    public static SecurityEJBProvider getSecurityEJBProvider() {
        if (providerInstance == null) {
            String ejbJndi = System.getProperty(SECURITY_EJB_JNDI_SYSTEM_PROPERTY);
            try {
                InitialContext context = new InitialContext();
                providerInstance = (SecurityEJBProvider) context.lookup(ejbJndi);
                if (log.isDebugEnabled()) {
                    if (providerInstance != null) {
                        log.debug("Successfully looked up instance of {} from JNDI: {}", SecurityEJBProvider.class.getName(), ejbJndi);
                    } else {
                        log.debug("Null instance of {} loaded from JNDI: {}", SecurityEJBProvider.class.getName(), ejbJndi);
                    }
                }
            } catch (NamingException e) {
                log.error("Failed to look up instance of {} from JNDI {}", SecurityEJBProvider.class.getName(), ejbJndi, e);
                throw new RuntimeException(e);
            }
        }

        return providerInstance;
    }

    private SecurityEJBUtils() {
        throw new UnsupportedOperationException();
    }
}
