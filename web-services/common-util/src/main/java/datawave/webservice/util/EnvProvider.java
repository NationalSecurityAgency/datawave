package datawave.webservice.util;

import datawave.webservice.common.logging.ThreadConfigurableLogger;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class EnvProvider {
    
    private static final Logger log = ThreadConfigurableLogger.getLogger(EnvProvider.class);
    
    private static final String ENV_PREFIX = "env:";
    
    private EnvProvider() {}
    
    /**
     * Resolves a property with a <code>'env:'</code> prefix, or returns the original value
     * 
     * @param property
     *            the property value
     * @return the property value
     */
    public static String resolve(String property) {
        if (property != null && property.startsWith(ENV_PREFIX)) {
            String target = property.substring(4);
            if (StringUtils.isNotBlank(target)) {
                String value = System.getenv(target);
                if (StringUtils.isNotBlank(value)) {
                    log.trace("env target resolved");
                    return value;
                } else {
                    log.warn("could not resolve env target: " + target);
                }
            }
        }
        return property;
    }
}
