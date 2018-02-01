package datawave.webservice.geowave.auth.filter;

import org.geoserver.security.config.J2eeAuthenticationBaseFilterConfig;
import org.geoserver.security.config.SecurityAuthFilterConfig;

public class DatawaveAuthenticationFilterConfig extends J2eeAuthenticationBaseFilterConfig implements SecurityAuthFilterConfig {
    private static final long serialVersionUID = 1L;
    
    public DatawaveAuthenticationFilterConfig() {}
}
