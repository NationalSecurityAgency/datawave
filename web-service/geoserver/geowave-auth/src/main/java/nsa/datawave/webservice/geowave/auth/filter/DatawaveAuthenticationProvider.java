package nsa.datawave.webservice.geowave.auth.filter;

import com.thoughtworks.xstream.XStream;
import org.geoserver.config.util.XStreamPersister;
import org.geoserver.security.config.SecurityNamedServiceConfig;
import org.geoserver.security.filter.AbstractFilterProvider;
import org.geoserver.security.filter.GeoServerSecurityFilter;

public class DatawaveAuthenticationProvider extends AbstractFilterProvider {
    @Override
    public void configure(XStreamPersister xp) {
        super.configure(xp);
        XStream xs = xp.getXStream();
        xs.alias("datawaveAuthentication", DatawaveAuthenticationFilterConfig.class);
        xs.allowTypes(new Class[] {DatawaveAuthenticationFilterConfig.class});
    }
    
    @Override
    public Class<? extends GeoServerSecurityFilter> getFilterClass() {
        return DatawaveAuthenticationFilter.class;
    }
    
    @Override
    public GeoServerSecurityFilter createFilter(SecurityNamedServiceConfig config) {
        return new DatawaveAuthenticationFilter();
    }
}
