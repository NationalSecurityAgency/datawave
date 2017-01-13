package nsa.datawave.webservice.geowave.auth.filter;

import org.geoserver.security.web.auth.AuthenticationFilterPanelInfo;

public class DatawaveAuthenticationFilterPanelInfo extends AuthenticationFilterPanelInfo<DatawaveAuthenticationFilterConfig,DatawaveAuthenticationFilterPanel> {
    public DatawaveAuthenticationFilterPanelInfo() {
        this.setComponentClass(DatawaveAuthenticationFilterPanel.class);
        this.setServiceClass(DatawaveAuthenticationFilter.class);
        this.setServiceConfigClass(DatawaveAuthenticationFilterConfig.class);
        this.setPriority(0);
    }
}
