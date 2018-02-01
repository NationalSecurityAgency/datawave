package datawave.webservice.geowave.auth.filter;

import org.apache.wicket.model.IModel;
import org.geoserver.security.web.auth.J2eeBaseAuthFilterPanel;

public class DatawaveAuthenticationFilterPanel extends J2eeBaseAuthFilterPanel<DatawaveAuthenticationFilterConfig> {
    public DatawaveAuthenticationFilterPanel(String id, IModel<DatawaveAuthenticationFilterConfig> model) {
        super(id, model);
    }
}
