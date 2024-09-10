package datawave.webservice.mr.configuration;

import org.jboss.security.JSSESecurityDomain;

public interface NeedSecurityDomain {

    public void setSecurityDomain(JSSESecurityDomain jsseSecurityDomain);
}
