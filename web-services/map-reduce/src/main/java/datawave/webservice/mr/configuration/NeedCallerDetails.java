package datawave.webservice.mr.configuration;

import java.security.Principal;

public interface NeedCallerDetails {

    void setUserSid(String sid);

    void setPrincipal(Principal principal);
}
