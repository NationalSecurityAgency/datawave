package datawave.microservice.authorization.oauth;

import java.io.Serializable;

import datawave.microservice.authorization.user.DatawaveUserDetails;

public class AuthorizationRequest implements Serializable {
    private static final long serialVersionUID = -6916962468855241150L;
    
    private DatawaveUserDetails datawaveUserDetails;
    private AuthorizedClient authorizedClient;
    private String redirect_uri;
    
    public AuthorizationRequest(DatawaveUserDetails datawaveUserDetails, AuthorizedClient authorizedClient, String redirect_uri) {
        this.datawaveUserDetails = datawaveUserDetails;
        this.authorizedClient = authorizedClient;
        this.redirect_uri = redirect_uri;
    }
    
    public DatawaveUserDetails getDatawaveUserDetails() {
        return datawaveUserDetails;
    }
    
    public void setDatawaveUserDetails(DatawaveUserDetails datawaveUserDetails) {
        this.datawaveUserDetails = datawaveUserDetails;
    }
    
    public void setAuthorizedClient(AuthorizedClient authorizedClient) {
        this.authorizedClient = authorizedClient;
    }
    
    public AuthorizedClient getAuthorizedClient() {
        return authorizedClient;
    }
    
    public void setRedirect_uri(String redirect_uri) {
        this.redirect_uri = redirect_uri;
    }
    
    public String getRedirect_uri() {
        return redirect_uri;
    }
}
