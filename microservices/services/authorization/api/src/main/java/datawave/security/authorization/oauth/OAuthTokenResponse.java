package datawave.security.authorization.oauth;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class OAuthTokenResponse {
    
    private String id_token;
    private String access_token;
    private String refresh_token;
    private String expires_in;
    private String token_type = "Bearer";
    
    @JsonCreator
    public OAuthTokenResponse(@JsonProperty(value = "id_token", required = true) String id_token,
                    @JsonProperty(value = "access_token", required = true) String access_token,
                    @JsonProperty(value = "refresh_token", required = true) String refresh_token,
                    @JsonProperty(value = "expires_in", required = true) long expires_in) {
        this.id_token = id_token;
        this.access_token = access_token;
        this.refresh_token = refresh_token;
        this.expires_in = Long.toString(expires_in);
    }
    
    public void setId_token(String id_token) {
        this.id_token = id_token;
    }
    
    public String getId_token() {
        return id_token;
    }
    
    public void setAccess_token(String access_token) {
        this.access_token = access_token;
    }
    
    public String getAccess_token() {
        return access_token;
    }
    
    public void setRefresh_token(String refresh_token) {
        this.refresh_token = refresh_token;
    }
    
    public String getRefresh_token() {
        return refresh_token;
    }
    
    public void setExpires_in(String expires_in) {
        this.expires_in = expires_in;
    }
    
    public String getExpires_in() {
        return expires_in;
    }
    
    public void setToken_type(String token_type) {
        this.token_type = token_type;
    }
    
    public String getToken_type() {
        return token_type;
    }
}
