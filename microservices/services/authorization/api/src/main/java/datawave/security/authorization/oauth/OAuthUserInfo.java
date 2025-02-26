package datawave.security.authorization.oauth;

import java.util.Collection;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Multimap;

import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;

// Purpose of this class is to override the name field in DatawaveUser
// to use the commonName instead of the subjectDN/issuerDN
public class OAuthUserInfo extends DatawaveUser {
    
    private String name;
    
    public OAuthUserInfo(DatawaveUser user) {
        super(user.getDn(), user.getUserType(), user.getEmail(), user.getAuths(), user.getRoles(), user.getRoleToAuthMapping(), user.getCreationTime(),
                        user.getExpirationTime());
        this.name = user.getCommonName();
    }
    
    @JsonCreator
    public OAuthUserInfo(@JsonProperty(value = "dn", required = true) SubjectIssuerDNPair dn,
                    @JsonProperty(value = "userType", required = true) UserType userType, @JsonProperty(value = "email", required = true) String email,
                    @JsonProperty("auths") Collection<String> auths, @JsonProperty("roles") Collection<String> roles,
                    @JsonProperty("roleToAuthMapping") Multimap<String,String> roleToAuthMapping,
                    @JsonProperty(value = "creationTime", defaultValue = "-1L") long creationTime,
                    @JsonProperty(value = "expirationTime", defaultValue = "-1L") long expirationTime) {
        super(dn, userType, email, auths, roles, roleToAuthMapping, creationTime, expirationTime);
    }
    
    @Override
    public String getName() {
        return name;
    }
}
