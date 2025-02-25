package datawave.security.authorization;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Version 1 of DatawaveUser did not have the email or login field This class is used to suppress those fields from being serialized when returned form a v1
 * rest endpoint
 */
public class DatawaveUserV1 extends DatawaveUser {
    
    @JsonIgnore
    protected String email;
    
    @JsonIgnore
    protected String login;
    
    public DatawaveUserV1(DatawaveUser o) {
        super(o.getDn(), o.getUserType(), o.getEmail(), o.getAuths(), o.getRoles(), o.getRoleToAuthMapping(), o.getCreationTime(), o.getExpirationTime());
    }
}
