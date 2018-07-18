package datawave.security.authorization;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import datawave.security.authorization.DatawaveUser.UserType;

import java.io.Serializable;

/**
 * A holder object that contains basic information about a {@link DatawaveUser}. This object is used to return information about cached user objects, where a
 * potentially large list may be returned and including all roles, authorizations, and the mapping of roles to authorizations would consume too much memory.
 *
 * @see DatawaveUser
 */
public class DatawaveUserInfo implements Serializable {
    private final SubjectIssuerDNPair dn;
    private final UserType userType;
    private final long creationTime;
    private final long expirationTime;
    
    public DatawaveUserInfo(DatawaveUser user) {
        dn = user.getDn();
        userType = user.getUserType();
        creationTime = user.getCreationTime();
        expirationTime = user.getExpirationTime();
    }
    
    @JsonCreator
    public DatawaveUserInfo(@JsonProperty(value = "dn", required = true) SubjectIssuerDNPair dn,
                    @JsonProperty(value = "userType", required = true) UserType userType,
                    @JsonProperty(value = "creationTime", required = true) long creationTime,
                    @JsonProperty(value = "expirationTime", required = true) long expirationTime) {
        this.dn = dn;
        this.userType = userType;
        this.creationTime = creationTime;
        this.expirationTime = expirationTime;
        
    }
    
    public SubjectIssuerDNPair getDn() {
        return dn;
    }
    
    public UserType getUserType() {
        return userType;
    }
    
    public long getCreationTime() {
        return creationTime;
    }
    
    public long getExpirationTime() {
        return expirationTime;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        
        DatawaveUserInfo that = (DatawaveUserInfo) o;
        
        if (creationTime != that.creationTime)
            return false;
        if (expirationTime != that.expirationTime)
            return false;
        if (!dn.equals(that.dn))
            return false;
        return userType == that.userType;
    }
    
    @Override
    public int hashCode() {
        int result = dn.hashCode();
        result = 31 * result + userType.hashCode();
        result = 31 * result + (int) (creationTime ^ (creationTime >>> 32));
        result = 31 * result + (int) (expirationTime ^ (expirationTime >>> 32));
        return result;
    }
    
    @Override
    public String toString() {
        return "DatawaveUserInfo{" + "dn=" + dn + ", userType=" + userType + ", creationTime=" + creationTime + ", expirationTime=" + expirationTime + '}';
    }
}
