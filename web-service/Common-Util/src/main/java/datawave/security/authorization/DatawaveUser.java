package datawave.security.authorization;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import datawave.security.util.DnUtils;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * A user of a DATAWAVE service. Typically, one or more of these users (a chain where a user called an intermediate service which in turn called us) is
 * represented with a {@link DatawavePrincipal}.
 */
public class DatawaveUser implements Serializable {
    public enum UserType {
        USER, SERVER
    }
    
    public static final DatawaveUser ANONYMOUS_USER = new DatawaveUser(SubjectIssuerDNPair.of("ANONYMOUS"), UserType.USER, null, null, null, -1L);
    
    private final String name;
    private final String commonName;
    private final SubjectIssuerDNPair dn;
    private final UserType userType;
    private final Collection<String> auths;
    private final Collection<String> roles;
    private final Multimap<String,String> roleToAuthMapping;
    private final long creationTime;
    private final long expirationTime;
    
    public DatawaveUser(SubjectIssuerDNPair dn, UserType userType, Collection<String> auths, Collection<String> roles,
                    Multimap<String,String> roleToAuthMapping, long creationTime) {
        this(dn, userType, auths, roles, roleToAuthMapping, creationTime, -1L);
    }
    
    public DatawaveUser(SubjectIssuerDNPair dn, UserType userType, Collection<String> auths, Collection<String> roles,
                    Multimap<String,String> roleToAuthMapping, long creationTime, long expirationTime) {
        this.name = dn.toString();
        this.commonName = DnUtils.getCommonName(dn.subjectDN());
        this.dn = dn;
        this.userType = userType;
        this.auths = auths == null ? Collections.emptyList() : Collections.unmodifiableCollection(new LinkedHashSet<>(auths));
        this.roles = roles == null ? Collections.emptyList() : Collections.unmodifiableCollection(new LinkedHashSet<>(roles));
        this.roleToAuthMapping = roleToAuthMapping == null ? LinkedHashMultimap.create() : LinkedHashMultimap.create(roleToAuthMapping);
        this.creationTime = creationTime;
        this.expirationTime = expirationTime;
    }
    
    public String getName() {
        return name;
    }
    
    public String getCommonName() {
        return commonName;
    }
    
    public SubjectIssuerDNPair getDn() {
        return dn;
    }
    
    public UserType getUserType() {
        return userType;
    }
    
    public Collection<String> getAuths() {
        return auths == null ? Collections.emptyList() : auths;
    }
    
    public Collection<String> getRoles() {
        return roles == null ? Collections.emptyList() : roles;
    }
    
    public Multimap<String,String> getRoleToAuthMapping() {
        return roleToAuthMapping;
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
        
        DatawaveUser that = (DatawaveUser) o;
        
        return creationTime == that.creationTime && dn.equals(that.dn) && userType == that.userType && auths.equals(that.auths) && roles.equals(that.roles);
    }
    
    @Override
    public int hashCode() {
        int result = dn.hashCode();
        result = 31 * result + userType.hashCode();
        result = 31 * result + auths.hashCode();
        result = 31 * result + roles.hashCode();
        result = 31 * result + (int) (creationTime ^ (creationTime >>> 32));
        return result;
    }
    
    @Override
    public String toString() {
        return "DatawaveUser{" + "name='" + getName() + "'" + ", userType=" + getUserType() + ", auths=" + getAuths() + ", roles=" + getRoles()
                        + ", creationTime=" + getCreationTime() + "}";
    }
}
