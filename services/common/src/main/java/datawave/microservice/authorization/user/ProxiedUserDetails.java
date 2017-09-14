package datawave.microservice.authorization.user;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUser.UserType;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link UserDetails} that represents a set of proxied users. For example, this proxied user could represent a GUI server acting on behalf of a user. The GUI
 * server user represents the entity that made the call to us, but the user is the actual end user.
 */
@XmlRootElement
public class ProxiedUserDetails implements UserDetails {
    private String username;
    private Set<DatawaveUser> proxiedUsers = new LinkedHashSet<>();
    private List<SimpleGrantedAuthority> roles;
    private long creationTime;
    
    @JsonCreator
    public ProxiedUserDetails(@JsonProperty("proxiedUsers") Collection<? extends DatawaveUser> proxiedUsers, @JsonProperty("creationTime") long creationTime) {
        this.proxiedUsers.addAll(proxiedUsers);
        this.username = this.proxiedUsers.stream().map(DatawaveUser::getName).collect(Collectors.joining(" -> "));
        this.roles = getPrimaryUser().getRoles().stream().map(SimpleGrantedAuthority::new).collect(Collectors.toList());
        this.creationTime = creationTime;
    }
    
    public Collection<? extends DatawaveUser> getProxiedUsers() {
        return Collections.unmodifiableSet(proxiedUsers);
    }
    
    /**
     * Gets the {@link DatawaveUser} that represents the primary user in this proxied entity. This is the first (and presumably only) proxied principal whose
     * {@link DatawaveUser#getUserType()} is {@link UserType#USER}. If no proxied principal is a user principal, then the first principal in the list is
     * returned.
     *
     * @return The {@link DatawaveUser} that represents the user in the list of proxied users
     */
    @JsonIgnore
    public DatawaveUser getPrimaryUser() {
        DatawaveUser first = proxiedUsers.stream().findFirst().orElse(null);
        return proxiedUsers.stream().filter(u -> u.getUserType() == UserType.USER).findFirst().orElse(first);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        
        ProxiedUserDetails that = (ProxiedUserDetails) o;
        
        if (!username.equals(that.username))
            return false;
        return proxiedUsers.equals(that.proxiedUsers);
    }
    
    @Override
    public int hashCode() {
        int result = username.hashCode();
        result = 31 * result + proxiedUsers.hashCode();
        return result;
    }
    
    @Override
    public String toString() {
        return "ProxiedUserDetails{name='" + username + "'" + ", proxiedUsers=" + proxiedUsers + "}";
    }
    
    @Override
    @JsonIgnore
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return roles;
    }
    
    @Override
    @JsonIgnore
    public String getPassword() {
        return "";
    }
    
    @Override
    @JsonIgnore
    public String getUsername() {
        return username;
    }
    
    @Override
    @JsonIgnore
    public boolean isAccountNonExpired() {
        return true;
    }
    
    @Override
    @JsonIgnore
    public boolean isAccountNonLocked() {
        return true;
    }
    
    @Override
    @JsonIgnore
    public boolean isCredentialsNonExpired() {
        return true;
    }
    
    @Override
    @JsonIgnore
    public boolean isEnabled() {
        return true;
    }
    
    public long getCreationTime() {
        return creationTime;
    }
}
