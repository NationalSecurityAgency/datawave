package datawave.security.authorization;

import datawave.security.authorization.DatawaveUser.UserType;
import datawave.security.util.DnUtils;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.io.Serializable;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static datawave.security.authorization.DatawaveUser.ANONYMOUS_USER;

/**
 * A {@link Principal} that represents a set of proxied {@link DatawaveUser}s. For example, this proxied user could represent a GUI server acting on behalf of a
 * user. The GUI server user represents the entity that made the call to us and the other proxied user would be the actual end user.
 */
@XmlRootElement
@XmlType(factoryMethod = "anonymous", propOrder = {"name", "proxiedUsers", "creationTime"})
@XmlAccessorType(XmlAccessType.NONE)
public class DatawavePrincipal implements Principal, Serializable {
    private final String username;
    private final DatawaveUser primaryUser;
    private final Set<DatawaveUser> proxiedUsers = new LinkedHashSet<>();
    private final long creationTime;
    
    /**
     * This constructor should not be used. It is here to allow JAX-B mapping and CDI proxying of this class.
     */
    public DatawavePrincipal() {
        this("ANONYMOUS");
    }
    
    public DatawavePrincipal(String username) {
        this.username = username;
        this.primaryUser = ANONYMOUS_USER;
        this.creationTime = System.currentTimeMillis();
    }
    
    public DatawavePrincipal(Collection<? extends DatawaveUser> proxiedUsers) {
        this(proxiedUsers, System.currentTimeMillis());
    }
    
    public DatawavePrincipal(Collection<? extends DatawaveUser> proxiedUsers, long creationTime) {
        this.proxiedUsers.addAll(proxiedUsers);
        this.username = this.proxiedUsers.stream().map(DatawaveUser::getName).collect(Collectors.joining(" -> "));
        this.creationTime = creationTime;
        
        DatawaveUser first = this.proxiedUsers.stream().findFirst().orElse(null);
        this.primaryUser = this.proxiedUsers.stream().filter(u -> u.getUserType() == UserType.USER).findFirst().orElse(first);
    }
    
    public Set<DatawaveUser> getProxiedUsers() {
        return Collections.unmodifiableSet(proxiedUsers);
    }
    
    public DatawaveUser getPrimaryUser() {
        return primaryUser;
    }
    
    public Collection<? extends Collection<String>> getAuthorizations() {
        return Collections.unmodifiableCollection(proxiedUsers.stream().map(DatawaveUser::getAuths).collect(Collectors.toList()));
    }
    
    public String[] getDNs() {
        return proxiedUsers.stream().map(DatawaveUser::getDn).map(SubjectIssuerDNPair::toString).toArray(String[]::new);
    }
    
    public long getCreationTime() {
        return creationTime;
    }
    
    @Override
    public String getName() {
        return username;
    }
    
    public String getShortName() {
        return DnUtils.getShortName(getPrimaryUser().getName());
    }
    
    public SubjectIssuerDNPair getUserDN() {
        return getPrimaryUser().getDn();
    }
    
    public Set<String> getProxyServers() {
        
        // @formatter:off
        Set<String> proxyServers = getProxiedUsers().stream()
                .filter(u -> u.getUserType() == UserType.SERVER)
                .map(DatawaveUser::getDn)
                .map(SubjectIssuerDNPair::subjectDN)
                .collect(Collectors.toSet());
        // @formatter:on
        return proxyServers.isEmpty() ? null : proxyServers;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        
        DatawavePrincipal that = (DatawavePrincipal) o;
        
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
        return "DatawavePrincipal{" + "name='" + username + "'" + ", proxiedUsers=" + proxiedUsers + "}";
    }
    
    public static DatawavePrincipal anonymousPrincipal() {
        return new DatawavePrincipal("ANONYMOUS");
    }
    
}
