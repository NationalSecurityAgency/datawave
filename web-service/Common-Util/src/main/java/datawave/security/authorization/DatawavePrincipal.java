package datawave.security.authorization;

import java.io.Serializable;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.security.util.DnUtils;
import datawave.webservice.HtmlProvider;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

@XmlRootElement
@XmlType(factoryMethod = "emptyPrincipal", propOrder = {"shortName", "userDN", "name", "DNs", "userRoles", "authorizations"})
@XmlAccessorType(XmlAccessType.NONE)
public class DatawavePrincipal implements Principal, Serializable, HtmlProvider {
    private static final long serialVersionUID = 1149052328073983085L;
    private static final String TITLE = "Credentials", EMPTY = "";
    
    public enum UserType {
        USER, SERVER
    }
    
    @XmlElement
    private SubjectIssuerDNPair userDN;
    
    @XmlElement
    private List<SubjectIssuerDNPair> proxiedEntities;
    
    private String[] dns;
    
    @XmlElement
    private String name;
    
    @XmlElement
    private String shortName;
    
    @XmlElement
    private UserType userType;
    
    @XmlJavaTypeAdapter(AuthsAdapter.class)
    private Map<String,Collection<String>> authorizations;
    
    @XmlJavaTypeAdapter(RolesAdapter.class)
    private Map<String,Collection<String>> userRoles;
    
    @XmlTransient
    private List<String> roleSets;
    
    @XmlTransient
    private Multimap<String,String> roleToAuthMapping = HashMultimap.create();
    
    /**
     * This constructor should not be used normally. It must be public in order for CDI to proxy this object and for JAX-B to properly serialize/de-serialize
     * it.
     */
    public DatawavePrincipal() {}
    
    /**
     * This constructor is only to be used by certain login modules that need to create a principal based on a username. Anyone implmenting a
     * DatawavePrincipalService should use one of the other constructors which specify a user type.
     *
     * @param username
     *            the username, which is assumed to be a DN in the form {@code subjectDN<issuerDN>[<proxiedSubjectDN><proxiedIssuerDN>...]}
     */
    public DatawavePrincipal(String username) {
        this.name = username;
        this.dns = DnUtils.splitProxiedSubjectIssuerDNs(username);
        
        String udn = DnUtils.getUserDN(dns, true);
        if (udn == null)
            udn = dns[0];
        userType = DnUtils.isServerDN(udn) ? UserType.SERVER : UserType.USER;
        for (int i = 0; i < dns.length; i += 2) {
            if (dns[i].equals(udn)) {
                userDN = SubjectIssuerDNPair.of(udn, dns[i + 1]);
                shortName = DnUtils.getShortName(udn);
                break;
            }
        }
        
        if (dns.length > 2) {
            proxiedEntities = new ArrayList<>();
            for (int i = 0; i < dns.length; i += 2) {
                if (!dns[i].equals(udn)) {
                    proxiedEntities.add(SubjectIssuerDNPair.of(dns[i], dns[i + 1]));
                }
            }
        }
    }
    
    public DatawavePrincipal(SubjectIssuerDNPair primaryDN, UserType userType) {
        this(primaryDN, userType, null);
    }
    
    public DatawavePrincipal(SubjectIssuerDNPair primaryDN, UserType userType, List<SubjectIssuerDNPair> proxiedDNs) {
        this.userDN = primaryDN;
        this.userType = userType;
        shortName = DnUtils.getShortName(primaryDN.subjectDN());
        
        if (proxiedDNs == null) {
            proxiedEntities = new ArrayList<>();
            dns = new String[] {userDN.subjectDN(), userDN.issuerDN()};
            name = userDN.toString();
        } else {
            proxiedEntities = new ArrayList<>(proxiedDNs);
            dns = new String[2 * (1 + proxiedDNs.size())];
            dns[0] = userDN.subjectDN();
            dns[1] = userDN.issuerDN();
            for (int i = 0, pos = 2; i < proxiedDNs.size(); ++i) {
                dns[pos++] = proxiedDNs.get(i).subjectDN();
                dns[pos++] = proxiedDNs.get(i).issuerDN();
            }
            name = DnUtils.buildProxiedDN(dns);
        }
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    /**
     * Gets the user DN for this principal. If the principal is for a single user with no proxy involved, this is the only DN. Otherwise, if there is a proxy
     * chain, this will return the DN for the user in the proxy (assumes there is never more than one user in the proxy chain).
     */
    public SubjectIssuerDNPair getUserDN() {
        return userDN;
    }
    
    public List<SubjectIssuerDNPair> getProxiedEntities() {
        return Collections.unmodifiableList(proxiedEntities);
    }
    
    /**
     * Gets all DNs associated with this principal. If this principal represents a proxied entity, this will contain the user's DN and the DNs for all servers
     * in the proxy chain.
     */
    public String[] getDNs() {
        return dns;
    }
    
    /**
     * Gets the username for the user DN (see {@link #getUserDN()}) of this principal.
     */
    public String getShortName() {
        return shortName;
    }
    
    /**
     * Gets the {@link UserType} of this principal. If this is a proxied principal, then this will be the user type of the primary calling entity.
     */
    public UserType getUserType() {
        return userType;
    }
    
    public void setUserType(UserType userType) {
        this.userType = userType;
    }
    
    /**
     * Gets the Accumulo authorizations for this principal, one set per proxied entity.
     * 
     * @return a collection of Accumulo authorizations, one for each DN associated with this principal
     */
    public Collection<? extends Collection<String>> getAuthorizations() {
        return authorizations == null ? null : Collections.unmodifiableCollection(authorizations.values());
    }
    
    /**
     * Gets the Accumulo authorizations for this principal, one set per proxied entity.
     * 
     * @return a map of DN to Accumulo authorizations for each DN associated with this principal
     */
    public Map<String,Collection<String>> getAuthorizationsMap() {
        return authorizations == null ? null : Collections.unmodifiableMap(authorizations);
    }
    
    /**
     * Gets the Accumulo authorizations for the user represented by this principal.
     */
    public Collection<String> getUserAuthorizations() {
        if (authorizations != null) {
            for (Entry<String,Collection<String>> entry : authorizations.entrySet()) {
                if (entry.getKey().startsWith(getUserDN().subjectDN())) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }
    
    /**
     * Sets the Accumulo authorizations for {@code dn} to {@code authorizations}.
     */
    public void setAuthorizations(String dn, Collection<String> authorizations) {
        if (this.authorizations == null)
            this.authorizations = new HashMap<>();
        this.authorizations.put(dn, Collections.unmodifiableCollection(new LinkedHashSet<>(authorizations)));
    }
    
    /**
     * Gets the user roles (which may include additional roles added through client configuration in the security layer) for all DNs associated with this
     * principal.
     * 
     * @return a collection of roles, one for each DN associated with this principal
     */
    public Collection<? extends Collection<String>> getUserRoles() {
        return userRoles == null ? null : Collections.unmodifiableCollection(userRoles.values());
    }
    
    /**
     * Gets the user roles (which may include additional roles added through client configuration in the security layer) for {@code dn}
     *
     * @return a collection of roles, one for each DN associated with this principal
     */
    public Collection<String> getUserRoles(String dn) {
        Collection<String> roles = (userRoles == null) ? null : userRoles.get(dn);
        return (roles == null) ? null : Collections.unmodifiableCollection(roles);
    }
    
    /**
     * Gets the roles (which may include additional roles added through client configuration in the security layer) for all DNs associated with this principal.
     * 
     * @return a map of DN to user roles for each DN associated with this principal
     */
    public Map<String,Collection<String>> getUserRolesMap() {
        return userRoles == null ? null : Collections.unmodifiableMap(userRoles);
    }
    
    /**
     * Sets the user roles (which may include additional roles added through client configuration in the security layer) for {@code dn} to {@code userRoles}.
     */
    public void setUserRoles(String dn, Collection<String> userRoles) {
        if (this.userRoles == null)
            this.userRoles = new HashMap<>();
        this.userRoles.put(dn, Collections.unmodifiableCollection(new LinkedHashSet<>(userRoles)));
    }
    
    /**
     * Sets the mapping of role to Accumulo authorization that indicates what incoming authorization role let to what Accumulo authorization(s).
     *
     * @param roleToAuthMapping
     */
    public void setRoleToAuthMapping(Multimap<String,String> roleToAuthMapping) {
        if (roleToAuthMapping == null)
            roleToAuthMapping = HashMultimap.create();
        this.roleToAuthMapping = roleToAuthMapping;
    }
    
    public Map<String,Collection<String>> getRoleToAuthMapping() {
        return Collections.unmodifiableMap(roleToAuthMapping.asMap());
    }
    
    /**
     * Gets the role sets for this principal. The role sets is a combined list of authorizations used to configure method/bean level access throughout the
     * DATAWAVE web service.
     */
    public List<String> getRoleSets() {
        return roleSets == null ? null : Collections.unmodifiableList(roleSets);
    }
    
    /**
     * Sets the role sets for this principal. This is a combined set of roles used to configure method/bean level access throughout the DATAWAVE web service.
     */
    public void setRoleSets(List<String> roleSets) {
        this.roleSets = new ArrayList<>(roleSets);
    }
    
    public List<String> getProxyServers() {
        
        List<String> proxyServers = null;
        if (dns.length > 2) {
            for (int x = 0; x < dns.length; x = x + 2) {
                String dn = dns[x];
                if (DnUtils.isServerDN(dn)) {
                    
                    String cn = DnUtils.getCommonName(dn);
                    if (proxyServers == null) {
                        proxyServers = new ArrayList<>();
                    }
                    if (proxyServers.contains(cn) == false) {
                        proxyServers.add(cn);
                    }
                }
            }
        }
        return proxyServers;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("dn=").append(getName()).append(", userDN=").append(userDN).append(", user=").append(shortName).append(", authorizations=")
                        .append(authorizations);
        return sb.toString();
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((authorizations == null) ? 0 : authorizations.hashCode());
        result = prime * result + ((userRoles == null) ? 0 : userRoles.hashCode());
        result = prime * result + Arrays.hashCode(dns);
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((shortName == null) ? 0 : shortName.hashCode());
        result = prime * result + ((userDN == null) ? 0 : userDN.hashCode());
        return result;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DatawavePrincipal other = (DatawavePrincipal) obj;
        if (authorizations == null) {
            if (other.authorizations != null)
                return false;
        } else if (!authorizations.equals(other.authorizations))
            return false;
        if (userRoles == null) {
            if (other.userRoles != null)
                return false;
        } else if (!userRoles.equals(other.userRoles))
            return false;
        if (!Arrays.equals(dns, other.dns))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (shortName == null) {
            if (other.shortName != null)
                return false;
        } else if (!shortName.equals(other.shortName))
            return false;
        if (userDN == null) {
            if (other.userDN != null)
                return false;
        } else if (!userDN.equals(other.userDN))
            return false;
        return true;
    }
    
    public static DatawavePrincipal emptyPrincipal() {
        return new DatawavePrincipal();
    }
    
    public static class Roles {
        @XmlElement(name = "rolesSet")
        public List<RolesSet> roles = new ArrayList<>();
    }
    
    public static class RolesSet {
        @XmlElement(name = "dn")
        public String dn;
        @XmlElement(name = "role")
        public Collection<String> roles;
        
        public RolesSet() {}
        
        public RolesSet(String dn, Collection<String> roles) {
            this.dn = dn;
            this.roles = roles;
        }
    }
    
    public static class Auths {
        @XmlElement(name = "authorizationsSet")
        public List<AuthsSet> auths = new ArrayList<>();
    }
    
    public static class AuthsSet {
        @XmlElement(name = "dn")
        public String dn;
        @XmlElement(name = "auth")
        public Collection<String> auths;
        
        public AuthsSet() {}
        
        public AuthsSet(String dn, Collection<String> auths) {
            this.dn = dn;
            this.auths = auths;
        }
    }
    
    public static class RolesAdapter extends XmlAdapter<Roles,Map<String,? extends Collection<String>>> {
        @Override
        public Roles marshal(Map<String,? extends Collection<String>> v) throws Exception {
            Roles roles = new Roles();
            for (Entry<String,? extends Collection<String>> e : v.entrySet()) {
                roles.roles.add(new RolesSet(e.getKey(), e.getValue()));
            }
            return roles;
        }
        
        @Override
        public Map<String,? extends Collection<String>> unmarshal(Roles v) throws Exception {
            HashMap<String,HashSet<String>> result = new HashMap<>();
            for (RolesSet rs : v.roles)
                result.put(rs.dn, new HashSet<>(rs.roles));
            return result;
        }
    }
    
    public static class AuthsAdapter extends XmlAdapter<Auths,Map<String,? extends Collection<String>>> {
        @Override
        public Auths marshal(Map<String,? extends Collection<String>> v) throws Exception {
            Auths auths = new Auths();
            for (Entry<String,? extends Collection<String>> e : v.entrySet()) {
                auths.auths.add(new AuthsSet(e.getKey(), e.getValue()));
            }
            return auths;
        }
        
        @Override
        public Map<String,? extends Collection<String>> unmarshal(Auths v) throws Exception {
            HashMap<String,HashSet<String>> result = new HashMap<>();
            for (AuthsSet as : v.auths)
                result.put(as.dn, new HashSet<>(as.auths));
            return result;
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.HtmlProvider#getTitle()
     */
    @Override
    public String getTitle() {
        return TITLE;
    }
    
    @Override
    public String getHeadContent() {
        return EMPTY;
    }
    
    @Override
    public String getPageHeader() {
        return "Credentials for " + StringEscapeUtils.escapeHtml(getUserDN().toString());
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.HtmlProvider#getMainContent()
     */
    @Override
    public String getMainContent() {
        StringBuilder builder = new StringBuilder();
        builder.append("<table>");
        builder.append("<tr class=\"highlight\"><td>").append("UserDN").append("</td><td>").append(StringEscapeUtils.escapeHtml(getUserDN().toString()))
                        .append("</td></tr>");
        builder.append("<tr><td>").append("Name").append("</td><td>").append(StringEscapeUtils.escapeHtml(this.getName())).append("</td></tr>");
        builder.append("<tr class=\"highlight\"><td>").append("Uid").append("</td><td>").append(this.getShortName()).append("</td></tr>");
        builder.append("<tr><td>").append("Roles").append("</td><td>").append(StringUtils.join(this.getUserRoles(), ", ")).append("</td></tr>");
        builder.append("<tr class=\"highlight\"><td>").append("Authorizations").append("</td><td>").append(StringUtils.join(this.getAuthorizations(), ", "))
                        .append("</td></tr>");
        builder.append("</table>");
        
        return builder.toString();
    }
}
