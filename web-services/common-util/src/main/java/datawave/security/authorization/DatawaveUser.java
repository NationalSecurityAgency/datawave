package datawave.security.authorization;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import datawave.security.util.DnUtils;
import datawave.webservice.HtmlProvider;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A user of a DATAWAVE service. Typically, one or more of these users (a chain where a user called an intermediate service which in turn called us) is
 * represented with a {@link DatawavePrincipal}.
 */
public class DatawaveUser implements Serializable, HtmlProvider {
    public enum UserType {
        USER, SERVER
    }
    
    public static final DatawaveUser ANONYMOUS_USER = new DatawaveUser(SubjectIssuerDNPair.of("ANONYMOUS"), UserType.USER, null, null, null, -1L);
    protected static final String TITLE = "Datawave User", EMPTY = "";
    
    private final String name;
    private final String commonName;
    private final SubjectIssuerDNPair dn;
    private final UserType userType;
    private final Collection<String> auths;
    private final Collection<String> unmodifiableAuths;
    private final Collection<String> roles;
    private final Collection<String> unmodifiableRoles;
    private final Multimap<String,String> roleToAuthMapping;
    private final long creationTime;
    private final long expirationTime;
    
    public DatawaveUser(SubjectIssuerDNPair dn, UserType userType, Collection<String> auths, Collection<String> roles,
                    Multimap<String,String> roleToAuthMapping, long creationTime) {
        this(dn, userType, auths, roles, roleToAuthMapping, creationTime, -1L);
    }
    
    @JsonCreator
    public DatawaveUser(@JsonProperty(value = "dn", required = true) SubjectIssuerDNPair dn,
                    @JsonProperty(value = "userType", required = true) UserType userType, @JsonProperty("auths") Collection<String> auths,
                    @JsonProperty("roles") Collection<String> roles, @JsonProperty("roleToAuthMapping") Multimap<String,String> roleToAuthMapping,
                    @JsonProperty(value = "creationTime", defaultValue = "-1L") long creationTime,
                    @JsonProperty(value = "expirationTime", defaultValue = "-1L") long expirationTime) {
        this.name = dn.toString();
        this.commonName = DnUtils.getCommonName(dn.subjectDN());
        this.dn = dn;
        this.userType = userType;
        this.auths = auths == null ? Collections.emptyList() : new LinkedHashSet<>(auths);
        this.unmodifiableAuths = Collections.unmodifiableCollection(this.auths);
        this.roles = roles == null ? Collections.emptyList() : new LinkedHashSet<>(roles);
        this.unmodifiableRoles = Collections.unmodifiableCollection(this.roles);
        this.roleToAuthMapping = roleToAuthMapping == null ? LinkedHashMultimap.create() : Multimaps.unmodifiableMultimap(LinkedHashMultimap
                        .create(roleToAuthMapping));
        this.creationTime = creationTime;
        this.expirationTime = expirationTime;
    }
    
    public String getName() {
        return name;
    }
    
    @JsonIgnore
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
        return unmodifiableAuths;
    }
    
    public Collection<String> getRoles() {
        return unmodifiableRoles;
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
    
    @Override
    @JsonIgnore
    public String getTitle() {
        return TITLE;
    }
    
    @Override
    @JsonIgnore
    public String getHeadContent() {
        return EMPTY;
    }
    
    @Override
    @JsonIgnore
    public String getPageHeader() {
        return TITLE + " - " + getCommonName();
    }
    
    @Override
    @JsonIgnore
    public String getMainContent() {
        StringBuilder builder = new StringBuilder();
        
        builder.append("<table>\n");
        builder.append("<tr><td>Subject DN:</td><td>").append(getDn().subjectDN()).append("</td></tr>\n");
        builder.append("<tr class='highlight'><td>User DN:</td><td>").append(getDn().issuerDN()).append("</td></tr>\n");
        builder.append("<tr><td>User Type:</td><td>").append(getUserType()).append("</td></tr>\n");
        builder.append("<tr class='highlight'><td>Creation Time:</td><td>").append(getCreationTime()).append("</td></tr>\n");
        builder.append("<tr><td>Expiration Time:</td><td>").append(getExpirationTime()).append("</td></tr>\n");
        builder.append("</table>\n");
        
        builder.append("<h2>Roles</h2>\n");
        generateTable(getRoles(), builder);
        
        builder.append("<h2>Authorizations</h2>\n");
        generateTable(getAuths(), builder);
        
        builder.append("<h2>Role To Auth Mapping</h2>\n");
        builder.append("<table>\n");
        builder.append("<tr><th>Role</th><th>Auth(s)</th><th>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</th><th>Role</th><th>Auth(s)</th></tr>");
        boolean highlight = false;
        for (Iterator<Map.Entry<String,Collection<String>>> iter = getRoleToAuthMapping().asMap().entrySet().iterator(); iter.hasNext(); /* empty */) {
            if (highlight) {
                builder.append("<tr class=\"highlight\">");
            } else {
                builder.append("<tr>");
            }
            highlight = !highlight;
            
            int cols = 2;
            for (int i = 0; i < cols && iter.hasNext(); i++) {
                if (iter.hasNext()) {
                    Map.Entry<String,Collection<String>> entry = iter.next();
                    builder.append("<td>").append(entry.getKey()).append("</td>");
                    builder.append("<td>").append(entry.getValue().stream().collect(Collectors.joining(", "))).append("</td>");
                } else {
                    builder.append("<td></td>");
                }
                if (i < (cols - 1))
                    builder.append("<td>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>");
            }
        }
        builder.append("</table>\n");
        
        return builder.toString();
    }
    
    private void generateTable(Collection<String> entries, StringBuilder builder) {
        builder.append("<table>\n");
        boolean highlight = false;
        for (Iterator<String> iter = entries.iterator(); iter.hasNext(); /* empty */) {
            if (highlight) {
                builder.append("<tr class=\"highlight\">");
            } else {
                builder.append("<tr>");
            }
            highlight = !highlight;
            
            for (int i = 0; i < 4 && iter.hasNext(); i++) {
                builder.append("<td>");
                builder.append(iter.hasNext() ? iter.next() : "");
                builder.append("</td>");
            }
            builder.append("</tr>");
        }
        builder.append("</table>\n");
    }
}
