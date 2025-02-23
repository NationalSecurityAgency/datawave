package datawave.user;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.commons.lang3.StringUtils;

import io.protostuff.Schema;

/**
 * A list representing authorizations to be used with the DataWave web service. User authorizations are not necessarily a single list of authorizations. When a
 * user calls a GUI that in turn calls the DataWave web service (or maybe that service calls other services which eventually call the web service), then there
 * is a set of authorizations produced for each entity in the chain--the original user, and each server in between. The nature of the data prevents returning
 * just a single list in some cases. This class returns a list of authorizations for each entity in the chain, along with the DN for each entity.
 * <p>
 * In cases where it can be calculated, the minimum authorizations are the single set of authorizations that resulted from combining all entity authorizations.
 * For example, if all entities have the same authorizations, then a single list will be returned.
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
public class DefaultAuthorizationsList extends AuthorizationsListBase<DefaultAuthorizationsList> implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @XmlElement(name = "authMapping")
    @XmlJavaTypeAdapter(AuthMappingAdapter.class)
    public Map<String,Collection<String>> getAuthMapping() {
        return new TreeMap<String,Collection<String>>(authMapping);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("userAuths=").append(userAuths);
        sb.append(", entityAuths=").append("[");
        for (Entry<SubjectIssuerDNPair,Set<String>> e : auths.entrySet()) {
            sb.append(e.getKey()).append('=').append(e.getValue());
        }
        sb.append("]");
        sb.append(", authMapping=[");
        for (Entry<String,Collection<String>> mapping : authMapping.entrySet()) {
            sb.append(mapping.getKey()).append("->(");
            for (String value : mapping.getValue()) {
                sb.append(value).append(",");
            }
            sb.append("), ");
        }
        sb.append("]");
        return sb.toString();
    }
    
    public static class AuthMapList {
        public List<AuthMapEntry> map = new LinkedList<AuthMapEntry>();
    }
    
    @XmlType(propOrder = {"role", "authorizationString"})
    @XmlAccessorType(XmlAccessType.NONE)
    @SuppressWarnings("unused")
    public static class AuthMapEntry {
        @XmlElement(name = "Role")
        private String role;
        @XmlElement(name = "AuthorizationString")
        private String authorizationString;
        
        public AuthMapEntry() {}
        
        public AuthMapEntry(String role, String authorizationString) {
            super();
            this.role = role;
            this.authorizationString = authorizationString;
        }
        
        public String getRole() {
            return role;
        }
        
        public void setRole(String role) {
            this.role = role;
        }
        
        public String getAuthorizationString() {
            return authorizationString;
        }
        
        public void setAuthorizationString(String authorizationString) {
            this.authorizationString = authorizationString;
        }
    }
    
    public static class AuthMappingAdapter extends XmlAdapter<AuthMapList,Map<String,Collection<String>>> {
        
        @Override
        public Map<String,Collection<String>> unmarshal(AuthMapList v) throws Exception {
            Map<String,Collection<String>> results = new TreeMap<String,Collection<String>>();
            for (AuthMapEntry entry : v.map) {
                if (!results.containsKey(entry.getRole())) {
                    results.put(entry.getRole(), new HashSet<String>());
                }
                results.get(entry.getRole()).add(entry.getAuthorizationString());
            }
            return results;
        }
        
        @Override
        public AuthMapList marshal(Map<String,Collection<String>> v) throws Exception {
            AuthMapList map = new AuthMapList();
            for (Entry<String,Collection<String>> mapping : v.entrySet()) {
                for (String value : mapping.getValue()) {
                    map.map.add(new AuthMapEntry(mapping.getKey(), value));
                }
            }
            return map;
        }
        
    }
    
    public static Schema<DefaultAuthorizationsList> getSchema() {
        return SCHEMA;
    }
    
    @Override
    public Schema<DefaultAuthorizationsList> cachedSchema() {
        return SCHEMA;
    }
    
    private static final Schema<DefaultAuthorizationsList> SCHEMA = new AuthListSchema<DefaultAuthorizationsList>() {
        
        @Override
        public DefaultAuthorizationsList newMessage() {
            return new DefaultAuthorizationsList();
        }
        
        @Override
        public Class<? super DefaultAuthorizationsList> typeClass() {
            return DefaultAuthorizationsList.class;
        }
        
        @Override
        public String messageName() {
            return DefaultAuthorizationsList.class.getSimpleName();
        }
        
        @Override
        public String messageFullName() {
            return DefaultAuthorizationsList.class.getName();
        }
    };
    
    @Override
    public String getMainContent() {
        StringBuilder buf = new StringBuilder();
        buf.append("<h2>Auths for user Subject: ").append(userDn).append(" (Issuer ").append(issuerDn).append(")</h2>");
        buf.append("<table>");
        int x = 0;
        Iterator<String> iter = this.userAuths.iterator();
        for (int i = 0; i < userAuths.size() + 1; i += 3) {
            if (x % 2 == 0)
                buf.append("<tr>");
            else
                buf.append("<tr class=\"highlight\">");
            
            if (iter.hasNext())
                buf.append("<td>").append(iter.next()).append("</td>");
            if (iter.hasNext())
                buf.append("<td>").append(iter.next()).append("</td>");
            if (iter.hasNext())
                buf.append("<td>").append(iter.next()).append("</td>");
            buf.append("</tr>");
            x++;
        }
        buf.append("</table>");
        for (Entry<SubjectIssuerDNPair,Set<String>> entry : this.auths.entrySet()) {
            // Skip the user's authorizations, since we just displayed them above.
            if (StringUtils.equals(entry.getKey().subjectDN, userDn))
                continue;
            buf.append("<h2>Auths for Subject: ").append(entry.getKey().subjectDN).append(" (Issuer: ").append(entry.getKey().issuerDN).append(")</h2>");
            buf.append("<table>");
            x = 0;
            iter = entry.getValue().iterator();
            for (int i = 0; i < entry.getValue().size() + 1; i += 3) {
                if (x % 2 == 0)
                    buf.append("<tr>");
                else
                    buf.append("<tr class=\"highlight\">");
                
                if (iter.hasNext())
                    buf.append("<td>").append(iter.next()).append("</td>");
                if (iter.hasNext())
                    buf.append("<td>").append(iter.next()).append("</td>");
                if (iter.hasNext())
                    buf.append("<td>").append(iter.next()).append("</td>");
                buf.append("</tr>");
                x++;
            }
            buf.append("</table>");
        }
        
        buf.append("<h2>Roles to Accumulo Auths</h2>");
        buf.append("<table>");
        buf.append("<tr><th>Role</th><th>Accumulo Authorizations</th></tr>");
        x = 0;
        for (Entry<String,Collection<String>> mapping : this.authMapping.entrySet()) {
            if (x % 2 == 0)
                buf.append("<tr>");
            else
                buf.append("<tr class=\"highlight\">");
            buf.append("<td>").append(mapping.getKey()).append("</td><td>").append(StringUtils.join(mapping.getValue(), ",")).append("</td></tr>");
            x++;
        }
        buf.append("</table>");
        
        return buf.toString();
    }
    
}
