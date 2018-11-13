package datawave.user;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;
import datawave.webservice.HtmlProvider;

/**
 * 
 */
@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultAuthorizationsList.class)
public abstract class AuthorizationsListBase<T> implements Message<T>, HtmlProvider {
    protected static final String TITLE = "Effective Authorizations", EMPTY = "";
    
    @XmlElementWrapper(name = "allAuths")
    @XmlElement(name = "auth")
    protected TreeSet<String> userAuths = new TreeSet<String>();
    
    protected String userDn;
    protected String issuerDn;
    
    protected LinkedHashMap<SubjectIssuerDNPair,Set<String>> auths = new LinkedHashMap<SubjectIssuerDNPair,Set<String>>();
    
    protected Map<String,Collection<String>> authMapping;
    
    public abstract String getMainContent();
    
    @XmlElement(name = "entityAuths")
    @XmlJavaTypeAdapter(AuthListAdapter.class)
    public LinkedHashMap<SubjectIssuerDNPair,Set<String>> getAuths() {
        
        LinkedHashMap<SubjectIssuerDNPair,Set<String>> authMap = new LinkedHashMap<SubjectIssuerDNPair,Set<String>>();
        for (Map.Entry<SubjectIssuerDNPair,Set<String>> e : auths.entrySet()) {
            authMap.put(new SubjectIssuerDNPair(e.getKey().subjectDN, e.getKey().issuerDN), Collections.unmodifiableSet(e.getValue()));
        }
        return authMap;
    }
    
    public void addAuths(String dn, String issuerDN, Collection<String> dnAuths) {
        auths.put(new SubjectIssuerDNPair(dn, issuerDN), new TreeSet<String>(dnAuths));
    }
    
    public void setUserAuths(String userDn, String issuerDn, Collection<String> userAuths) {
        this.userDn = userDn;
        this.issuerDn = issuerDn;
        this.userAuths.addAll(userAuths);
    }
    
    public TreeSet<String> getAllAuths() {
        return new TreeSet<String>(userAuths);
    }
    
    public void setAuthMapping(Map<String,Collection<String>> authMapping) {
        this.authMapping = new TreeMap<String,Collection<String>>(authMapping);
    }
    
    public String getTitle() {
        return TITLE;
    }
    
    public String getHeadContent() {
        return EMPTY;
    }
    
    public String getPageHeader() {
        return TITLE;
    }
    
    public static class SubjectIssuerDNPair {
        @XmlElement(name = "subjectDN")
        public String subjectDN;
        @XmlElement(name = "issuerDN")
        public String issuerDN;
        
        // Empty constructor needed for mapping frameworks
        @SuppressWarnings("unused")
        public SubjectIssuerDNPair() {}
        
        public SubjectIssuerDNPair(String subjectDN, String issuerDN) {
            this.subjectDN = subjectDN;
            this.issuerDN = issuerDN;
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(subjectDN);
            if (issuerDN != null) {
                sb.append("<").append(issuerDN).append(">");
            }
            return sb.toString();
        }
        
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((issuerDN == null) ? 0 : issuerDN.hashCode());
            result = prime * result + ((subjectDN == null) ? 0 : subjectDN.hashCode());
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
            SubjectIssuerDNPair other = (SubjectIssuerDNPair) obj;
            if (issuerDN == null) {
                if (other.issuerDN != null)
                    return false;
            } else if (!issuerDN.equals(other.issuerDN))
                return false;
            if (subjectDN == null) {
                if (other.subjectDN != null)
                    return false;
            } else if (!subjectDN.equals(other.subjectDN))
                return false;
            return true;
        }
    }
    
    public static class AuthList {
        @XmlElement(name = "entity")
        public List<AuthListEntry> entries = new ArrayList<AuthListEntry>();
    }
    
    @XmlType(propOrder = {"dn", "auths"})
    public static class AuthListEntry {
        @XmlElement(name = "entityID")
        public SubjectIssuerDNPair dn;
        @XmlElement(name = "auth")
        private TreeSet<String> auths;
        
        // Empty constructor needed for mapping frameworks
        @SuppressWarnings("unused")
        public AuthListEntry() {}
        
        public AuthListEntry(SubjectIssuerDNPair dn, Set<String> auths) {
            this.dn = dn;
            this.auths = new TreeSet<String>(auths);
        }
    }
    
    public static class AuthListAdapter extends XmlAdapter<AuthList,Map<SubjectIssuerDNPair,Set<String>>> {
        @Override
        public AuthList marshal(Map<SubjectIssuerDNPair,Set<String>> v) throws Exception {
            AuthList list = new AuthList();
            for (Map.Entry<SubjectIssuerDNPair,Set<String>> e : v.entrySet())
                list.entries.add(new AuthListEntry(e.getKey(), e.getValue()));
            return list;
        }
        
        @Override
        public Map<SubjectIssuerDNPair,Set<String>> unmarshal(AuthList v) throws Exception {
            Map<SubjectIssuerDNPair,Set<String>> result = new LinkedHashMap<SubjectIssuerDNPair,Set<String>>();
            for (AuthListEntry e : v.entries)
                result.put(e.dn, e.auths);
            return result;
        }
    }
    
    protected abstract static class AuthListSchema<T extends AuthorizationsListBase<?>> implements Schema<T> {
        
        @Override
        public boolean isInitialized(T message) {
            return true;
        }
        
        @Override
        public void writeTo(Output output, T message) throws IOException {
            if (message.auths != null) {
                for (Map.Entry<SubjectIssuerDNPair,Set<String>> e : message.auths.entrySet()) {
                    StringBuilder sb = new StringBuilder(e.getKey().subjectDN);
                    sb.append("|").append(e.getKey().issuerDN);
                    for (String auth : e.getValue())
                        sb.append("|").append(auth);
                    output.writeString(1, sb.toString(), true);
                }
            }
            if (message.authMapping != null) {
                for (Map.Entry<String,Collection<String>> entry : message.authMapping.entrySet()) {
                    for (String entry2 : entry.getValue()) {
                        output.writeString(2, entry.getKey() + "|" + entry2, true);
                    }
                }
            }
        }
        
        @Override
        public void mergeFrom(Input input, T message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        if (message.auths == null)
                            message.auths = new LinkedHashMap<SubjectIssuerDNPair,Set<String>>();
                        String[] parts = input.readString().split("\\|");
                        if ("null".equals(parts[1]))
                            parts[1] = null;
                        TreeSet<String> set = new TreeSet<String>();
                        set.addAll(Arrays.asList(parts).subList(2, parts.length));
                        message.addAuths(parts[0], parts[1], set);
                        break;
                    case 2:
                        if (message.authMapping == null)
                            message.authMapping = new TreeMap<String,Collection<String>>();
                        String[] auths = input.readString().split("\\|");
                        if (!message.authMapping.containsKey(auths[0]))
                            message.authMapping.put(auths[0], new HashSet<String>());
                        message.authMapping.get(auths[0]).add(auths[1]);
                        break;
                    default:
                        input.handleUnknownField(number, this);
                        break;
                }
            }
        }
        
        @Override
        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "auths";
                case 2:
                    return "authMapping";
                default:
                    return null;
            }
        }
        
        @Override
        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number;
        }
        
        final java.util.HashMap<String,Integer> fieldMap = new java.util.HashMap<String,Integer>();
        {
            fieldMap.put("auths", 1);
            fieldMap.put("authMapping", 2);
        }
    }
}
