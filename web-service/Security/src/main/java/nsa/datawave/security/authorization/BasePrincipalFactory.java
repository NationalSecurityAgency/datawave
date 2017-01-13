package nsa.datawave.security.authorization;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import nsa.datawave.configuration.spring.SpringBean;
import nsa.datawave.security.util.DnUtils;

import javax.inject.Inject;

public abstract class BasePrincipalFactory implements PrincipalFactory {
    
    private static final long serialVersionUID = 1L;
    
    @Inject
    @SpringBean(refreshable = true)
    private BasePrincipalFactoryConfiguration config;
    
    public Set<String> getDNTRoles(String subjectDN) {
        
        LinkedHashSet<String> additionalRoles = new LinkedHashSet<>();
        
        boolean serverDN = DnUtils.isServerDN(subjectDN);
        
        if (config.isInsertDNTypeRoles()) {
            if (serverDN) {
                additionalRoles.add(config.getServerRoleName());
            } else {
                additionalRoles.add(config.getUserRoleName());
            }
        }
        
        return additionalRoles;
    }
    
    public Set<String> getOURoles(String subjectDN) {
        
        Set<String> ouRoles = new LinkedHashSet<>();
        
        Collection<String> insertOURoles = config.getInsertOURoles();
        if (insertOURoles != null && !insertOURoles.isEmpty()) {
            String[] ous = DnUtils.getOrganizationalUnits(subjectDN);
            for (int i = 0; i < ous.length; ++i) {
                String ou = ous[i];
                ou = ou.replaceAll(" ", "_");
                ou = ou.replaceAll("\\.", "_");
                // Do a case-insensitive comparison, and then add the role as it appears in the OU role list.
                for (String ouRole : insertOURoles) {
                    if (ou.equalsIgnoreCase(ouRole)) {
                        ouRoles.add(ouRole);
                        break;
                    }
                }
            }
        }
        return ouRoles;
    }
    
    public String[] mergeRoles(String[] target, String[] additional) {
        LinkedHashSet<String> s1 = asSet(target);
        LinkedHashSet<String> s2 = asSet(additional);
        
        boolean sticky = false;
        String stickyUserRole = config.getStickyUserRole();
        String stickyServerRole = config.getStickyServerRole();
        if (stickyUserRole != null && stickyServerRole != null) {
            sticky = (s1.contains(stickyUserRole) && s2.contains(stickyServerRole)) || (s1.contains(stickyServerRole) && s2.contains(stickyUserRole));
        }
        
        LinkedHashSet<String> finalSet = new LinkedHashSet<>();
        finalSet.addAll(s1);
        finalSet.retainAll(s2);
        
        if (sticky)
            finalSet.add(stickyUserRole);
        return finalSet.toArray(new String[finalSet.size()]);
    }
    
    private static <T> LinkedHashSet<T> asSet(T... vals) {
        LinkedHashSet<T> set = new LinkedHashSet<>(Math.max(2 * vals.length, 11));
        for (T val : vals)
            set.add(val);
        return set;
    }
}
