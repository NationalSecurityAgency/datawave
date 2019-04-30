package datawave.microservice.audit;

import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static datawave.security.authorization.DatawaveUser.UserType.USER;

public class TestUtils {
    
    public static final SubjectIssuerDNPair USER_DN = SubjectIssuerDNPair.of("userDn", "issuerDn");
    
    /**
     * Build ProxiedUserDetails instance with the specified user roles and auths
     */
    public static ProxiedUserDetails userDetails(Collection<String> assignedRoles, Collection<String> assignedAuths) {
        DatawaveUser dwUser = new DatawaveUser(USER_DN, USER, assignedAuths, assignedRoles, null, System.currentTimeMillis());
        return new ProxiedUserDetails(Collections.singleton(dwUser), dwUser.getCreationTime());
    }
    
    /**
     * Build URL querystring from the specified params
     */
    public static String queryString(String... params) {
        StringBuilder sb = new StringBuilder();
        Arrays.stream(params).forEach(s -> sb.append(s).append("&"));
        return sb.toString();
    }
}
