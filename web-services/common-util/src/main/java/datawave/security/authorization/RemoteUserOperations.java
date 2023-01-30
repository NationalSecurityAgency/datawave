package datawave.security.authorization;

import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.AuthorizationsUtil;
import datawave.user.AuthorizationsListBase;
import datawave.webservice.result.GenericResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A remote query service is one that can pass calls off to another external user operations endpoint
 */
public interface RemoteUserOperations {
    
    AuthorizationsListBase listEffectiveAuthorizations(Object callerObject) throws AuthorizationException;
    
    GenericResponse<String> flushCachedCredentials(Object callerObject);
    
    default DatawavePrincipal getRemoteUser(DatawavePrincipal principal) throws AuthorizationException {
        // get the effective authorizations for this user
        AuthorizationsListBase auths = listEffectiveAuthorizations(principal);
        
        // create a new set of proxied users
        List<DatawaveUser> mappedUsers = new ArrayList<>();
        Map<SubjectIssuerDNPair,DatawaveUser> users = principal.getProxiedUsers().stream().collect(Collectors.toMap(DatawaveUser::getDn, Function.identity()));
        
        // create a mapped user for the primary user with the auths returned by listEffectiveAuthorizations
        SubjectIssuerDNPair pair = SubjectIssuerDNPair.of(auths.getUserDn(), auths.getIssuerDn());
        DatawaveUser user = users.get(pair);
        mappedUsers.add(new DatawaveUser(pair, user.getUserType(), auths.getAllAuths(), user.getRoles(), user.getRoleToAuthMapping(), System
                        .currentTimeMillis()));
        
        // for each proxied user, create a new user with the auths returned by listEffectiveAuthroizations
        Map<AuthorizationsListBase.SubjectIssuerDNPair,Set<String>> authMap = auths.getAuths();
        for (Map.Entry<AuthorizationsListBase.SubjectIssuerDNPair,Set<String>> entry : authMap.entrySet()) {
            pair = SubjectIssuerDNPair.of(entry.getKey().subjectDN, entry.getKey().issuerDN);
            user = users.get(pair);
            mappedUsers.add(new DatawaveUser(pair, user.getUserType(), entry.getValue(), user.getRoles(), user.getRoleToAuthMapping(), System
                            .currentTimeMillis()));
        }
        
        // return a principal with the mapped users
        return new DatawavePrincipal(mappedUsers);
    }
    
}
