package datawave.security.authorization;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.user.AuthorizationsListBase;
import datawave.webservice.result.GenericResponse;

/**
 * A user operations service is one that can pass calls off to another external user operations endpoint
 */
public interface UserOperations {
    
    AuthorizationsListBase listEffectiveAuthorizations(ProxiedUserDetails callerObject) throws AuthorizationException;
    
    GenericResponse<String> flushCachedCredentials(ProxiedUserDetails callerObject) throws AuthorizationException;
    
    default <T extends ProxiedUserDetails> T getRemoteUser(T currentUser) throws AuthorizationException {
        // get the effective authorizations for this user
        AuthorizationsListBase auths = listEffectiveAuthorizations(currentUser);
        // create a new set of proxied users
        List<DatawaveUser> mappedUsers = new ArrayList<>();
        Map<SubjectIssuerDNPair,DatawaveUser> localUsers = currentUser.getProxiedUsers().stream()
                        .collect(Collectors.toMap(DatawaveUser::getDn, Function.identity(), (v1, v2) -> v2));
        
        // create a mapped user for the primary user with the auths returned by listEffectiveAuthorizations
        SubjectIssuerDNPair primaryDn = SubjectIssuerDNPair.of(auths.getUserDn(), auths.getIssuerDn());
        DatawaveUser localUser = localUsers.get(primaryDn);
        mappedUsers.add(new DatawaveUser(primaryDn, localUser.getUserType(), auths.getAllAuths(), auths.getAuthMapping().keySet(),
                        toMultimap(auths.getAuthMapping()), System.currentTimeMillis()));
        // for each proxied user, create a new user with the auths returned by listEffectiveAuthorizations
        Map<AuthorizationsListBase.SubjectIssuerDNPair,Set<String>> authMap = auths.getAuths();
        for (Map.Entry<AuthorizationsListBase.SubjectIssuerDNPair,Set<String>> entry : authMap.entrySet()) {
            SubjectIssuerDNPair pair = SubjectIssuerDNPair.of(entry.getKey().subjectDN, entry.getKey().issuerDN);
            if (!pair.equals(primaryDn)) {
                mappedUsers.add(new DatawaveUser(pair, DatawaveUser.UserType.SERVER, entry.getValue(), null, null, System.currentTimeMillis()));
            }
        }
        
        // return a proxied user details with the mapped users
        return currentUser.newInstance(mappedUsers);
    }
    
    static Multimap<String,String> toMultimap(Map<String,Collection<String>> map) {
        Multimap<String,String> multimap = HashMultimap.create();
        map.forEach(multimap::putAll);
        return multimap;
    }
    
}
