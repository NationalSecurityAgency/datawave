package datawave.webservice.geowave.auth.provider;

import mil.nga.giat.geowave.adapter.auth.AuthorizationSPI;
import datawave.security.authorization.DatawavePrincipal;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.Collection;

public class DatawaveAuthorizationProvider implements AuthorizationSPI {
    public String[] getAuthorizations() {
        Object principalObj = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        if (principalObj instanceof DatawavePrincipal) {
            Collection<String> accesses = ((DatawavePrincipal) principalObj).getUserAuthorizations();
            return accesses.toArray(new String[accesses.size()]);
        } else {
            throw new IllegalArgumentException("Invalid Spring Security principal type: " + principalObj.getClass().getName() + " with value " + principalObj);
        }
    }
}
