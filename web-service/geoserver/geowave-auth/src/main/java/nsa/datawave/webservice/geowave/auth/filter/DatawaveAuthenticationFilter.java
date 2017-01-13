package nsa.datawave.webservice.geowave.auth.filter;

import org.geoserver.security.filter.GeoServerJ2eeAuthenticationFilter;
import org.geoserver.security.impl.GeoServerRole;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.Principal;
import java.util.Collection;
import java.util.logging.Level;

public class DatawaveAuthenticationFilter extends GeoServerJ2eeAuthenticationFilter {
    @Override
    protected void doAuthenticate(HttpServletRequest request, HttpServletResponse response) {
        Principal principal = request.getUserPrincipal();
        if (principal != null) {
            LOGGER.log(Level.FINE, "preAuthenticatedPrincipal = " + principal + ", trying to authenticate");
            PreAuthenticatedAuthenticationToken result = null;
            Collection roles = null;
            
            try {
                roles = this.getRoles(request, principal.getName());
            } catch (IOException var7) {
                throw new RuntimeException(var7);
            }
            
            if (!roles.contains(GeoServerRole.AUTHENTICATED_ROLE)) {
                roles.add(GeoServerRole.AUTHENTICATED_ROLE);
            }
            
            result = new PreAuthenticatedAuthenticationToken(principal, (Object) null, roles);
            
            result.setDetails(this.getAuthenticationDetailsSource().buildDetails(request));
            SecurityContextHolder.getContext().setAuthentication(result);
        }
    }
}
