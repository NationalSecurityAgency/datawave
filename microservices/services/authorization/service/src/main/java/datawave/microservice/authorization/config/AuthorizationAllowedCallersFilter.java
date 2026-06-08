package datawave.microservice.authorization.config;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.security.web.AuthenticationEntryPoint;

import datawave.microservice.config.security.AllowedCallersFilter;

public class AuthorizationAllowedCallersFilter extends AllowedCallersFilter {
    private static final Set<String> allowAllUsersPaths = new LinkedHashSet<>();

    static {
        allowAllUsersPaths.add("/oauth");
        allowAllUsersPaths.add("/listEffectiveAuthorizations");
    }

    public static void addAllowAllUsersPath(String path) {
        allowAllUsersPaths.add(path);
    }

    public AuthorizationAllowedCallersFilter(DatawaveSecurityProperties securityProperties, AuthenticationEntryPoint authenticationEntryPoint) {
        super(securityProperties, authenticationEntryPoint);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, FilterChain filterChain)
                    throws ServletException, IOException {

        if (AuthorizationAllowedCallersFilter.enforceAllowedCallersForRequest(httpServletRequest)) {
            super.doFilterInternal(httpServletRequest, httpServletResponse, filterChain);
        } else {
            // Continue the chain to handle any other filters
            filterChain.doFilter(httpServletRequest, httpServletResponse);
        }
    }

    public static boolean enforceAllowedCallersForRequest(HttpServletRequest httpServletRequest) {
        String requestPath = httpServletRequest.getServletPath();
        if (requestPath.matches("/v\\d*/.*")) {
            // find second / to remove the /v1, /v2, etc. from the start of the path
            int x = requestPath.indexOf("/", 1);
            requestPath = requestPath.substring(x);
        }
        for (String allowedPath : allowAllUsersPaths) {
            if (requestPath.startsWith(allowedPath)) {
                return false;
            }
        }
        return true;
    }
}
