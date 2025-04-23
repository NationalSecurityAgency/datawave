package datawave.microservice.config.security;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import datawave.microservice.authorization.config.DatawaveSecurityProperties;

public class DeniedAccessRoleFilter extends OncePerRequestFilter {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private DatawaveSecurityProperties securityProperties;
    
    public DeniedAccessRoleFilter(DatawaveSecurityProperties securityProperties) {
        this.securityProperties = securityProperties;
    }
    
    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws ServletException, IOException {
        if (securityProperties.getDeniedAccessRole() != null) {
            Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
            if (authentication != null && authentication.getAuthorities().stream().map(GrantedAuthority::getAuthority)
                            .anyMatch(role -> role.equals(securityProperties.getDeniedAccessRole()))) {
                logger.warn("Login denied for {} due to membership in the deny-access group {}", authentication.getName(),
                                securityProperties.getDeniedAccessRole());
                throw new BadCredentialsException(authentication.getName() + " is not authorized");
            }
        }
        
        // Continue the chain to handle any other filters
        filterChain.doFilter(request, response);
    }
}
