package datawave.microservice.authorization.preauth;

import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.ProxiedEntityUtils;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.authentication.preauth.AbstractPreAuthenticatedProcessingFilter;
import org.springframework.util.StringUtils;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Allows authorization based on a supplied X.509 client certificate (or information from trusted headers) and proxied entities/issuers named in headers.
 * <p>
 * If constructed to allow trusted subject headers, and no X.509 certificate is available in the request, this filter will look for the certificate information
 * in trusted headers {@value #SUBJECT_DN_HEADER} and {@value #ISSUER_DN_HEADER}. If a load balancer is trusted to terminate incoming SSL connections and
 * extract the client certificate information into headers, this method of authentication can be used.
 */
public class ProxiedEntityX509Filter extends AbstractPreAuthenticatedProcessingFilter {
    public static final String SUBJECT_DN_HEADER = "X-SSL-clientcert-subject";
    public static final String ISSUER_DN_HEADER = "X-SSL-clientcert-issuer";
    public static final String ENTITIES_HEADER = "X-ProxiedEntitiesChain";
    public static final String ISSUERS_HEADER = "X-ProxiedIssuersChain";
    
    private final boolean useTrustedSubjectHeaders;
    private final boolean requireProxiedEntities;
    private final boolean requireIssuers;
    private final AuthenticationEntryPoint authenticationEntryPoint;
    
    public ProxiedEntityX509Filter(boolean useTrustedSubjectHeaders, boolean requireProxiedEntities, boolean requireIssuers,
                    AuthenticationEntryPoint authenticationEntryPoint) {
        this.useTrustedSubjectHeaders = useTrustedSubjectHeaders;
        this.requireProxiedEntities = requireProxiedEntities;
        this.requireIssuers = requireIssuers;
        this.authenticationEntryPoint = authenticationEntryPoint;
        setCheckForPrincipalChanges(true);
    }
    
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        try {
            super.doFilter(request, response, chain);
        } catch (AuthenticationException e) {
            // Don't fail over to next authentication mechanism if there's an exception.
            // Instead, just go right to the authentication entry point (if we have one)
            if (authenticationEntryPoint != null) {
                authenticationEntryPoint.commence((HttpServletRequest) request, (HttpServletResponse) response, e);
            } else {
                throw e;
            }
        }
    }
    
    @Override
    protected Object getPreAuthenticatedPrincipal(HttpServletRequest request) {
        SubjectIssuerDNPair caller = (SubjectIssuerDNPair) getPreAuthenticatedCredentials(request);
        // If there is no certificate or trusted headers specified, then we can't return a pre-authenticated principal
        if (caller == null)
            return null;
        
        String proxiedSubjects = request.getHeader(ENTITIES_HEADER);
        String proxiedIssuers = request.getHeader(ISSUERS_HEADER);
        
        if (requireProxiedEntities) {
            if (proxiedSubjects == null) {
                throw new BadCredentialsException(ENTITIES_HEADER + " header is missing!");
            } else if (requireIssuers && proxiedIssuers == null) {
                throw new BadCredentialsException(ENTITIES_HEADER + " header was supplied, but " + ISSUERS_HEADER + " header is missing.");
            }
        }
        // If we're not requiring proxied entities, then copy the caller's DN information into the proxied entities slot.
        // Normally, we operate in a mode where an authorized certificate holder calls us on behalf of other entities. However,
        // if we don't require that, then we want to create the principal as though the caller proxied for itself.
        else if (proxiedSubjects == null) {
            proxiedSubjects = "<" + caller.subjectDN() + ">";
            proxiedIssuers = "<" + caller.issuerDN() + ">";
        }
        
        Set<SubjectIssuerDNPair> proxiedEntities = getSubjectIssuerDNPairs(proxiedSubjects, proxiedIssuers);
        
        return new ProxiedEntityPreauthPrincipal(caller, proxiedEntities);
    }
    
    @Override
    protected Object getPreAuthenticatedCredentials(HttpServletRequest request) {
        String subjectDN = null;
        String issuerDN = null;
        X509Certificate cert = extractClientCertificate(request);
        if (cert != null) {
            subjectDN = cert.getSubjectX500Principal().getName();
            issuerDN = cert.getIssuerX500Principal().getName();
        } else if (useTrustedSubjectHeaders) {
            subjectDN = request.getHeader(SUBJECT_DN_HEADER);
            issuerDN = request.getHeader(ISSUER_DN_HEADER);
        }
        if (subjectDN == null || issuerDN == null) {
            return null;
        } else {
            return SubjectIssuerDNPair.of(subjectDN, issuerDN);
        }
    }
    
    @Override
    protected boolean principalChanged(HttpServletRequest request, Authentication currentAuthentication) {
        Object principal = getPreAuthenticatedPrincipal(request);
        
        if (currentAuthentication.getCredentials() instanceof SubjectIssuerDNPair && currentAuthentication.getPrincipal() instanceof ProxiedUserDetails
                        && principal instanceof ProxiedEntityPreauthPrincipal) {
            ProxiedUserDetails curUsr = (ProxiedUserDetails) currentAuthentication.getPrincipal();
            ProxiedEntityPreauthPrincipal preAuthPrincipal = (ProxiedEntityPreauthPrincipal) principal;
            SubjectIssuerDNPair caller = (SubjectIssuerDNPair) currentAuthentication.getCredentials();
            
            List<String> curNames = curUsr.getProxiedUsers().stream().map(DatawaveUser::getName).collect(Collectors.toList());
            List<String> preAuthNames = preAuthPrincipal.getProxiedEntities().stream().map(SubjectIssuerDNPair::toString).collect(Collectors.toList());
            
            if (caller.equals(preAuthPrincipal.getCallerPrincipal()) && curNames.equals(preAuthNames)) {
                return false;
            }
        } else {
            // Authentication token isn't the right type, so we shouldn't be trying to authenticate.
            return false;
        }
        
        if (logger.isDebugEnabled()) {
            logger.debug("Pre-authenticated principal has changed to " + principal + " and will be re-authenticated");
        }
        return true;
    }
    
    private Set<SubjectIssuerDNPair> getSubjectIssuerDNPairs(String proxiedSubjects, String proxiedIssuers) {
        if (StringUtils.isEmpty(proxiedSubjects)) {
            return null;
        } else {
            Set<SubjectIssuerDNPair> proxiedEntities;
            Collection<String> entities = Arrays.asList(ProxiedEntityUtils.splitProxiedDNs(proxiedSubjects, true));
            if (!requireIssuers) {
                proxiedEntities = entities.stream().map(SubjectIssuerDNPair::of).collect(Collectors.toCollection(LinkedHashSet::new));
            } else {
                Collection<String> issuers = Arrays.asList(ProxiedEntityUtils.splitProxiedDNs(proxiedIssuers, true));
                if (issuers.size() != entities.size()) {
                    logger.warn("Failing authorization since issuers list (" + proxiedIssuers + ") and entities list (" + proxiedSubjects
                                    + ") don't match up.");
                    throw new BadCredentialsException("Invalid proxied entities chain.");
                }
                Iterator<String> issIt = issuers.iterator();
                proxiedEntities = entities.stream().map(dn -> SubjectIssuerDNPair.of(dn, issIt.next())).collect(Collectors.toCollection(LinkedHashSet::new));
            }
            return proxiedEntities;
        }
    }
    
    private X509Certificate extractClientCertificate(HttpServletRequest request) {
        X509Certificate[] certs = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");
        
        if (certs != null && certs.length > 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("X.509 client authorization certificate: " + certs[0]);
            }
            
            return certs[0];
        }
        
        if (logger.isDebugEnabled()) {
            logger.debug("No client certificate found in request.");
        }
        
        return null;
    }
}
