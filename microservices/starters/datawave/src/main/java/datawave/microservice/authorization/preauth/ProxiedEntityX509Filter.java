package datawave.microservice.authorization.preauth;

import static datawave.microservice.config.web.Constants.REQUEST_LOGIN_TIME_ATTRIBUTE;
import static datawave.microservice.config.web.Constants.REQUEST_START_TIME_NS_ATTRIBUTE;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.authentication.preauth.AbstractPreAuthenticatedProcessingFilter;
import org.springframework.util.StringUtils;

import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.util.ProxiedEntityUtils;

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
        // if JWTAuthenticationFilter has authenticated the user already, we should
        // use that Authentication instead of checking for Principal changes
        setCheckForPrincipalChanges(false);
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
        List<SubjectIssuerDNPair> proxiedEntities = new ArrayList<>();
        if (proxiedSubjects != null) {
            proxiedEntities.addAll(getSubjectIssuerDNPairs(proxiedSubjects, proxiedIssuers));
        }
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
        // this would only get called if checkForPrincipalChanges=true (constructor sets it to false) and there is an
        // Authentication already in the SecurityContext. If a previous filter such as the
        // JWTAuthenticationFilter/JWTAuthenticationProvider parsed a JWT into a JWTAuthentication then we should
        // accept that Authentication and not check for a changed principal
        return false;
    }
    
    protected List<SubjectIssuerDNPair> getSubjectIssuerDNPairs(String proxiedSubjects, String proxiedIssuers) {
        if (StringUtils.isEmpty(proxiedSubjects)) {
            return null;
        } else {
            List<SubjectIssuerDNPair> proxiedEntities;
            Collection<String> entities = Arrays.asList(ProxiedEntityUtils.splitProxiedDNs(proxiedSubjects, true));
            if (!requireIssuers) {
                proxiedEntities = entities.stream().map(SubjectIssuerDNPair::of).collect(Collectors.toCollection(ArrayList::new));
            } else {
                Collection<String> issuers = Arrays.asList(ProxiedEntityUtils.splitProxiedDNs(proxiedIssuers, true));
                if (issuers.size() != entities.size()) {
                    logger.warn("Failing authorization since issuers list (" + proxiedIssuers + ") and entities list (" + proxiedSubjects
                                    + ") don't match up.");
                    throw new BadCredentialsException("Invalid proxied entities chain.");
                }
                Iterator<String> issIt = issuers.iterator();
                proxiedEntities = entities.stream().map(dn -> SubjectIssuerDNPair.of(dn, issIt.next())).collect(Collectors.toCollection(ArrayList::new));
            }
            return proxiedEntities;
        }
    }
    
    private X509Certificate extractClientCertificate(HttpServletRequest request) {
        X509Certificate[] certs = (X509Certificate[]) request.getAttribute("javax.servlet.request.X509Certificate");
        
        if (certs != null && certs.length > 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("X.509 client authorization certificate: [Subject DN: " + certs[0].getSubjectDN().getName() + ", Issuer DN: "
                                + certs[0].getIssuerDN().getName() + "]");
            }
            
            return certs[0];
        }
        
        if (logger.isDebugEnabled()) {
            logger.debug("No client certificate found in request.");
        }
        
        return null;
    }
    
    @Override
    protected void successfulAuthentication(HttpServletRequest request, HttpServletResponse response, Authentication authResult)
                    throws IOException, ServletException {
        super.successfulAuthentication(request, response, authResult);
        setLoginTimeHeader(request);
    }
    
    @Override
    protected void unsuccessfulAuthentication(HttpServletRequest request, HttpServletResponse response, AuthenticationException failed)
                    throws IOException, ServletException {
        super.unsuccessfulAuthentication(request, response, failed);
        setLoginTimeHeader(request);
    }
    
    private void setLoginTimeHeader(HttpServletRequest request) {
        if (request.getAttribute(REQUEST_START_TIME_NS_ATTRIBUTE) != null) {
            long loginTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - (long) request.getAttribute(REQUEST_START_TIME_NS_ATTRIBUTE));
            request.setAttribute(REQUEST_LOGIN_TIME_ATTRIBUTE, String.valueOf(loginTime));
        }
    }
}
