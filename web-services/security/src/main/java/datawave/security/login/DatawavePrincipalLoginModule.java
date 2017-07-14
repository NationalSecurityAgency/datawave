package datawave.security.login;

import java.io.IOException;
import java.security.KeyStore;
import java.security.Principal;
import java.security.acl.Group;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.inject.Inject;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AccountLockedException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;

import io.undertow.security.idm.X509CertificateCredential;
import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.configuration.spring.BeanProvider;
import datawave.security.auth.DatawaveCredential;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawavePrincipalLookup;
import datawave.security.util.DnUtils;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.jboss.logging.Logger;
import org.jboss.security.JSSESecurityDomain;
import org.jboss.security.SimpleGroup;
import org.jboss.security.SimplePrincipal;
import org.jboss.security.auth.callback.ObjectCallback;
import org.jboss.security.auth.certs.X509CertificateVerifier;
import org.jboss.security.auth.spi.AbstractServerLoginModule;
import org.picketbox.util.StringUtil;

@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class DatawavePrincipalLoginModule extends AbstractServerLoginModule {
    
    private Principal identity;
    private X509Certificate certificateCredential;
    private DatawaveCredential datawaveCredential;
    private X509CertificateVerifier verifier;
    private boolean datawaveVerifier;
    
    private boolean trustedHeaderLogin;
    private boolean allowUserProxying;
    
    private String blacklistUserRole = null;
    
    @Inject
    private DatawavePrincipalLookup datawavePrincipalLookupBean;
    @Inject
    private JSSESecurityDomain domain;
    
    private boolean trace;
    
    public DatawavePrincipalLoginModule() {
        log = Logger.getLogger(getClass());
    }
    
    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String,?> sharedState, Map<String,?> options) {
        
        trace = log.isTraceEnabled();
        
        super.initialize(subject, callbackHandler, sharedState, options);
        
        // Have the bean container do injection for us so we don't have to do JNDI lookup.
        performFieldInjection();
        
        String option = (String) options.get("verifier");
        if (option != null) {
            try {
                ClassLoader loader = Thread.currentThread().getContextClassLoader();
                Class<?> verifierClass = loader.loadClass(option);
                verifier = (X509CertificateVerifier) verifierClass.newInstance();
                if (verifier instanceof DatawaveCertVerifier) {
                    ((DatawaveCertVerifier) verifier).setLogger(log);
                    ((DatawaveCertVerifier) verifier).setOcspLevel((String) options.get("ocspLevel"));
                    datawaveVerifier = true;
                }
            } catch (Throwable e) {
                e.printStackTrace();
                if (trace)
                    log.trace("Failed to create X509CertificateVerifier", e);
                throw new IllegalArgumentException("Invalid verifier: " + option, e);
            }
        }
        
        option = (String) options.get("trustedHeaderLogin");
        if (option != null)
            trustedHeaderLogin = Boolean.valueOf(option);
        
        option = (String) options.get("allowUserProxying");
        if (option != null)
            allowUserProxying = Boolean.valueOf(option);
        
        blacklistUserRole = (String) options.get("blacklistUserRole");
        if (blacklistUserRole != null && "".equals(blacklistUserRole.trim()))
            blacklistUserRole = null;
        
        if (trace)
            log.trace("exit: initialize(Subject, CallbackHandler, Map, Map)");
    }
    
    protected void performFieldInjection() {
        if (datawavePrincipalLookupBean == null) {
            BeanProvider.injectFields(this);
        }
    }
    
    @Override
    protected Principal getIdentity() {
        return identity;
    }
    
    protected String getUsername() {
        String username = null;
        if (getIdentity() != null)
            username = getIdentity().getName();
        return username;
    }
    
    @Override
    protected Group[] getRoleSets() throws LoginException {
        Group groups[];
        try {
            Set<String> roles = new TreeSet<>();
            String targetUser = getUsername();
            DatawavePrincipal principal = (DatawavePrincipal) getIdentity();
            
            List<String> cpRoleSets = principal.getRoleSets();
            if (cpRoleSets != null) {
                roles.addAll(cpRoleSets);
            }
            StringBuilder buf = new StringBuilder("[" + roles.size() + "] Groups for " + targetUser + " {");
            if (roles.size() > 0) {
                Group group = new SimpleGroup("Roles");
                boolean first = true;
                for (String r : roles)
                    try {
                        if (!first) {
                            buf.append(":");
                        }
                        first = false;
                        group.addMember(new SimplePrincipal(r));
                        buf.append(" ").append(r).append(" ");
                    } catch (Exception e) {
                        log.debug("Failed to create principal for: " + r, e);
                    }
                
                groups = new Group[2];
                groups[0] = group;
                groups[1] = new SimpleGroup("CallerPrincipal");
                groups[1].addMember(getIdentity());
            } else {
                groups = new Group[0];
            }
            buf.append("}");
            log.debug(buf.toString());
        } catch (RuntimeException e) {
            groups = new Group[0];
            log.warn("Exception in getRoleSets: " + e.getMessage(), e);
            abort();
        }
        return groups;
    }
    
    @Override
    public boolean commit() throws LoginException {
        // If our login is ok, then remove any principals from the subject principals list that match our type.
        // If another login module produces a DatawavePrincipal before us, it will be associated with the subject
        // and later retrieved instead of the one we produce here. Therefore we remove any DatawavePrincipals
        // associated with the subject so that doesn't happen.
        log.trace("Committing login for " + getIdentity() + "@" + System.identityHashCode(getIdentity()) + ". loginOk=" + loginOk);
        if (loginOk) {
            DatawavePrincipal dp = (DatawavePrincipal) getIdentity();
            for (DatawavePrincipal p : subject.getPrincipals(DatawavePrincipal.class)) {
                if (dp.getName().equals(p.getName())) {
                    log.trace("Removing duplicate principal " + p + "@" + System.identityHashCode(p));
                    subject.getPrincipals().remove(p);
                } else {
                    log.trace("Skipping " + p + "@" + System.identityHashCode(p) + " since [" + p.getName() + "] != [" + p.getName() + "]");
                }
            }
            
            // There is also a CallerPrincipal group that login modules create and add the identity to. Other login modules will
            // then maybe add a DatawavePrincipal to this group. The identity manager uses the CallerPrincipal group and the
            // principals on the subject to determine the true caller principal, so we need to be sure to remove the previously
            // created DatawavePrincipal from the CallerPrincipal group as well.
            Group callerGroup = getCallerPrincipalGroup(subject.getPrincipals());
            if (callerGroup != null) {
                Set<Principal> principalsToRemove = new HashSet<>();
                for (Enumeration<? extends Principal> e = callerGroup.members(); e.hasMoreElements();) {
                    Principal p = e.nextElement();
                    if (p instanceof DatawavePrincipal) {
                        if (dp.getName().equals(p.getName())) {
                            principalsToRemove.add(p);
                        } else {
                            log.trace("Skipping from CallerPrincipal group " + p + "@" + System.identityHashCode(p) + " since [" + p.getName() + "] != ["
                                            + p.getName() + "]");
                        }
                    }
                }
                for (Principal p : principalsToRemove) {
                    log.trace("Removing from CallerPrincipal group duplicate principal " + p + "@" + System.identityHashCode(p));
                    callerGroup.removeMember(p);
                }
            }
        }
        boolean ok = super.commit();
        if (ok && certificateCredential != null)
            subject.getPublicCredentials().add(certificateCredential);
        return ok;
    }
    
    @Override
    public boolean login() throws LoginException {
        try {
            // We don't really place nice with other login modules. If the other module sticks a cert
            // in the shared state for login, then we're ok. Otherwise, we are going to reject the login.
            if (super.login()) {
                Object username = sharedState.get("javax.security.auth.login.name");
                if (username instanceof Principal) {
                    identity = (Principal) username;
                    if (trace)
                        log.trace("**** Username is a principle");
                } else {
                    if (trace)
                        log.trace("**** Username is not a principle");
                    String name = username.toString();
                    try {
                        identity = createIdentity(name);
                    } catch (Exception e) {
                        log.debug("Failed to create principal", e);
                        throw new LoginException("Failed to create principal: " + e.getMessage());
                    }
                }
                Object password = sharedState.get("javax.security.auth.login.password");
                if (password instanceof X509Certificate) {
                    if (trace)
                        log.trace("**** Credential is a X509Certificate");
                    certificateCredential = (X509Certificate) password;
                } else if (password instanceof X509Certificate[]) {
                    if (trace)
                        log.trace("**** Credential is an X509Certificate array");
                    certificateCredential = ((X509Certificate[]) password)[0];
                } else if (password instanceof DatawaveCredential) {
                    if (trace)
                        log.trace("**** Credential is a DatawaveCredential");
                    datawaveCredential = (DatawaveCredential) password;
                    certificateCredential = datawaveCredential.getCertificate();
                } else {
                    log.warn("Login failed due to unknown password.");
                    return false;
                }
            } else {
                Object userInfo[] = getUserInfo();
                loginOk = validateUser(userInfo);
                if (trace) {
                    log.trace("User '" + identity + "' authenticated, loginOk=" + loginOk);
                    log.debug("exit: login()");
                }
            }
            
            if (blacklistUserRole != null && loginOk && identity != null) {
                DatawavePrincipal principal = (DatawavePrincipal) getIdentity();
                String[] dns = principal.getDNs();
                for (int i = 0; i < dns.length; i += 2) {
                    if (i == dns.length - 1)
                        break;
                    String dn = DnUtils.buildProxiedDN(dns[i], dns[i + 1]);
                    Collection<String> rawRoles = principal.getRawRoles(dn);
                    if (rawRoles != null && rawRoles.contains(blacklistUserRole)) {
                        loginOk = false; // this is critical as it is what the parent class uses to actually deny login
                        String message = "Login denied for " + principal.getUserDN() + " due to membership of " + dn + " in the deny-access group "
                                        + blacklistUserRole;
                        log.debug(message);
                        throw new AccountLockedException(message);
                    }
                }
            }
        } catch (RuntimeException e) {
            
            log.warn("Login failed due to exception: " + e.getMessage(), e);
            throw new FailedLoginException(e.getMessage());
        }
        return true;
    }
    
    protected Object[] getUserInfo() throws LoginException {
        if (trace)
            log.trace("enter: getUserInfo()");
        if (callbackHandler == null) {
            log.error("Error: no CallbackHandler available to collect authentication information");
            throw new LoginException("Error: no CallbackHandler available to collect authentication information");
        }
        NameCallback nc = new NameCallback("Username: ");
        ObjectCallback oc = new ObjectCallback("Credentials: ");
        Callback callbacks[] = {nc, oc};
        String[] names;
        String name;
        Object creds = null;
        Object supplementalData = null;
        try {
            callbackHandler.handle(callbacks);
            // We use a custom class to convert the certificate to a principal.
            // The class checks the current request for X-ProxiedEntitiesChain/X-ProxiedIssuersChain
            // and uses that along with the certificate subject and issuer DNs to construct a name
            // that looks like <incomingCertSubjectDN><incomingCertIssuerDN><proxiedSubjectCertPairs>.
            // We split that out here.
            name = nc.getName();
            names = DnUtils.splitProxiedSubjectIssuerDNs(name);
            Object tmpCreds = oc.getCredential();
            // Don't check for the cert if we're using a trusted header login
            if (tmpCreds != null && !trustedHeaderLogin)
                if (tmpCreds instanceof X509CertificateCredential) {
                    creds = ((X509CertificateCredential) tmpCreds).getCertificate();
                    if (trace)
                        log.trace("found cert " + ((X509Certificate) creds).getSerialNumber().toString(16) + ":"
                                        + ((X509Certificate) creds).getSubjectDN().getName());
                } else if (tmpCreds instanceof DatawaveCredential) {
                    DatawaveCredential dwCredential = (DatawaveCredential) tmpCreds;
                    creds = dwCredential.getCertificate();
                    if (creds == null) {
                        String msg = "No certificate supplied with login credential for " + dwCredential.getUserName();
                        log.warn(msg);
                        throw new LoginException(msg);
                    }
                    if (trace)
                        log.trace("found cert " + ((X509Certificate) creds).getSerialNumber().toString(16) + ":"
                                        + ((X509Certificate) creds).getSubjectDN().getName());
                } else if (tmpCreds instanceof X509Certificate) {
                    creds = tmpCreds;
                    if (trace)
                        log.trace("found cert " + ((X509Certificate) creds).getSerialNumber().toString(16) + ":"
                                        + ((X509Certificate) creds).getSubjectDN().getName());
                } else if (tmpCreds instanceof X509Certificate[]) {
                    X509Certificate certChain[] = (X509Certificate[]) tmpCreds;
                    if (certChain.length > 0)
                        creds = certChain[0];
                    if (certChain.length > 1)
                        supplementalData = certChain[1];
                } else {
                    String msg = "Don't know how to obtain X509Certificate from: " + tmpCreds.getClass();
                    log.warn(msg);
                    throw new LoginException(msg);
                }
        } catch (IOException e) {
            log.debug("Failed to invoke callback", e);
            throw new LoginException("Failed to invoke callback: " + e);
        } catch (UnsupportedCallbackException uce) {
            log.debug("CallbackHandler does not support: " + uce.getCallback());
            throw new LoginException("CallbackHandler does not support: " + uce.getCallback());
        }
        if (trace)
            log.trace("exit: getUserInfo()");
        return new Object[] {names, creds, supplementalData};
    }
    
    @SuppressWarnings("unchecked")
    protected boolean validateUser(Object info[]) throws LoginException {
        if (trace)
            log.trace("enter: validateUserCert");
        String[] dns = (String[]) info[0];
        // Normalize all the DNs here so we store them in a consistent manner
        // in our caches. This normalization affects the case and order of
        // the DNs.
        for (int i = 0; i < dns.length; ++i)
            dns[i] = DnUtils.normalizeDN(dns[i]);
        String alias = DnUtils.buildProxiedDN(dns);
        if (info[1] instanceof X509Certificate) {
            certificateCredential = (X509Certificate) info[1];
        } else if (info[1] instanceof DatawaveCredential) {
            datawaveCredential = (DatawaveCredential) info[1];
        }
        if (trace)
            log.trace("alias = " + alias);
        if (StringUtil.isNullOrEmpty(alias)) {
            identity = unauthenticatedIdentity;
            log.trace("Authenticating as unauthenticatedIdentity=" + identity);
        }
        if (trace)
            log.trace("identity = " + identity);
        if (identity == null) {
            try {
                identity = createIdentity(alias);
                if (trace)
                    log.trace("new identity = " + identity);
            } catch (Exception e) {
                log.debug("Failed to create identity for alias:" + alias, e);
            }
            
            if (!trustedHeaderLogin) {
                String latestDN = dns[0];
                if (!validateCertificateCredential(latestDN, certificateCredential)) {
                    log.debug("Bad credential for alias=" + latestDN);
                    throw new FailedLoginException("Supplied Credential did not match existing credential for " + latestDN);
                }
            }
            
            if (!allowUserProxying && dns.length > 1) {
                int userDNcount = 0;
                // Skip every other DN since it is an issuer DN
                for (int i = 0; i < dns.length; i += 2) {
                    if (!DnUtils.isServerDN(dns[i]))
                        ++userDNcount;
                }
                if (userDNcount > 1) {
                    log.debug("Login failed due to too many user DNs (users can't proxy for other users): " + Arrays.toString(dns));
                    throw new FailedLoginException("Users may not proxy other users.");
                }
            }
            
            try {
                identity = datawavePrincipalLookupBean.lookupPrincipal(dns);
            } catch (Exception e) {
                log.debug("Failing login due to EJB exception " + e.getMessage(), e);
                throw new FailedLoginException("Unable to authenticate: " + e.getMessage());
            }
        }
        if (getUseFirstPass()) {
            sharedState.put("javax.security.auth.login.name", alias);
            sharedState.put("javax.security.auth.login.password", certificateCredential);
        }
        if (trace)
            log.trace("exit: validateUserCert");
        return true;
    }
    
    protected boolean validateCertificateCredential(String alias, X509Certificate cert) {
        if (trace)
            log.trace("enter: validateCertificateCredential(String, X509Certificate, Object)[" + verifier + "]");
        boolean isValid = false;
        KeyStore keyStore = null;
        KeyStore trustStore = null;
        if (domain != null) {
            keyStore = domain.getKeyStore();
            trustStore = domain.getTrustStore();
        }
        if (trustStore == null)
            trustStore = keyStore;
        if (verifier != null) {
            if (datawaveVerifier) {
                String issuerSubjectDn = cert.getIssuerX500Principal().getName();
                if (((DatawaveCertVerifier) verifier).isIssuerSupported(issuerSubjectDn, trustStore)) {
                    isValid = verifier.verify(cert, alias, keyStore, trustStore);
                } else if (trace) {
                    log.trace("Unsupported issuer: " + issuerSubjectDn);
                }
            } else {
                if (trace)
                    log.trace("Validating using non datawave cert verifier.");
                isValid = verifier.verify(cert, alias, keyStore, trustStore);
            }
            if (trace)
                log.trace("Cert Validation result : " + isValid);
        } else if (cert != null)
            isValid = true;
        else
            log.warn("Domain, KeyStore, or cert is null. Unable to validate the certificate.");
        if (trace)
            log.trace("exit: validateCertificateCredential(String, X509Certificate)");
        return isValid;
    }
}
