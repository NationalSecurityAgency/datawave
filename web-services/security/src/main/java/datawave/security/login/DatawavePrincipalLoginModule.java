package datawave.security.login;

import java.io.IOException;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.Principal;
import java.security.acl.Group;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.net.ssl.X509KeyManager;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AccountLockedException;
import javax.security.auth.login.CredentialException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;

import org.apache.deltaspike.core.api.exclude.Exclude;
import org.jboss.logging.Logger;
import org.jboss.security.JSSESecurityDomain;
import org.jboss.security.SimpleGroup;
import org.jboss.security.SimplePrincipal;
import org.jboss.security.auth.callback.ObjectCallback;
import org.jboss.security.auth.certs.X509CertificateVerifier;
import org.jboss.security.auth.spi.AbstractServerLoginModule;
import org.picketbox.util.StringUtil;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.configuration.spring.BeanProvider;
import datawave.security.auth.DatawaveCredential;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserService;
import datawave.security.authorization.JWTTokenHandler;
import datawave.util.StringUtils;

@SuppressWarnings("SpringAutowiredFieldsWarningInspection")
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class DatawavePrincipalLoginModule extends AbstractServerLoginModule {

    private Principal identity;
    private X509Certificate certificateCredential;
    private DatawaveCredential datawaveCredential;
    private X509CertificateVerifier verifier;
    private boolean datawaveVerifier;

    private boolean trustedHeaderLogin;
    private boolean jwtHeaderLogin;

    private String disallowlistUserRole = null;

    /**
     * Required roles are a set of roles such that each entity in a proxy chain must have at least one of the required roles. If that is not the case, then the
     * login module will ensure that none of the required roles are included in the response to getRoleSets.
     */
    private Set<String> requiredRoles = new HashSet<>();

    private Set<String> directRoles = new HashSet<>();

    @Inject
    private DatawaveUserService datawaveUserService;
    @Inject
    private JSSESecurityDomain domain;

    private JWTTokenHandler jwtTokenHandler;

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
                verifier = (X509CertificateVerifier) verifierClass.getDeclaredConstructor().newInstance();
                if (verifier instanceof DatawaveCertVerifier) {
                    ((DatawaveCertVerifier) verifier).setLogger(log);
                    ((DatawaveCertVerifier) verifier).setOcspLevel((String) options.get("ocspLevel"));
                    datawaveVerifier = true;
                }
            } catch (Throwable e) {
                if (trace) {
                    log.trace("Failed to create X509CertificateVerifier", e);
                }
                throw new IllegalArgumentException("Invalid verifier: " + option, e);
            }
        }

        option = (String) options.get("trustedHeaderLogin");
        if (option != null)
            trustedHeaderLogin = Boolean.valueOf(option);

        option = (String) options.get("jwtHeaderLogin");
        if (option != null)
            jwtHeaderLogin = Boolean.valueOf(option);

        disallowlistUserRole = (String) options.get("disallowlistUserRole");
        if (disallowlistUserRole != null && "".equals(disallowlistUserRole.trim()))
            disallowlistUserRole = null;

        option = (String) options.get("requiredRoles");
        if (option != null) {
            requiredRoles.clear();
            requiredRoles.addAll(Arrays.asList(StringUtils.split(option, ':', false)));
        } else {
            requiredRoles.add("AuthorizedUser");
            requiredRoles.add("AuthorizedServer");
            requiredRoles.add("AuthorizedQueryServer");
            requiredRoles.add("AuthorizedProxiedServer");
        }

        /**
         * the directRoles check is restricted to UserType.SERVER so the AuthorizedUser is not required in this set. There is no explicit check to verify that
         * there is overlap between requiredRoles and directRoles. If that check is wanted it could be added in the #getRoleSets()
         */

        option = (String) options.get("directRoles");
        if (option != null) {
            directRoles.clear();
            directRoles.addAll(Arrays.asList(StringUtils.split(option, ':', false)));
        } else {
            directRoles.add("AuthorizedServer");
            directRoles.add("AuthorizedQueryServer");
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(MapperFeature.USE_WRAPPER_NAME_AS_PROPERTY_NAME);
            mapper.registerModule(new GuavaModule());
            mapper.registerModule(new JaxbAnnotationModule());

            String alias = domain.getKeyStore().aliases().nextElement();
            X509KeyManager keyManager = (X509KeyManager) domain.getKeyManagers()[0];
            X509Certificate[] certs = keyManager.getCertificateChain(alias);
            Key signingKey = keyManager.getPrivateKey(alias);

            jwtTokenHandler = new JWTTokenHandler(certs[0], signingKey, 24, TimeUnit.HOURS, JWTTokenHandler.TtlMode.RELATIVE_TO_CURRENT_TIME, mapper);
        } catch (KeyStoreException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        if (trace) {
            log.trace("exit: initialize(Subject, CallbackHandler, Map, Map)");
        }
    }

    protected void performFieldInjection() {
        if (datawaveUserService == null) {
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

            Collection<String> cpRoleSets = principal.getPrimaryUser().getRoles();
            if (cpRoleSets != null) {
                roles.addAll(cpRoleSets);
                // We are requiring that at every entity in the call chain has at least one of the required roles.
                // If any entity has none of them, then exclude all of the required roles from the computed final set.
                if (principal.getProxiedUsers().stream().anyMatch(u -> Collections.disjoint(u.getRoles(), requiredRoles))) {
                    roles.removeAll(requiredRoles);
                }

            }
            StringBuilder buf = new StringBuilder("[" + roles.size() + "] Groups for " + targetUser + " {");
            if (!roles.isEmpty()) {
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
                    if (trace) {
                        log.trace("**** Username is a principal");
                    }
                } else {
                    if (trace) {
                        log.trace("**** Username is not a principal");
                    }
                    String name = username.toString();
                    try {
                        identity = createIdentity(name);
                    } catch (Exception e) {
                        loginOk = false;
                        log.debug("Failed to create principal", e);
                        // should result in a FORBIDDEN (403) response code in DatawaveAuthenticationMechanism.sendChallenge
                        throw new FailedLoginException("Failed to create principal: " + e.getMessage());
                    }
                }
                Object password = sharedState.get("javax.security.auth.login.password");
                if (password instanceof X509Certificate) {
                    if (trace) {
                        log.trace("**** Credential is a X509Certificate");
                    }
                    certificateCredential = (X509Certificate) password;
                } else if (password instanceof X509Certificate[]) {
                    if (trace) {
                        log.trace("**** Credential is an X509Certificate array");
                    }
                    certificateCredential = ((X509Certificate[]) password)[0];
                } else if (password instanceof DatawaveCredential) {
                    if (trace) {
                        log.trace("**** Credential is a DatawaveCredential");
                    }
                    datawaveCredential = (DatawaveCredential) password;
                    certificateCredential = datawaveCredential.getCertificate();
                } else {
                    log.warn("Login failed due to unknown password.");
                    return false;
                }
            } else {
                DatawaveCredential credential = getDatawaveCredential();
                loginOk = validateCredential(credential);
                if (trace) {
                    log.trace("User '" + identity + "' authenticated, loginOk=" + loginOk);
                    log.debug("exit: login()");
                }
            }

            if (disallowlistUserRole != null && loginOk && identity != null) {
                DatawavePrincipal principal = (DatawavePrincipal) getIdentity();

                if (principal.getProxiedUsers().stream().anyMatch(u -> u.getRoles().contains(disallowlistUserRole))) {
                    loginOk = false; // this is critical as it is what the parent class uses to actually deny login
                    String message = "Login denied for " + principal.getUserDN() + " due to membership in the deny-access group " + disallowlistUserRole;
                    log.debug(message);
                    // should result in a FORBIDDEN (403) response code in DatawaveAuthenticationMechanism.sendChallenge
                    throw new AccountLockedException(message);
                }
            }

            /**
             * Check terminal server to verify that it can directly connect. This requires the positive check that the terminal server has an approved role
             * AuthorizedServer or AuthorizedQueryServer. If TerminalServer does not have the correct role we will fail the login. Currently this only checks
             * for UserType.SERVER. However the predicate could be modified to include a check for UserType.USER. Logic just streams through the list of
             * ProxiedUsers to get the last element (terminal server). Make sure it's not null and if it is a server then we want it to have a direct role.
             */

            DatawavePrincipal principal = (DatawavePrincipal) getIdentity();
            DatawaveUser terminalServer = principal.getProxiedUsers().stream().reduce((prev, next) -> next).orElse(null);
            // terminalUser should never be null, at a minimum the PrincipalUser should be in the chain
            if (terminalServer == null || (terminalServer.getUserType() == DatawaveUser.UserType.SERVER
                            && !(terminalServer.getRoles().stream().anyMatch(directRoles::contains)))) {
                loginOk = false; // this is critical as it is what the parent class uses to actually deny login
                String message = "Login denied for terminal server " + terminalServer.getDn() + " due to missing role. Needs one of: " + directRoles
                                + " but has roles: " + terminalServer.getRoles();
                log.debug(message);
                // should result in a FORBIDDEN (403) response code in DatawaveAuthenticationMechanism.sendChallenge
                throw new FailedLoginException(message);
            }
        } catch (LoginException e) {
            log.warn("Login failed due to LoginException: " + e.getMessage(), e);
            throw e;
        } catch (RuntimeException e) {
            log.warn("Login failed due to RuntimeException: " + e.getMessage(), e);
            // should result in a SERVICE_UNAVAILABLE (503) response code in DatawaveAuthenticationMechanism.sendChallenge
            throw new LoginException(e.getMessage());
        }
        return true;
    }

    protected DatawaveCredential getDatawaveCredential() throws LoginException {
        if (trace) {
            log.trace("enter: getDatawaveCredential()");
        }
        if (callbackHandler == null) {
            log.error("Error: no CallbackHandler available to collect authentication information");
            throw new LoginException("Error: no CallbackHandler available to collect authentication information");
        }
        NameCallback nc = new NameCallback("Username: ");
        ObjectCallback oc = new ObjectCallback("Credentials: ");
        Callback callbacks[] = {nc, oc};
        try {
            callbackHandler.handle(callbacks);

            // We use a custom authentication mechanism to convert the certificate into a DatawaveCredential.
            // The custom authentication mechanism checks the request for the X-ProxiedEntitiesChain/X-ProxiedIssuersChain
            // headers and uses them along with either the certificate subject and issuer DNs or trusted headers
            // (supplied by the load balancer) containing the subject and issuer DNs to construct a list of entities.
            Object tmpCreds = oc.getCredential();
            if (tmpCreds instanceof DatawaveCredential) {
                return (DatawaveCredential) tmpCreds;
            } else {
                String credentialClass = tmpCreds == null ? "null" : tmpCreds.getClass().getName();
                String msg = "Unknown credential class " + credentialClass + " is not a " + DatawaveCredential.class.getName();
                log.warn(msg);
                throw new LoginException(msg);
            }
        } catch (IOException e) {
            log.debug("Failed to invoke callback", e);
            throw new LoginException("Failed to invoke callback: " + e);
        } catch (UnsupportedCallbackException uce) {
            log.debug("CallbackHandler does not support: " + uce.getCallback());
            throw new LoginException("CallbackHandler does not support: " + uce.getCallback());
        } finally {
            if (trace) {
                log.trace("exit: getDatawaveCredential()");
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected boolean validateCredential(DatawaveCredential credential) throws LoginException {
        if (trace) {
            log.trace("enter: validateCredential");
        }

        datawaveCredential = credential;

        String alias = credential.getUserName();
        if (trace) {
            log.trace("alias = " + alias);
        }
        if (StringUtil.isNullOrEmpty(alias)) {
            identity = unauthenticatedIdentity;
            log.trace("Authenticating as unauthenticatedIdentity=" + identity);
        }
        if (trace) {
            log.trace("identity = " + identity);
        }
        if (identity == null) {
            if (credential.getCertificate() != null || (!trustedHeaderLogin && !jwtHeaderLogin)) {
                if (!validateCertificateCredential(credential)) {
                    log.debug("Bad credential for alias=" + credential.getUserName());
                    throw new CredentialException("Validation of credential failed");
                }
            }

            if (!jwtHeaderLogin || credential.getJwtToken() == null) {
                try {
                    identity = new DatawavePrincipal(datawaveUserService.lookup(credential.getEntities()));
                } catch (AuthorizationException e) {
                    Throwable cause = e.getCause();
                    String message = cause != null ? cause.getMessage() : e.getMessage();
                    log.debug("Failing login due to datawave user service exception " + e.getMessage(), e);
                    // should result in a SERVICE_UNAVAILABLE (503) response code in DatawaveAuthenticationMechanism.sendChallenge
                    throw new LoginException("Unable to authenticate: " + message);
                } catch (Exception e) {
                    log.debug("Failing login due to datawave user service exception " + e.getMessage(), e);
                    // should result in a SERVICE_UNAVAILABLE (503) response code in DatawaveAuthenticationMechanism.sendChallenge
                    throw new LoginException("Unable to authenticate: " + e.getMessage());
                }
            } else {
                try {
                    identity = new DatawavePrincipal(jwtTokenHandler.createUsersFromToken(credential.getJwtToken()));
                } catch (Exception e) {
                    log.debug("Failing login due to JWT token exception " + e.getMessage(), e);
                    // should result in an UNAUTHORIZED (401) response code in DatawaveAuthenticationMechanism.sendChallenge
                    throw new CredentialException("Unable to authenticate: " + e.getMessage());
                }
            }
        }
        if (getUseFirstPass()) {
            sharedState.put("javax.security.auth.login.name", alias);
            sharedState.put("javax.security.auth.login.password", credential.getCertificate());
        }
        if (trace) {
            log.trace("exit: validateCredential");
        }
        return true;
    }

    protected boolean validateCertificateCredential(DatawaveCredential credential) {
        if (trace) {
            log.trace("enter: validateCertificateCredential(DatawaveCredential)[" + verifier + "]");
        }
        boolean isValid = false;
        KeyStore keyStore = null;
        KeyStore trustStore = null;
        if (domain != null) {
            keyStore = domain.getKeyStore();
            trustStore = domain.getTrustStore();
        }
        if (trustStore == null) {
            trustStore = keyStore;
        }
        if (verifier != null) {
            String issuerSubjectDn = credential.getCertificate().getIssuerX500Principal().getName();
            if (datawaveVerifier) {
                if (((DatawaveCertVerifier) verifier).isIssuerSupported(issuerSubjectDn, trustStore)) {
                    isValid = verifier.verify(credential.getCertificate(), issuerSubjectDn, keyStore, trustStore);
                } else {
                    if (trace) {
                        log.trace("Unsupported issuer: " + issuerSubjectDn);
                    }
                }
            } else {
                if (trace) {
                    log.trace("Validating using non datawave cert verifier.");
                }
                isValid = verifier.verify(credential.getCertificate(), issuerSubjectDn, keyStore, trustStore);
            }
            if (trace) {
                log.trace("Cert Validation result : " + isValid);
            }
        } else if (credential.getCertificate() != null) {
            isValid = true;
        } else {
            log.warn("Certificate is null - unable to validate");
        }
        if (trace) {
            log.trace("exit: validateCertificateCredential(DatawaveCredential)");
        }
        return isValid;
    }
}
