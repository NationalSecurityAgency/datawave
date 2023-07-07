package datawave.security.login;

import java.io.IOException;
import java.security.Principal;
import java.security.acl.Group;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;

import org.jboss.logging.Logger;
import org.jboss.security.PicketBoxLogger;
import org.jboss.security.PicketBoxMessages;
import org.jboss.security.SimplePrincipal;
import org.jboss.security.auth.callback.ObjectCallback;
import org.jboss.security.auth.spi.CertRolesLoginModule;

import datawave.security.auth.DatawaveCredential;

/**
 * A specialized version of {@link CertRolesLoginModule} that fails the login if there are no roles for a given user. Even if the user has a valid certificate,
 * if they don't appear in the roles file, we'll fail the login.
 */
public class DatawaveCertRolesLoginModule extends CertRolesLoginModule {

    private static final String TRUSTED_HEADER_OPT = "trustedHeaderLogin";

    private ThreadLocal<Boolean> createSimplePrincipal = new ThreadLocal<>();
    private boolean trustedHeaderLogin;

    public DatawaveCertRolesLoginModule() {
        log = Logger.getLogger(getClass());
    }

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String,?> sharedState, Map<String,?> options) {
        addValidOptions(new String[] {TRUSTED_HEADER_OPT});
        super.initialize(subject, callbackHandler, sharedState, options);

        String option = (String) options.get(TRUSTED_HEADER_OPT);
        if (option != null)
            trustedHeaderLogin = Boolean.valueOf(option);
    }

    @Override
    public boolean login() throws LoginException {
        // This login module should do nothing if we're using the trusted header login (since a cert won't be supplied)
        // The reason for having this option is so that the module can be on the stack and support either configuration --
        // normal SSL certificate or SSL-terminated trusted header
        if (trustedHeaderLogin) {
            log.trace("trustedHeaderLogin is true - returning false for login success");
            return false;
        }

        boolean success = super.login();

        int roleCount = 0;
        Group[] roleSets = getRoleSets();
        if (roleSets != null) {
            for (Group roleSet : roleSets) {
                for (Enumeration<? extends Principal> e = roleSet.members(); e.hasMoreElements(); e.nextElement()) {
                    ++roleCount;
                }
            }
        }

        // Fail the login if there are no roles. This way we can try
        // another module potentially.
        if (roleCount == 0) {
            loginOk = false;
            success = false;
        }

        return success;
    }

    @Override
    protected Group[] getRoleSets() throws LoginException {
        // Set a thread local to indicate that we should create a SimplePrincipal when asked to create an identity. This is needed
        // because the parent class uses a utility to create the groups and that utility delegates back to our own createIdentity
        // method for adding each member to the group. Since our group members won't be valid names for use with DatawavePrincipal
        // we need to ensure that we use a SimplePrincipal to represent the group members instead.
        createSimplePrincipal.set(Boolean.TRUE);
        try {
            return super.getRoleSets();
        } finally {
            createSimplePrincipal.remove();
        }
    }

    @Override
    protected Principal createIdentity(String username) throws Exception {
        // Create a simple principal if our thread-local indicates we are supposed to,
        // which only happens during the getRolesSets method call.
        if (Boolean.TRUE.equals(createSimplePrincipal.get())) {
            return new SimplePrincipal(username);
        } else {
            return super.createIdentity(DatawaveUsersRolesLoginModule.normalizeUsername(username));
        }
    }

    // Copied from org.jboss.security.auth.spi.BaseCertLoginModule to handle the addition of DatawaveCredential
    @Override
    protected Object[] getAliasAndCert() throws LoginException {
        PicketBoxLogger.LOGGER.traceBeginGetAliasAndCert();
        Object[] info = {null, null};
        // prompt for a username and password
        if (callbackHandler == null) {
            throw PicketBoxMessages.MESSAGES.noCallbackHandlerAvailable();
        }
        NameCallback nc = new NameCallback("Alias: ");
        ObjectCallback oc = new ObjectCallback("Certificate: ");
        Callback[] callbacks = {nc, oc};
        String alias;
        X509Certificate cert = null;
        X509Certificate[] certChain;
        try {
            callbackHandler.handle(callbacks);
            alias = nc.getName();
            Object tmpCert = oc.getCredential();
            if (tmpCert != null) {
                if (tmpCert instanceof X509Certificate) {
                    cert = (X509Certificate) tmpCert;
                    PicketBoxLogger.LOGGER.traceCertificateFound(cert.getSerialNumber().toString(16), cert.getSubjectDN().getName());
                } else if (tmpCert instanceof X509Certificate[]) {
                    certChain = (X509Certificate[]) tmpCert;
                    if (certChain.length > 0)
                        cert = certChain[0];
                } else if (tmpCert instanceof DatawaveCredential) {
                    DatawaveCredential dwCredential = (DatawaveCredential) tmpCert;
                    cert = dwCredential.getCertificate();
                    if (cert == null) {
                        String msg = "No certificate supplied with login credential for " + dwCredential.getUserName();
                        log.warn(msg);
                        throw new LoginException(msg);
                    }
                    PicketBoxLogger.LOGGER.traceCertificateFound(cert.getSerialNumber().toString(16), cert.getSubjectDN().getName());
                } else {
                    throw PicketBoxMessages.MESSAGES.unableToGetCertificateFromClass(tmpCert.getClass());
                }
            } else {
                PicketBoxLogger.LOGGER.warnNullCredentialFromCallbackHandler();
            }
        } catch (IOException e) {
            LoginException le = PicketBoxMessages.MESSAGES.failedToInvokeCallbackHandler();
            le.initCause(e);
            throw le;
        } catch (UnsupportedCallbackException uce) {
            LoginException le = new LoginException();
            le.initCause(uce);
            throw le;
        }

        info[0] = alias;
        info[1] = cert;
        PicketBoxLogger.LOGGER.traceEndGetAliasAndCert();
        return info;
    }
}
