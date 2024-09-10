package datawave.security.login;

import java.io.IOException;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.jboss.logging.Logger;
import org.jboss.security.ClientLoginModule;
import org.jboss.security.SimplePrincipal;
import org.jboss.security.auth.callback.ObjectCallback;

import datawave.security.auth.DatawaveCredential;

/**
 * A {@link LoginModule} intended for use with {@link ClientLoginModule}. Normally, when a client wishes to make a call to a secured EJB, it authenticates to
 * the {@link ClientLoginModule}, which saves the supplied username/password and then passes them along when the secured call is attempted and requests at login
 * time. However, if the eventual server login module doesn't want a password as its credential (e.g., because it wants an {@link X509Certificate}) then this
 * approach doesn't work. This module bridges that gap by requesting the username via a {@link NameCallback} and a credential using an {@link ObjectCallback}.
 * These are saved in the shared state. When used with the {@link ClientLoginModule} with password-stacking set to useFirstPass, the credentials supplied to
 * this module will be passed along by the client login module.
 */
public class ClientCertLoginModule implements LoginModule {

    private static Logger log = Logger.getLogger(ClientCertLoginModule.class);
    private Map<String,Object> sharedState;
    private boolean trace;

    private CallbackHandler callbackHandler;

    @SuppressWarnings("unchecked")
    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String,?> sharedState, Map<String,?> options) {

        trace = log.isTraceEnabled();
        this.sharedState = (Map<String,Object>) sharedState;
        this.callbackHandler = callbackHandler;
    }

    @Override
    public boolean login() throws LoginException {
        if (trace)
            log.trace("Begin login");

        if (callbackHandler == null)
            throw new LoginException("Error: no CallbackHandler available for collecting authentication information.");

        NameCallback nc = new NameCallback("DN");
        ObjectCallback oc = new ObjectCallback("Certificate");
        Callback[] callbacks = new Callback[] {nc, oc};

        try {
            callbackHandler.handle(callbacks);

            Principal loginPrincipal = new SimplePrincipal(nc.getName());
            Object credential = oc.getCredential();
            X509Certificate loginCredential;
            if (credential instanceof X509Certificate) {
                loginCredential = (X509Certificate) oc.getCredential();
            } else {
                String clazz = (credential == null) ? "null" : credential.getClass().getName();
                throw new LoginException("Supplied credential is a " + clazz + " but needs to be an " + X509Certificate.class.getName());
            }

            sharedState.put("javax.security.auth.login.name", loginPrincipal);
            sharedState.put("javax.security.auth.login.password", new DatawaveCredential(loginCredential, null, null));

        } catch (IOException e) {
            LoginException le = new LoginException(e.toString());
            le.initCause(e);
            throw le;
        } catch (UnsupportedCallbackException e) {
            LoginException le = new LoginException("Error: " + e.getCallback() + ", not available to use this callback.");
            le.initCause(e);
            throw le;
        }

        if (trace)
            log.trace("End login");
        return true;
    }

    @Override
    public boolean commit() throws LoginException {
        return true;
    }

    @Override
    public boolean abort() throws LoginException {
        return true;
    }

    @Override
    public boolean logout() throws LoginException {
        return true;
    }

}
