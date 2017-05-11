package datawave.webservice.security;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * A simple extension of {@link LoginContext} that implements {@link AutoCloseable} and calls {@link LoginContext#logout()} upon close. This allows this version
 * of LoginContext to be used in a try-with-resources statement.
 */
public class CloseableLoginContext extends LoginContext implements AutoCloseable {
    public CloseableLoginContext(String name) throws LoginException {
        super(name);
    }
    
    public CloseableLoginContext(String name, Subject subject) throws LoginException {
        super(name, subject);
    }
    
    public CloseableLoginContext(String name, CallbackHandler callbackHandler) throws LoginException {
        super(name, callbackHandler);
    }
    
    public CloseableLoginContext(String name, Subject subject, CallbackHandler callbackHandler) throws LoginException {
        super(name, subject, callbackHandler);
    }
    
    public CloseableLoginContext(String name, Subject subject, CallbackHandler callbackHandler, Configuration config) throws LoginException {
        super(name, subject, callbackHandler, config);
    }
    
    @Override
    public void close() throws LoginException {
        logout();
    }
}
