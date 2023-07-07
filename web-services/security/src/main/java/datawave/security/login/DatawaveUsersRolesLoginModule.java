package datawave.security.login;

import java.security.Principal;
import java.security.acl.Group;
import java.util.Enumeration;

import javax.security.auth.login.LoginException;

import org.jboss.logging.Logger;
import org.jboss.security.SimplePrincipal;
import org.jboss.security.auth.spi.UsersRolesLoginModule;

import datawave.security.util.DnUtils;

/**
 * A specialized version of {@link UsersRolesLoginModule} that fails the login if there are no roles for a given user. The parent module will take the supplied
 * credential, which in our case will be a {@link datawave.security.auth.DatawaveCredential}, and turn it to a string, so the password used in the associated
 * properties file must match the {@code toString()} version of the credential.
 */
public class DatawaveUsersRolesLoginModule extends UsersRolesLoginModule {
    private ThreadLocal<Boolean> createSimplePrincipal = new ThreadLocal<>();

    public DatawaveUsersRolesLoginModule() {
        log = Logger.getLogger(getClass());
    }

    @Override
    public boolean login() throws LoginException {
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
            if (log.isTraceEnabled()) {
                log.trace("Creating simple principal, passing username: " + username);
            }
            return new SimplePrincipal(username);
        } else {
            String normalizedUsername = normalizeUsername(username);
            if (log.isTraceEnabled()) {
                log.trace("original username: " + username + " normalizedUsername: " + normalizedUsername);
            }
            return super.createIdentity(normalizedUsername);
        }
    }

    protected static String normalizeUsername(String username) {
        StringBuilder result = new StringBuilder();

        String[] splitDns = DnUtils.splitProxiedSubjectIssuerDNs(username);
        for (int i = 0; i < splitDns.length; i++) {
            if (i > 0) {
                result.append("<");
                result.append(DnUtils.normalizeDN(splitDns[i]));
                result.append(">");
            } else {
                result.append(DnUtils.normalizeDN(splitDns[i]));
            }
        }
        return result.toString();
    }
}
