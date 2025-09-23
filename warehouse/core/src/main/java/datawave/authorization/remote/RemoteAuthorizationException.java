package datawave.authorization.remote;

import datawave.security.authorization.AuthorizationException;

/**
 * a special AuthorizationException to indicate a timeout accessing a RemoteUserOperations
 */
public class RemoteAuthorizationException extends AuthorizationException {
    public RemoteAuthorizationException(String message, Exception e) {
        super(message, e);
    }
}
