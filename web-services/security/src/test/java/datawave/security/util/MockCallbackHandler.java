package datawave.security.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.jboss.security.auth.callback.ObjectCallback;

public class MockCallbackHandler implements CallbackHandler {
    public String name;
    public Object credential;

    private String nameCallbackPrompt;
    private String credentialsCallbackPrompt;

    public MockCallbackHandler(String nameCallbackPrompt, String credentialsCallbackPrompt) {
        this.nameCallbackPrompt = nameCallbackPrompt;
        this.credentialsCallbackPrompt = credentialsCallbackPrompt;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        assertEquals(2, callbacks.length);
        assertEquals(NameCallback.class, callbacks[0].getClass());
        assertEquals(ObjectCallback.class, callbacks[1].getClass());

        NameCallback nc = (NameCallback) callbacks[0];
        ObjectCallback oc = (ObjectCallback) callbacks[1];

        assertEquals(nameCallbackPrompt, nc.getPrompt());
        assertEquals(credentialsCallbackPrompt, oc.getPrompt());

        nc.setName(name);
        oc.setCredential(credential);
    }
}
