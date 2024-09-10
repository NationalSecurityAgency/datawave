package datawave.util;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

public class MockInitialContextFactory implements InitialContextFactory {
    private static Context mockContext = null;

    public static void setMockContext(Context context) {
        mockContext = context;
    }

    @Override
    public Context getInitialContext(Hashtable<?,?> environment) throws NamingException {
        if (mockContext == null)
            throw new IllegalStateException("mock context must be set first");

        return mockContext;
    }
}
