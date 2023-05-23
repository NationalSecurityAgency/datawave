package datawave.common.classloader;

import org.apache.accumulo.core.classloader.DefaultContextClassLoaderFactory;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.spi.common.ContextClassLoaderFactory;

public class ErrorSafeVfsClassLoaderFactory implements ContextClassLoaderFactory {

    private DefaultContextClassLoaderFactory delegate;

    @Override
    public ClassLoader getClassLoader(String contextName) {
        return new ErrorSafeDelegatingClassLoader(delegate.getClassLoader(contextName));
    }

    /**
     * TODO: Update this to init(ContextClassLoaderEnvironment env) once accumulo PR #3400 is merged
     */
    public void setConfiguration(final AccumuloConfiguration conf) {
        delegate = new DefaultContextClassLoaderFactory(conf);
    }
}
