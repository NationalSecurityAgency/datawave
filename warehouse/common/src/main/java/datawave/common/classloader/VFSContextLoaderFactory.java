package datawave.common.classloader;

import org.apache.accumulo.core.classloader.DefaultContextClassLoaderFactory;
import org.apache.accumulo.core.conf.AccumuloConfiguration;

public class VFSContextLoaderFactory extends DefaultContextClassLoaderFactory {
    public VFSContextLoaderFactory(AccumuloConfiguration accConf) {
        super(accConf);
    }

    @Override
    public ClassLoader getClassLoader(String contextName) {
        return new ErrorSafeDelegatingClassLoader(super.getClassLoader(contextName));
    }
}
