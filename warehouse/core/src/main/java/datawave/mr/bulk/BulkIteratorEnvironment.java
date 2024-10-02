package datawave.mr.bulk;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;

public class BulkIteratorEnvironment implements IteratorEnvironment {

    private IteratorScope scope;

    public BulkIteratorEnvironment(IteratorScope scope) {
        this.scope = scope;
    }

    @Override
    public IteratorScope getIteratorScope() {
        return scope;
    }

    @Override
    public boolean isFullMajorCompaction() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isUserCompaction() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Authorizations getAuthorizations() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IteratorEnvironment cloneWithSamplingEnabled() {
        throw new SampleNotPresentException();
    }

    @Override
    public boolean isSamplingEnabled() {
        return false;
    }

    @Override
    public SamplerConfiguration getSamplerConfiguration() {
        return null;
    }

    @Override
    public PluginEnvironment getPluginEnv() {
        return null;
    }

}
