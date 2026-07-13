package datawave.test.iter;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;

public class TestIteratorEnv implements IteratorEnvironment {

    private IteratorScope scope;
    private final AccumuloConfiguration conf;

    public TestIteratorEnv() {
        this.conf = DefaultConfiguration.getInstance();
    }

    public void setScope(IteratorScope scope) {
        this.scope = scope;
    }

    @Override
    public IteratorScope getIteratorScope() {
        return scope;
    }

    @Override
    public boolean isFullMajorCompaction() {
        return scope.equals(IteratorScope.majc);
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
    public boolean isUserCompaction() {
        return false;
    }

    @Override
    public PluginEnvironment getPluginEnv() {
        return new TestPluginEnv();
    }

    @Override
    public TableId getTableId() {
        return null;
    }

    @Override
    public boolean isRunningLowOnMemory() {
        return false;
    }
}
