package datawave.mr.bulk;

import java.io.IOException;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;

public class BulkIteratorEnvironment implements IteratorEnvironment {

    private IteratorScope scope;
    private AccumuloConfiguration conf;

    public BulkIteratorEnvironment(IteratorScope scope) {
        this.scope = scope;
        this.conf = DefaultConfiguration.getInstance();
    }

    /**
     * @deprecated since 2.0.0. This method was using an unstable non-public type. Use {@link #getPluginEnv()}
     */
    @Deprecated(since = "2.0.0")
    @Override
    public AccumuloConfiguration getConfig() {
        return conf;
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
    public void registerSideChannel(SortedKeyValueIterator<Key,Value> iter) {
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
    public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String mapFileName) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ServiceEnvironment getServiceEnv() {
        return null;
    }

    @Override
    public PluginEnvironment getPluginEnv() {
        return null;
    }

}
