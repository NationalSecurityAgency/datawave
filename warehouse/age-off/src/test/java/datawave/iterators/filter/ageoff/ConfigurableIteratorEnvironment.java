package datawave.iterators.filter.ageoff;

import java.io.IOException;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ConfigurationImpl;

public class ConfigurableIteratorEnvironment implements IteratorEnvironment {

    private IteratorUtil.IteratorScope scope;
    private AccumuloConfiguration conf;

    public ConfigurableIteratorEnvironment() {
        scope = null;
        conf = null;
    }

    public ConfigurableIteratorEnvironment(AccumuloConfiguration conf, IteratorUtil.IteratorScope scope) {
        this.conf = conf;
        this.scope = scope;

    }

    public void setConf(AccumuloConfiguration conf) {
        this.conf = conf;
    }

    public void setScope(IteratorUtil.IteratorScope scope) {
        this.scope = scope;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> reserveMapFileReader(String s) throws IOException {
        return null;
    }

    @Override
    public AccumuloConfiguration getConfig() {
        return conf;
    }

    @Override
    public IteratorUtil.IteratorScope getIteratorScope() {
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
    public void registerSideChannel(SortedKeyValueIterator<Key,Value> sortedKeyValueIterator) {
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
        return new PluginEnvironment() {

            @Override
            public Configuration getConfiguration() {
                return null;
            }

            @Override
            public Configuration getConfiguration(TableId tableId) {
                return new ConfigurationImpl(conf);
            }

            @Override
            public String getTableName(TableId tableId) throws TableNotFoundException {
                return null;
            }

            @Override
            public <T> T instantiate(String s, Class<T> aClass) throws Exception {
                return null;
            }

            @Override
            public <T> T instantiate(TableId tableId, String s, Class<T> aClass) throws Exception {
                return null;
            }
        };
    }
}
