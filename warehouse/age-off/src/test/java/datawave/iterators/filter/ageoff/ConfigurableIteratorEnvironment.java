package datawave.iterators.filter.ageoff;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ConfigurationImpl;

public class ConfigurableIteratorEnvironment implements IteratorEnvironment {

    private static final TableId FAKE_ID = TableId.of("FAKE");
    private IteratorUtil.IteratorScope scope;
    private PluginEnvironment.Configuration conf;

    public ConfigurableIteratorEnvironment() {
        scope = null;
        conf = PluginEnvironment.Configuration.from(Map.of(), false);
    }

    public ConfigurableIteratorEnvironment(PluginEnvironment.Configuration conf, IteratorUtil.IteratorScope scope) {
        this.conf = conf;
        this.scope = scope;

    }

    public void setConf(PluginEnvironment.Configuration conf) {
        this.conf = conf;
    }

    public void setScope(IteratorUtil.IteratorScope scope) {
        this.scope = scope;
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
    public TableId getTableId() {
        return FAKE_ID;
    }

    @Override
    public PluginEnvironment getPluginEnv() {
        return new PluginEnvironment() {

            @Override
            public Configuration getConfiguration() {
                return conf;
            }

            @Override
            public Configuration getConfiguration(TableId tableId) {
                return conf;
            }

            @Override
            public String getTableName(TableId tableId) throws TableNotFoundException {
                return null;
            }

            @Override
            public <T> T instantiate(String s, Class<T> aClass) {
                return null;
            }

            @Override
            public <T> T instantiate(TableId tableId, String s, Class<T> aClass) {
                return null;
            }
        };
    }
}
