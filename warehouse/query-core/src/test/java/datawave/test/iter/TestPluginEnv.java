package datawave.test.iter;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.util.ConfigurationImpl;

public class TestPluginEnv implements PluginEnvironment {

    private final AccumuloConfiguration conf = DefaultConfiguration.getInstance();

    @Override
    public Configuration getConfiguration() {
        return new ConfigurationImpl(conf);
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
    public <T> T instantiate(String className, Class<T> base) throws Exception {
        return null;
    }

    @Override
    public <T> T instantiate(TableId tableId, String className, Class<T> base) throws Exception {
        return null;
    }
}
