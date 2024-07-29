package something.somethingelse.finalsomething;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.util.ConfigurationImpl;
import org.apache.accumulo.server.ServerContext;

import java.io.IOException;

public class SingleConfigurationPluginEnvironment implements PluginEnvironment {


    private final PluginEnvironment.Configuration conf;

    public SingleConfigurationPluginEnvironment() {
        this.conf = null;
    }

    public SingleConfigurationPluginEnvironment(ServerContext context) {
        this.conf = new ConfigurationImpl(context.getConfiguration());
    }

    @Override
    public Configuration getConfiguration() {
        return conf;
    }

    @Override
    public Configuration getConfiguration(TableId tableId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTableName(TableId tableId) throws TableNotFoundException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T instantiate(String className, Class<T> base) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T instantiate(TableId tableId, String className, Class<T> base) throws ReflectiveOperationException, IOException {
        throw new UnsupportedOperationException();
    }
}
