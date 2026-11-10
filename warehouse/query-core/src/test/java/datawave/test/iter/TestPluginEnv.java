package datawave.test.iter;

import java.util.Map;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.TableId;

public class TestPluginEnv implements PluginEnvironment {

    private final Configuration conf = Configuration.from(Map.of(), true);

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
    public <T> T instantiate(String className, Class<T> base) throws ReflectiveOperationException {
        return null;
    }

    @Override
    public <T> T instantiate(TableId tableId, String className, Class<T> base) throws ReflectiveOperationException {
        return null;
    }
}
