package datawave.accumulo.inmemory;

import java.util.Map;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.client.PluginEnvironment.Configuration;
import org.apache.accumulo.core.data.TableId;
import org.mockito.Mockito;

/**
 * A mock of a {@link org.apache.accumulo.core.client.PluginEnvironment} using mockito.
 */
public final class MockPluginEnvironment {
    private MockPluginEnvironment() {
        // Utility only, do not instantiate
    }

    /**
     * Creates a new mock PluginEnvironment.
     *
     * @param inMemoryTable
     *            The inMemoryTable to use for loading a configuration
     * @return A plugin environment
     */
    public static PluginEnvironment newInstance(InMemoryTable inMemoryTable) {
        PluginEnvironment pluginEnv = Mockito.mock(PluginEnvironment.class);
        Mockito.when(pluginEnv.getConfiguration(TableId.of(inMemoryTable.getTableId()))).thenReturn(Configuration.from(inMemoryTable.settings, true));
        Mockito.when(pluginEnv.getConfiguration()).thenReturn(Configuration.from(Map.of(), true));

        return pluginEnv;
    }
}
