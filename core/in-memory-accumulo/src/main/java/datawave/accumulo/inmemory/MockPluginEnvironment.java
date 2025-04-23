package datawave.accumulo.inmemory;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.util.ConfigurationImpl;
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
     * @param conf
     *            The accumulo configuration
     * @return A plugin environment
     */
    public static PluginEnvironment newInstance(AccumuloConfiguration conf) {
        PluginEnvironment pluginEnv = Mockito.mock(PluginEnvironment.class);
        Mockito.when(pluginEnv.getConfiguration()).thenReturn(new ConfigurationImpl(conf));
        return pluginEnv;
    }
}
