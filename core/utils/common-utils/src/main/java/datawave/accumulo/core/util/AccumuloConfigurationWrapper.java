package datawave.accumulo.core.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.PluginEnvironment;
import org.apache.accumulo.core.conf.AccumuloConfiguration;

/**
 * A utility class that wraps an AccumuloConfiguration and implements PluginEnvironment.Configuration. This replaces the non-public
 * org.apache.accumulo.core.util.ConfigurationImpl class.
 */
public class AccumuloConfigurationWrapper implements PluginEnvironment.Configuration {

    private final AccumuloConfiguration conf;

    public AccumuloConfigurationWrapper(AccumuloConfiguration conf) {
        this.conf = conf;
    }

    @Override
    public boolean isSet(String key) {
        return conf.get(key) != null;
    }

    @Override
    public String get(String key) {
        return conf.get(key);
    }

    @Override
    public Map<String,String> getWithPrefix(String prefix) {
        Map<String,String> result = new HashMap<>();
        for (Map.Entry<String,String> entry : conf) {
            if (entry.getKey().startsWith(prefix)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    @Override
    public Map<String,String> getCustom() {
        return getWithPrefix("general.custom.");
    }

    @Override
    public String getCustom(String keySuffix) {
        return get("general.custom." + keySuffix);
    }

    @Override
    public Map<String,String> getTableCustom() {
        return getWithPrefix("table.custom.");
    }

    @Override
    public String getTableCustom(String keySuffix) {
        return get("table.custom." + keySuffix);
    }

    @Override
    public Iterator<Map.Entry<String,String>> iterator() {
        return conf.iterator();
    }

    @Override
    public <T> Supplier<T> getDerived(Function<PluginEnvironment.Configuration,T> computeDerivedValue) {
        T value = computeDerivedValue.apply(this);
        return () -> value;
    }
}
