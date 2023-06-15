package datawave.ingest.mapreduce.job;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.TreeMap;

/**
 * A Hadoop configuration that "overrides" properties by removing the given prefix from any configurations that start with it.
 *
 * Example: If created with the prefix of "test", then the property "test.table.name" would be changed to "table.name".
 *
 * This is used for overriding configurations in ingest components.
 */
public class OverridingConfiguration extends Configuration {
    public OverridingConfiguration(String prefix, Configuration base) {
        Map<String,String> overrides = new TreeMap<>();

        for (Map.Entry<String,String> property : base) {
            String k = property.getKey();
            if (k.startsWith(prefix + ".")) {
                k = k.substring(k.indexOf('.') + 1);
                overrides.put(k, property.getValue());
            } else {
                set(k, property.getValue());
            }
        }

        for (Map.Entry<String,String> entry : overrides.entrySet()) {
            set(entry.getKey(), entry.getValue());
        }
    }
}
