package datawave.ingest.config;

import java.util.ArrayList;
import java.util.ServiceLoader;

import com.google.common.collect.Iterables;

/**
 * A factory for loading the {@link IngestConfiguration} implementation specified by the current packaging using the {@link ServiceLoader} mechanism. Note that
 * only one implementation of {@link IngestConfiguration} must by defined, or else this class will throw an error.
 */
public class IngestConfigurationFactory {
    private static IngestConfiguration ingestConfiguration;
    static {
        ServiceLoader<IngestConfiguration> loader = ServiceLoader.load(IngestConfiguration.class);
        ArrayList<IngestConfiguration> allConfigurations = new ArrayList<>();
        Iterables.addAll(allConfigurations, loader);

        if (allConfigurations.isEmpty()) {
            throw new IllegalStateException("No IngestConfiguration providers found.");
        } else if (allConfigurations.size() > 1) {
            throw new IllegalStateException("Multiple IngestConfiguration providers found: " + allConfigurations);
        } else {
            ingestConfiguration = allConfigurations.get(0);
        }
    }

    public static IngestConfiguration getIngestConfiguration() {
        return ingestConfiguration;
    }
}
