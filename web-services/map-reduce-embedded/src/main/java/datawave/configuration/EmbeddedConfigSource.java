package datawave.configuration;

import org.apache.deltaspike.core.api.config.PropertyFileConfig;

/**
 * A property file configuration that loads config properties embedded-configuration.properties in the JobContext classpath
 */
public class EmbeddedConfigSource implements PropertyFileConfig {
    private static final String PROPERTY_FILE_NAME = "embedded-configuration.properties";

    @Override
    public String getPropertyFileName() {
        return PROPERTY_FILE_NAME;
    }

    @Override
    public boolean isOptional() {
        return true;
    }
}
