package datawave.configuration;

import org.apache.deltaspike.core.api.config.PropertyFileConfig;

/**
 * A property file configuration that loads config properties from all /META-INF/datawave-configuration.properties files on the class path.
 */
@SuppressWarnings("UnusedDeclaration")
public class DatawavePropertyFileConfigSource implements PropertyFileConfig {
    private static final String PROPERTY_FILE_NAME = "META-INF/datawave-configuration.properties";

    @Override
    public String getPropertyFileName() {
        return PROPERTY_FILE_NAME;
    }

    @Override
    public boolean isOptional() {
        return true;
    }
}
