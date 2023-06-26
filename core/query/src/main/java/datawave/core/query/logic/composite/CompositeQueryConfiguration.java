package datawave.core.query.logic.composite;

import datawave.core.query.configuration.GenericQueryConfiguration;

import java.io.Serializable;
import java.util.Map;

public class CompositeQueryConfiguration extends GenericQueryConfiguration implements Serializable {

    private Map<String,GenericQueryConfiguration> configs;

    public CompositeQueryConfiguration(String queryString, Map<String,GenericQueryConfiguration> configs) {
        this.setQueryString(queryString);
        this.configs = configs;
    }

    public Map<String,GenericQueryConfiguration> getConfigs() {
        return configs;
    }

    public void setConfigs(Map<String,GenericQueryConfiguration> configs) {
        this.configs = configs;
    }
}
