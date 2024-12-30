package datawave.modification.configuration;

import java.util.Map;

public class ModificationConfiguration {

    private String tableName = null;
    private String poolName = null;
    private Map<String,ModificationServiceConfiguration> configurations = null;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPoolName() {
        return poolName;
    }

    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }

    public Map<String,ModificationServiceConfiguration> getConfigurations() {
        return configurations;
    }

    public void setConfigurations(Map<String,ModificationServiceConfiguration> configurations) {
        this.configurations = configurations;
    }

    public ModificationServiceConfiguration getConfiguration(String serviceName) {
        return this.configurations.get(serviceName);
    }

    public static class Factory {
        // this method is overriden by spring to fetch a wired ModificationConfiguration (with injected configurations)
        public ModificationConfiguration createModificationConfiguration() {
            return new ModificationConfiguration();
        }
    }

}
