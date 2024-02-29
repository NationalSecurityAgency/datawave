package datawave.query.config;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class FederatedShardQueryConfiguration {

    private static final Logger log = Logger.getLogger(FederatedShardQueryConfiguration.class);
    private List<ShardQueryConfiguration> configs = new ArrayList<>();

    public FederatedShardQueryConfiguration() {}

    /**
     * Copy constructor
     *
     * @param other
     *            the {@link FederatedShardQueryConfiguration} to copy
     */
    public FederatedShardQueryConfiguration(FederatedShardQueryConfiguration other) {
        if (other.getConfigs() != null) {
            List<ShardQueryConfiguration> configs = new ArrayList<>();
            for (ShardQueryConfiguration config : other.configs) {
                configs.add(new ShardQueryConfiguration(config));
            }
            this.configs = configs;
        }

    }

    public void addConfig(ShardQueryConfiguration config) {
        log.debug("Adding config " + config.getBeginDate() + "-" + config.getEndDate());
        this.configs.add(config);
    }

    public List<ShardQueryConfiguration> getConfigs() {
        return configs;
    }

}
