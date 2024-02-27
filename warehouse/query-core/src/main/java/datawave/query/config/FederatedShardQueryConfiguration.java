package datawave.query.config;

import java.util.ArrayList;
import java.util.List;

public class FederatedShardQueryConfiguration {

    private List<ShardQueryConfiguration> configs = new ArrayList<>();

    public void addConfig(ShardQueryConfiguration config) {
        this.configs.add(config);
    }

    public List<ShardQueryConfiguration> getConfigs() {
        return configs;
    }
}
