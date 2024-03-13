package datawave.query.config;

import java.util.ArrayList;
import java.util.List;

import datawave.query.CloseableIterable;
import datawave.webservice.query.configuration.QueryData;

public class FederatedQueryConfiguration {

    private List<ShardQueryConfiguration> configs = new ArrayList<>();

    private List<CloseableIterable<QueryData>> queryDatas = new ArrayList<>();

    public List<ShardQueryConfiguration> getConfigs() {
        return configs;
    }

    public void addConfig(ShardQueryConfiguration configuration) {
        this.configs.add(configuration);
    }

    public List<CloseableIterable<QueryData>> getQueryDatas() {
        return queryDatas;
    }

    public void addQueryData(CloseableIterable<QueryData> iterator) {
        this.queryDatas.add(iterator);
    }
}
