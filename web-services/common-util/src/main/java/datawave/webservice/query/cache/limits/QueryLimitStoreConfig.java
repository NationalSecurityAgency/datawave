package datawave.webservice.query.cache.limits;

import org.apache.curator.framework.CuratorFramework;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import datawave.webservice.query.cache.limits.conditions.ZooKeeperCondition;

@Configuration
public class QueryLimitStoreConfig {

    @Bean
    @Conditional(ZooKeeperCondition.class)
    public QueryLimitStore zookeeperStore(CuratorFramework curator) {
        return new ZooKeekerQueryLimitStore(curator);
    }
}
