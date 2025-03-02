package datawave.webservice.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(ZookeeperPropertyCondition.class)
public class CuratorConfig {

    @Bean
    public CuratorFramework curatorFramework(@Value("${dw.warehouse.zookeepers}") String zookeepers) {
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(zookeepers).retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
        client.start();
        return client;
    }
}
