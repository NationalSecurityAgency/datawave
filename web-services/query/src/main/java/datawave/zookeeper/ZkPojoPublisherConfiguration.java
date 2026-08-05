package datawave.zookeeper;

import datawave.webservice.query.limit.QueryLimitConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ZkPojoPublisherConfiguration {
    
    private static final Logger log = LoggerFactory.getLogger(ZkPojoPublisherConfiguration.class);
    
    private String rootNamespace;
    
    private String hdfsConfig;
    
    private ZkClientBuilder zkClientBuilder;
    
    public void setRootNamespace(String rootNamespace) {
        this.rootNamespace = rootNamespace;
    }
    
    public void setHdfsConfig(String hdfsConfig) {
        this.hdfsConfig = hdfsConfig;
    }
    
    public void setZkClientBuilder(ZkClientBuilder zkClientBuilder) {
        this.zkClientBuilder = zkClientBuilder;
    }
    
    @Bean
    @Qualifier("queryLimitConfigPublisher")
    public ZkPojoPublisher<QueryLimitConfiguration> queryLimitConfigPublisher() {
        ZkClientBuilder clientBuilder = zkClientBuilder.duplicate().withNamespace(this.rootNamespace + "/queryLimitConfig");
        try {
            return new ZkPojoPublisherImpl<>(clientBuilder, hdfsConfig, QueryLimitConfiguration.class);
        } catch (Exception e) {
            log.error("Error creating {} instance of {}", "queryLimitConfigPublisher", ZkPojoPublisher.class, e);
            throw new RuntimeException(e);
        }
    }
}
