package datawave.webservice.query.limit;

import datawave.zookeeper.ZkClientBuilder;
import datawave.zookeeper.ZkPojoPublisherImpl;

import javax.ejb.Singleton;
import javax.ejb.Startup;

/**
 * Extension of {@link ZkPojoPublisherImpl} that will provide updates of {@link QueryLimitConfiguration} instances.
 */
@Singleton
@Startup
public class QueryLimitConfigPublisher extends ZkPojoPublisherImpl<QueryLimitConfiguration> {
    
    public QueryLimitConfigPublisher(ZkClientBuilder zkClientBuilder, String hdfsConfigUrls) {
        super(zkClientBuilder, hdfsConfigUrls, QueryLimitConfiguration.class);
    }
}
