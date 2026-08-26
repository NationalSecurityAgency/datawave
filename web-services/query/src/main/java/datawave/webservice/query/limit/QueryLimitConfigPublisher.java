package datawave.webservice.query.limit;

import datawave.zookeeper.ZkPojoPublisherImpl;

/**
 * Extension of {@link ZkPojoPublisherImpl} that will provide updates of {@link QueryLimitConfiguration} instances.
 */
public class QueryLimitConfigPublisher extends ZkPojoPublisherImpl<QueryLimitConfiguration> {

    public QueryLimitConfigPublisher() {
        super(QueryLimitConfiguration.class);
    }
}
