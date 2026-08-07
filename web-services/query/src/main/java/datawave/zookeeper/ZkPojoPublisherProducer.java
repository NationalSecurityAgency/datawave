package datawave.zookeeper;

import datawave.configuration.spring.SpringBean;
import datawave.webservice.query.limit.QueryLimitConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ejb.Startup;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Named;

/**
 * A producer that provides instances of {@link ZkPojoPublisher}.
 */
@Startup
@ApplicationScoped
public class ZkPojoPublisherProducer {
    
    private static final Logger log = LoggerFactory.getLogger(ZkPojoPublisherProducer.class);
    
    @Inject
    @SpringBean
    @SuppressWarnings("CdiInjectionPointsInspection")
    private ZkPojoPublisherProducerConfiguration config;
    
 
    @Inject
    @SpringBean(name = "defaultZkClientBuilder")
    @SuppressWarnings("CdiInjectionPointsInspection")
    private ZkClientBuilder zkClientBuilder;
    
    /**
     * Produces an instance of {@link ZkPojoPublisher} that will publish instances of {@link QueryLimitConfiguration} under the namespace
     * @return the {@link ZkPojoPublisher}
     */
    @Produces
    @Named("queryLimitConfigPublisher")
    public ZkPojoPublisher<QueryLimitConfiguration> queryLimitConfigPublisher() {
        ZkClientBuilder clientBuilder = createZkClientBuilder("queryLimitConfig");
        if(log.isDebugEnabled()) {
            log.debug("Creating {} instance for updates of {} with zkClientBuilder={}, hdfsConfigUrls={}", ZkPojoPublisher.class.getName(),
                            QueryLimitConfiguration.class.getName(), clientBuilder, config.getHdfsConfigUrls());
        }
        try {
            return new ZkPojoPublisherImpl<>(clientBuilder, config.getHdfsConfigUrls(), QueryLimitConfiguration.class);
        } catch (Exception e) {
            log.error("Error creating {} instance of {}", "queryLimitConfigPublisher", ZkPojoPublisher.class, e);
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Return a new {@link ZkClientBuilder} that is a duplicate of {@link #zkClientBuilder} with a namespace of {@code <rootNamespace>/<publisherNamespace>}.
     * @param publisherNamespace the publisher namespace
     * @return the new client builder
     */
    private ZkClientBuilder createZkClientBuilder(String publisherNamespace) {
        String root = config.getRootNamespace();
        String namespace = root != null ? root + "/" + publisherNamespace : publisherNamespace;
        return zkClientBuilder.duplicate().withNamespace(namespace);
    }
}
