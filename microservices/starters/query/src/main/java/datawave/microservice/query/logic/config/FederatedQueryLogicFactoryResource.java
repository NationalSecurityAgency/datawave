package datawave.microservice.query.logic.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Profile;

@Configuration
@ConditionalOnProperty(name = "datawave.query.logic.factory.importResources", matchIfMissing = true)
@Profile(value = "federation")
@ImportResource(locations = {"classpath:FederatedQueryLogicFactory.xml"})
public class FederatedQueryLogicFactoryResource {

}
