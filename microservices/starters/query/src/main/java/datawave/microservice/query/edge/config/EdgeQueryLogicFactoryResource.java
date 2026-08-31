package datawave.microservice.query.edge.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

@Configuration
@ConditionalOnProperty(name = "datawave.query.logic.factory.importResources", matchIfMissing = true)
@ImportResource(locations = {"classpath:EdgeQueryLogicFactory.xml"})
public class EdgeQueryLogicFactoryResource {
    public EdgeQueryLogicFactoryResource() {
        System.out.println("EdgeQueryLogicFactoryResource created");
    }
}
