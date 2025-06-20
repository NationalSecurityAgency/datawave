package datawave.microservice.query.logic.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

@Configuration
@ConditionalOnProperty(name = "datawave.query.logic.factory.importResources", matchIfMissing = true)
@ImportResource(locations = {"classpath:SSDeepQueryLogicFactory.xml", "classpath:KeywordExtractionQueryLogicFactory.xml"})
public class QueryLogicFactoryResource {

}
