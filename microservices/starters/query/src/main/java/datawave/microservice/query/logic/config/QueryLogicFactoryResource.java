package datawave.microservice.query.logic.config;

import org.springframework.boot.autoconfigure.condition.AllNestedConditions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

/*
 * If datawave.query.logic.factory.enabled is true or missing and datawave.query.logic.factory.import-resources if not false,
 * then import all listed QueryLogicFactory XML files.  This is the default behavior.
 */
@Configuration
@Conditional(QueryLogicFactoryResource.ImportAllResourcesCondition.class)
@ImportResource(locations = {"classpath:QueryLogicFactory.xml", "classpath:SSDeepQueryLogicFactory.xml", "classpath:KeywordExtractionQueryLogicFactory.xml"})
public class QueryLogicFactoryResource {

    static class ImportAllResourcesCondition extends AllNestedConditions {
        ImportAllResourcesCondition() {
            super(ConfigurationPhase.PARSE_CONFIGURATION);
        }

        @ConditionalOnProperty(name = "datawave.query.logic.factory.enabled", havingValue = "true", matchIfMissing = true)
        static class FactoryEnabled {}

        @ConditionalOnProperty(name = "datawave.query.logic.factory.import-resources", havingValue = "true", matchIfMissing = true)
        static class ImportResourcesEnabled {}
    }
}
