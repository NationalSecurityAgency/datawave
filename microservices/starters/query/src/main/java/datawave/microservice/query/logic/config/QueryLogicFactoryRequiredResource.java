package datawave.microservice.query.logic.config;

import org.springframework.boot.autoconfigure.condition.AllNestedConditions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;

/*
 * If datawave.query.logic.factory.enabled is true or missing and datawave.query.logic.factory.import-resources
 * is false, then we still need to import either the file specified by datawave.query.logic.factory.xmlBeansPath or
 * QueryLogicFactory.xml because they contain beans that are required by QueryLogicFactoryConfiguration.
 */
@Configuration
@Conditional(QueryLogicFactoryRequiredResource.ImportResourcesDisabledCondition.class)
@ImportResource(locations = {"${datawave.query.logic.factory.xmlBeansPath:classpath:QueryLogicFactory.xml}"})
public class QueryLogicFactoryRequiredResource {

    static class ImportResourcesDisabledCondition extends AllNestedConditions {
        ImportResourcesDisabledCondition() {
            super(ConfigurationPhase.PARSE_CONFIGURATION);
        }

        @ConditionalOnProperty(name = "datawave.query.logic.factory.enabled", havingValue = "true", matchIfMissing = true)
        static class FactoryEnabled {}

        @ConditionalOnProperty(name = "datawave.query.logic.factory.import-resources", havingValue = "false")
        static class ImportResourcesDisabled {}
    }
}
