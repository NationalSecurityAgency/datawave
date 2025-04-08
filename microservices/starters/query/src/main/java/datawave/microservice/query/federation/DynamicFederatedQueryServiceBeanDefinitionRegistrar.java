package datawave.microservice.query.federation;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;

import datawave.microservice.query.federation.config.FederatedQueryProperties;

/**
 * This class is used to dynamically create and register FederatedQueryService beans via properties.
 */
public class DynamicFederatedQueryServiceBeanDefinitionRegistrar implements BeanDefinitionRegistryPostProcessor {
    
    public static final String FEDERATED_QUERY_SERVICE_PREFIX = "datawave.query.federation.services";
    private final Map<String,FederatedQueryProperties> federatedQueryProperties;
    
    public DynamicFederatedQueryServiceBeanDefinitionRegistrar(Environment environment) {
        // @formatter:off
        federatedQueryProperties = Binder.get(environment)
                .bind(FEDERATED_QUERY_SERVICE_PREFIX, Bindable.mapOf(String.class, FederatedQueryProperties.class))
                .orElse(new HashMap<>());
        // @formatter:off
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry beanDefinitionRegistry) throws BeansException {
        federatedQueryProperties.forEach((name, props) -> {
            GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
            beanDefinition.setBeanClass(FederatedQueryService.class);

            ConstructorArgumentValues constructorArgValues = new ConstructorArgumentValues();
            constructorArgValues.addGenericArgumentValue(props);
            beanDefinition.setConstructorArgumentValues(constructorArgValues);

            beanDefinition.setScope(SCOPE_PROTOTYPE);
            beanDefinitionRegistry.registerBeanDefinition(name, beanDefinition);
        });
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory configurableListableBeanFactory) throws BeansException {
        // intentionally blank
    }
}
