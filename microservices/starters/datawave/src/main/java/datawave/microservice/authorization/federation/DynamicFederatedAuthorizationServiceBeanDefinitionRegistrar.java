package datawave.microservice.authorization.federation;

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
import org.springframework.core.Ordered;
import org.springframework.core.env.Environment;

import datawave.microservice.authorization.federation.config.FederatedAuthorizationServiceProperties;

/**
 * This class is used to dynamically create and register FederatedAuthorizationService beans via properties.
 */
public class DynamicFederatedAuthorizationServiceBeanDefinitionRegistrar implements BeanDefinitionRegistryPostProcessor, Ordered {
    
    public static final String FEDERATED_AUTHORIZATION_SERVICE_PREFIX = "datawave.authorization.federation.services";
    private final Map<String,FederatedAuthorizationServiceProperties> federatedAuthorizationProperties;
    
    public DynamicFederatedAuthorizationServiceBeanDefinitionRegistrar(Environment environment) {
        // @formatter:off
        federatedAuthorizationProperties = Binder.get(environment)
                .bind(FEDERATED_AUTHORIZATION_SERVICE_PREFIX, Bindable.mapOf(String.class, FederatedAuthorizationServiceProperties.class))
                .orElse(new HashMap<>());
        // @formatter:off
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry beanDefinitionRegistry) throws BeansException {
        federatedAuthorizationProperties.forEach((name, props) -> {
            GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
            beanDefinition.setBeanClass(FederatedAuthorizationService.class);

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
    @Override
    public int getOrder() {
        return getPrecedence();
    }

    public static int getPrecedence() {
        return HIGHEST_PRECEDENCE;
    }
}
