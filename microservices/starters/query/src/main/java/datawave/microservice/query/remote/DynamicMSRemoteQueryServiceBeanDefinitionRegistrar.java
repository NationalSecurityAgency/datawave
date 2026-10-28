package datawave.microservice.query.remote;

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

import datawave.core.query.remote.RemoteQueryServiceProperties;

/**
 * This class is used to dynamically create and register MSRemoteQueryService beans via properties.
 */
public class DynamicMSRemoteQueryServiceBeanDefinitionRegistrar implements BeanDefinitionRegistryPostProcessor {

    public static final String REMOTE_QUERY_SERVICE_PREFIX = "datawave.remote.query.services";
    private final Map<String,RemoteQueryServiceProperties> remoteQueryServicePropertiesMap;

    public DynamicMSRemoteQueryServiceBeanDefinitionRegistrar(Environment environment) {
        // @formatter:off

        remoteQueryServicePropertiesMap = Binder.get(environment)
                .bind(REMOTE_QUERY_SERVICE_PREFIX, Bindable.mapOf(String.class, RemoteQueryServiceProperties.class))
                .orElse(new HashMap<>());
        // @formatter:on
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry beanDefinitionRegistry) throws BeansException {
        try {
            remoteQueryServicePropertiesMap.forEach((name, remoteQueryServiceProperties) -> {
                GenericBeanDefinition beanDefinition = new GenericBeanDefinition();

                beanDefinition.setBeanClass(MSRemoteQueryService.class);
                ConstructorArgumentValues constructorArgValues = new ConstructorArgumentValues();
                constructorArgValues.addGenericArgumentValue(remoteQueryServiceProperties);
                beanDefinition.setConstructorArgumentValues(constructorArgValues);
                beanDefinition.setScope(SCOPE_PROTOTYPE);
                beanDefinitionRegistry.registerBeanDefinition(name, beanDefinition);
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to register remote query service beans", e);
        }
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory configurableListableBeanFactory) throws BeansException {
        // intentionally blank
    }
}
