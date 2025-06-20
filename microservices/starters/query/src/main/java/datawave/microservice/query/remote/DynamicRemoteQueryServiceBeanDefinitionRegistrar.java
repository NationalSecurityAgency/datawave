package datawave.microservice.query.remote;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
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
import datawave.webservice.common.remote.RemoteHttpServiceProperties;
import datawave.webservice.query.remote.WSRemoteQueryService;

/**
 * This class is used to dynamically create and register MicroserviceRemoteQueryService or RemoteQueryServiceImpl beans via properties.
 */
public class DynamicRemoteQueryServiceBeanDefinitionRegistrar implements BeanDefinitionRegistryPostProcessor {

    public static final String REMOTE_QUERY_SERVICE_PREFIX = "datawave.query.remote.services";
    private final Map<String,RemoteQueryServiceProperties> remoteQueryServicePropertiesMap;
    private final Map<String,RemoteHttpServiceProperties> remoteHttpServicePropertiesMap;

    public DynamicRemoteQueryServiceBeanDefinitionRegistrar(Environment environment) {
        // @formatter:off
        remoteQueryServicePropertiesMap = Binder.get(environment)
                .bind(REMOTE_QUERY_SERVICE_PREFIX, Bindable.mapOf(String.class, RemoteQueryServiceProperties.class))
                .orElse(new HashMap<>());
        remoteHttpServicePropertiesMap = Binder.get(environment)
                .bind(REMOTE_QUERY_SERVICE_PREFIX, Bindable.mapOf(String.class, RemoteHttpServiceProperties.class))
                .orElse(new HashMap<>());
        // @formatter:on
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry beanDefinitionRegistry) throws BeansException {
        remoteQueryServicePropertiesMap.forEach((name, remoteQueryServiceProperties) -> {
            GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
            String serviceClass = remoteQueryServiceProperties.getServiceClass();
            if (StringUtils.isBlank(serviceClass)) {
                throw new IllegalArgumentException("Service class must be specified for remote query service: " + name);
            }

            if (serviceClass.equals(MSRemoteQueryService.class.getCanonicalName())) {
                beanDefinition.setBeanClass(MSRemoteQueryService.class);
                ConstructorArgumentValues constructorArgValues = new ConstructorArgumentValues();
                constructorArgValues.addGenericArgumentValue(remoteQueryServiceProperties);
                beanDefinition.setConstructorArgumentValues(constructorArgValues);
            } else if (serviceClass.equals(WSRemoteQueryService.class.getName())) {
                beanDefinition.setBeanClass(WSRemoteQueryService.class);
                beanDefinition.getPropertyValues().add("remoteQueryServiceProperties", remoteQueryServiceProperties);
                RemoteHttpServiceProperties remoteHttpServiceProperties = remoteHttpServicePropertiesMap.get(name);
                if (remoteHttpServiceProperties != null) {
                    beanDefinition.getPropertyValues().add("config", remoteHttpServiceProperties);
                }
            } else {
                throw new IllegalArgumentException("Invalid service class: " + serviceClass);
            }
            beanDefinition.setScope(SCOPE_PROTOTYPE);
            beanDefinitionRegistry.registerBeanDefinition(name, beanDefinition);
        });
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory configurableListableBeanFactory) throws BeansException {
        // intentionally blank
    }
}
