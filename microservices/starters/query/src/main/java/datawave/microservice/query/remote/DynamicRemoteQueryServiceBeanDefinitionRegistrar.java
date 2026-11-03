package datawave.microservice.query.remote;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

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
import datawave.security.cert.SSLStores;
import datawave.webservice.common.json.ObjectMapperDecorator;
import datawave.webservice.common.remote.RemoteHttpServiceProperties;
import datawave.webservice.config.RemoteServicesConfiguration;
import datawave.webservice.query.remote.WSRemoteQueryService;
import datawave.webservice.query.result.event.ResponseObjectFactory;

/**
 * This class is used to dynamically create and register MicroserviceRemoteQueryService or RemoteQueryServiceImpl beans via properties.
 */
public class DynamicRemoteQueryServiceBeanDefinitionRegistrar implements BeanDefinitionRegistryPostProcessor {

    public static final String REMOTE_QUERY_SERVICE_CLASS = "datawave.query.remote.serviceClass";
    public static final String REMOTE_QUERY_SERVICE_PREFIX = "datawave.query.remote.services";
    private final Map<String,RemoteQueryServiceProperties> remoteQueryServicePropertiesMap;
    private final Map<String,RemoteHttpServiceProperties> remoteHttpServicePropertiesMap;
    private final Environment environment;
    private final String serviceClassName;

    public DynamicRemoteQueryServiceBeanDefinitionRegistrar(Environment environment) {
        // @formatter:off
        this.environment = environment;
        this.serviceClassName = environment.getProperty(REMOTE_QUERY_SERVICE_CLASS, String.class);
        if (StringUtils.isBlank(this.serviceClassName)) {
            throw new IllegalArgumentException("Service class must be specified for remote query services");
        }

        if (!this.serviceClassName.equals(MSRemoteQueryService.class.getSimpleName()) && !this.serviceClassName.equals(WSRemoteQueryService.class.getSimpleName())) {
            throw new IllegalArgumentException("Invalid service class: " + this.serviceClassName);
        }

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
        try {
            ObjectMapperDecorator objectMapperDecorator = RemoteServicesConfiguration.createObjectMapperDecorator();
            SSLStores sslStores = RemoteServicesConfiguration.createSSLStores(environment);
            ExecutorService executorService = RemoteServicesConfiguration.createExecutorService(environment);
            ResponseObjectFactory responseObjectFactory = RemoteServicesConfiguration.createResponseObjectFactory();

            remoteQueryServicePropertiesMap.forEach((name, remoteQueryServiceProperties) -> {
                GenericBeanDefinition beanDefinition = new GenericBeanDefinition();

                if (serviceClassName.equals(MSRemoteQueryService.class.getSimpleName())) {
                    beanDefinition.setBeanClass(MSRemoteQueryService.class);
                    ConstructorArgumentValues constructorArgValues = new ConstructorArgumentValues();
                    constructorArgValues.addGenericArgumentValue(remoteQueryServiceProperties);
                    beanDefinition.setConstructorArgumentValues(constructorArgValues);
                } else if (serviceClassName.equals(WSRemoteQueryService.class.getSimpleName())) {
                    beanDefinition.setBeanClass(WSRemoteQueryService.class);
                    beanDefinition.getPropertyValues().add("remoteQueryServiceProperties", remoteQueryServiceProperties);
                    beanDefinition.getPropertyValues().add("objectMapperDecorator", objectMapperDecorator);
                    beanDefinition.getPropertyValues().add("sslStores", sslStores);
                    beanDefinition.getPropertyValues().add("executorService", executorService);
                    beanDefinition.getPropertyValues().add("responseObjectFactory", responseObjectFactory);

                    RemoteHttpServiceProperties remoteHttpServiceProperties = remoteHttpServicePropertiesMap.get(name);
                    if (remoteHttpServiceProperties != null) {
                        beanDefinition.getPropertyValues().add("config", remoteHttpServiceProperties);
                    }
                }
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
