package datawave.webservice;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
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
 * This class is used to dynamically create and register WSRemoteQueryService beans via properties.
 */
public class DynamicWSRemoteQueryServiceBeanDefinitionRegistrar implements BeanDefinitionRegistryPostProcessor {

    public static final String REMOTE_QUERY_SERVICE_PREFIX = "datawave.remote.query.services";
    private final Map<String,RemoteQueryServiceProperties> remoteQueryServicePropertiesMap;
    private final Map<String,RemoteHttpServiceProperties> remoteHttpServicePropertiesMap;
    private final Environment environment;
    private final ResponseObjectFactory responseObjectFactory;

    public DynamicWSRemoteQueryServiceBeanDefinitionRegistrar(Environment environment, ResponseObjectFactory responseObjectFactory) {
        // @formatter:off
        this.environment = environment;
        this.responseObjectFactory = responseObjectFactory;

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
            ExecutorService executorService = RemoteServicesConfiguration.createExecutorService(environment, WSRemoteQueryService.PROP_PREFIX + ".executor");

            remoteQueryServicePropertiesMap.forEach((name, remoteQueryServiceProperties) -> {
                GenericBeanDefinition beanDefinition = new GenericBeanDefinition();

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
