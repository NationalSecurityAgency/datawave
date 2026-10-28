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
import org.springframework.core.Ordered;
import org.springframework.core.env.Environment;

import datawave.security.authorization.RemoteUserOperationsProperties;
import datawave.security.authorization.remote.WSRemoteUserOperations;
import datawave.security.cert.SSLStores;
import datawave.webservice.common.json.ObjectMapperDecorator;
import datawave.webservice.common.remote.RemoteHttpServiceProperties;
import datawave.webservice.config.RemoteServicesConfiguration;
import datawave.webservice.query.result.event.ResponseObjectFactory;

/**
 * This class is used to dynamically create and register RemoteUserOperations beans via properties.
 */
public class DynamicWSRemoteUserOperationsBeanDefinitionRegistrar implements BeanDefinitionRegistryPostProcessor, Ordered {

    public static final String REMOTE_AUTHORIZATION_SERVICE_PREFIX = "datawave.remote.user-operations.services";
    private final Map<String,RemoteUserOperationsProperties> remoteUserOperationsPropertiesMap;
    private final Map<String,RemoteHttpServiceProperties> remoteHttpServicePropertiesMap;
    private final Environment environment;
    private final ResponseObjectFactory responseObjectFactory;

    public DynamicWSRemoteUserOperationsBeanDefinitionRegistrar(Environment environment, ResponseObjectFactory responseObjectFactory) {
        this.environment = environment;
        this.responseObjectFactory = responseObjectFactory;
        // @formatter:off
        remoteUserOperationsPropertiesMap = Binder.get(environment)
                .bind(REMOTE_AUTHORIZATION_SERVICE_PREFIX, Bindable.mapOf(String.class, RemoteUserOperationsProperties.class))
                .orElse(new HashMap<>());
        remoteHttpServicePropertiesMap = Binder.get(environment)
                .bind(REMOTE_AUTHORIZATION_SERVICE_PREFIX, Bindable.mapOf(String.class, RemoteHttpServiceProperties.class))
                .orElse(new HashMap<>());
        // @formatter:on
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry beanDefinitionRegistry) throws BeansException {
        try {
            ObjectMapperDecorator objectMapperDecorator = RemoteServicesConfiguration.createObjectMapperDecorator();
            SSLStores sslStores = RemoteServicesConfiguration.createSSLStores(environment);
            ExecutorService executorService = RemoteServicesConfiguration.createExecutorService(environment, WSRemoteUserOperations.PROP_PREFIX + ".executor");

            remoteUserOperationsPropertiesMap.forEach((name, remoteUserOperationsProperties) -> {
                GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
                beanDefinition.setBeanClass(WSRemoteUserOperations.class);

                beanDefinition.getPropertyValues().add("remoteUserOperationsProperties", remoteUserOperationsProperties);
                RemoteHttpServiceProperties remoteHttpServiceProperties = remoteHttpServicePropertiesMap.get(name);
                if (remoteHttpServiceProperties != null) {
                    beanDefinition.getPropertyValues().add("config", remoteHttpServiceProperties);
                }
                beanDefinition.getPropertyValues().add("objectMapperDecorator", objectMapperDecorator);
                beanDefinition.getPropertyValues().add("sslStores", sslStores);
                beanDefinition.getPropertyValues().add("executorService", executorService);
                beanDefinition.getPropertyValues().add("responseObjectFactory", responseObjectFactory);
                beanDefinition.setScope(SCOPE_PROTOTYPE);
                beanDefinitionRegistry.registerBeanDefinition(name, beanDefinition);
            });
        } catch (Exception e) {
            throw new RuntimeException("Error creating RemoteUserOperations beans", e);
        }
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
