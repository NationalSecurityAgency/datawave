package datawave.microservice.authorization.remote;

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

import datawave.security.authorization.RemoteUserOperationsProperties;

/**
 * This class is used to dynamically create and register RemoteUserOperations beans via properties.
 */
public class DynamicMSRemoteUserOperationsBeanDefinitionRegistrar implements BeanDefinitionRegistryPostProcessor, Ordered {

    public static final String REMOTE_AUTHORIZATION_SERVICE_PREFIX = "datawave.authorization.remote.services";
    private final Map<String,RemoteUserOperationsProperties> remoteUserOperationsPropertiesMap;

    public DynamicMSRemoteUserOperationsBeanDefinitionRegistrar(Environment environment) {
        // @formatter:off
        remoteUserOperationsPropertiesMap = Binder.get(environment)
                .bind(REMOTE_AUTHORIZATION_SERVICE_PREFIX, Bindable.mapOf(String.class, RemoteUserOperationsProperties.class))
                .orElse(new HashMap<>());
        // @formatter:off
    }

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry beanDefinitionRegistry) throws BeansException {
        remoteUserOperationsPropertiesMap.forEach((name, props) -> {
            GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
            beanDefinition.setBeanClass(MSRemoteUserOperations.class);

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
