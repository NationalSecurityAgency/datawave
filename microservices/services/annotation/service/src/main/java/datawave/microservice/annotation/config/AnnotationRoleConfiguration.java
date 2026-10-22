package datawave.microservice.annotation.config;

import static datawave.microservice.annotation.config.AnnotationRoleConstants.ACCUMULO_ANNOTATION_SINK;
import static datawave.microservice.annotation.config.AnnotationRoleConstants.ACCUMULO_WRITER_ENABLED;
import static datawave.microservice.annotation.config.AnnotationRoleConstants.ANNOTATION_SOURCE;
import static datawave.microservice.annotation.config.AnnotationRoleConstants.BUS_CONSUMER;
import static datawave.microservice.annotation.config.AnnotationRoleConstants.BUS_ENABLED;
import static datawave.microservice.annotation.config.AnnotationRoleConstants.BUS_PROPERTY_SOURCE;
import static datawave.microservice.annotation.config.AnnotationRoleConstants.DUMP_ANNOTATION_SINK;
import static datawave.microservice.annotation.config.AnnotationRoleConstants.DUMP_WRITER_ENABLED;
import static datawave.microservice.annotation.config.AnnotationRoleConstants.FUNCTION_DEFINITION;
import static datawave.microservice.annotation.config.AnnotationRoleConstants.LOG_ANNOTATION_SINK;
import static datawave.microservice.annotation.config.AnnotationRoleConstants.LOG_WRITER_ENABLED;
import static datawave.microservice.annotation.config.AnnotationRoleConstants.ROLE_FUNCTION_PROPERTY_SOURCE;
import static datawave.microservice.annotation.config.AnnotationRoleConstants.ROLE_OVERLAY_PREFIX;
import static datawave.microservice.annotation.config.AnnotationRoleConstants.STREAM_FUNCTION_AUTODETECT;
import static datawave.microservice.annotation.config.AnnotationRoleConstants.STREAM_FUNCTION_BINDINGS;
import static datawave.microservice.annotation.config.AnnotationRoleConstants.STREAM_FUNCTION_DEFINITION;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;

/**
 * Selects stream functions after external/bootstrap configuration is available and before any stream properties or bindings are instantiated.
 */
@Configuration(proxyBeanMethods = false)
public class AnnotationRoleConfiguration {
    /**
     * Runs role-based function selection after external configuration is available and before Spring creates stream bindings.
     *
     * @param environment
     *            the application environment, including the selected role and its Config Server overlay
     * @return the post-processor that validates the role settings and installs the function definitions
     */
    @Bean
    public static BeanFactoryPostProcessor annotationRoleFunctions(ConfigurableEnvironment environment) {
        return beanFactory -> configureFunctions(environment);
    }

    /**
     * Selects the HTTP message producer for API roles and the enabled queue consumers for writer roles; combined roles include both. When Cloud Bus is enabled,
     * its consumer is included as well. The selected functions are installed as separate functions in both Spring function-definition properties, and automatic
     * function discovery is disabled.
     * <p>
     * Startup fails for a missing or conflicting role, a missing matching overlay, user-supplied function definitions, invalid backend flags, a writer with no
     * enabled queue consumer, or aliases that rename the fixed annotation bindings. Cloud Bus's generated {@code busConsumer} definition is allowed only in its
     * own property source; the original property sources are still checked for user overrides.
     *
     * @param environment
     *            the application environment to validate and update before stream bindings are created
     * @throws IllegalStateException
     *             if the role or messaging settings violate these startup requirements
     */
    static void configureFunctions(ConfigurableEnvironment environment) {
        AnnotationRole role = AnnotationRole.from(environment);
        String overlayMarker = ROLE_OVERLAY_PREFIX + role.profile();
        if (!environment.getProperty(overlayMarker, Boolean.class, false)) {
            throw new IllegalStateException("Profile " + role.profile() + " requires its matching Config Server overlay annotation-" + role.profile()
                            + ".yml; missing " + overlayMarker);
        }
        for (String property : List.of(FUNCTION_DEFINITION, STREAM_FUNCTION_DEFINITION)) {
            for (PropertySource<?> source : environment.getPropertySources()) {
                // Boot's attached source combines the other sources. Inspect the originals so Bus cannot hide a user override.
                if (ConfigurationPropertySources.isAttachedConfigurationPropertySource(source)) {
                    continue;
                }
                // Bus adds its own consumer before startup. The service includes that consumer in the selected role's function list below.
                if (BUS_PROPERTY_SOURCE.equals(source.getName()) && FUNCTION_DEFINITION.equals(property) && BUS_CONSUMER.equals(source.getProperty(property))) {
                    continue;
                }
                if (source.containsProperty(property)) {
                    throw new IllegalStateException(property + " is managed by the annotation service; remove the override and select consumers with "
                                    + LOG_WRITER_ENABLED + ", " + ACCUMULO_WRITER_ENABLED + ", and " + DUMP_WRITER_ENABLED);
                }
            }
        }
        // Use the same flag semantics as the backend bean conditions.
        boolean logEnabled = enabled(environment, LOG_WRITER_ENABLED);
        boolean accumuloEnabled = enabled(environment, ACCUMULO_WRITER_ENABLED);
        boolean dumpEnabled = enabled(environment, DUMP_WRITER_ENABLED);
        Map<String,Boolean> annotationFunctions = new LinkedHashMap<>();
        annotationFunctions.put(ANNOTATION_SOURCE, role.includesApi());
        annotationFunctions.put(LOG_ANNOTATION_SINK, role.includesWriter() && logEnabled);
        annotationFunctions.put(ACCUMULO_ANNOTATION_SINK, role.includesWriter() && accumuloEnabled);
        annotationFunctions.put(DUMP_ANNOTATION_SINK, role.includesWriter() && dumpEnabled);

        List<String> functions = annotationFunctions.entrySet().stream().filter(Map.Entry::getValue).map(Map.Entry::getKey)
                        .collect(Collectors.toCollection(ArrayList::new));
        if (role == AnnotationRole.WRITER && functions.isEmpty()) {
            throw new IllegalStateException("Profile writer requires an enabled log, accumulo, or dump queue consumer");
        }
        if (environment.getProperty(BUS_ENABLED, Boolean.class, true)) {
            functions.add(BUS_CONSUMER);
        }

        // Bind aliases exactly as Stream does, including bracket notation in property sources.
        Map<String,String> aliases = Binder.get(environment).bind(STREAM_FUNCTION_BINDINGS, Bindable.mapOf(String.class, String.class)).orElse(Map.of());
        for (String name : annotationFunctions.keySet()) {
            String binding = name + (ANNOTATION_SOURCE.equals(name) ? "-out-0" : "-in-0");
            String alias = aliases.get(binding);
            if (alias != null && !binding.equals(alias)) {
                throw new IllegalStateException(
                                STREAM_FUNCTION_BINDINGS + "." + binding + " must not rename the fixed annotation binding; received '" + alias + "'");
            }
        }

        Map<String,Object> properties = Map.of(FUNCTION_DEFINITION, String.join(";", functions), STREAM_FUNCTION_DEFINITION, String.join(";", functions),
                        STREAM_FUNCTION_AUTODETECT, false);
        environment.getPropertySources().addFirst(new MapPropertySource(ROLE_FUNCTION_PROPERTY_SOURCE, properties));
    }

    private static boolean enabled(ConfigurableEnvironment environment, String property) {
        // Match @ConditionalOnProperty(havingValue = "true"), without Boolean aliases or whitespace trimming.
        String value = environment.getProperty(property);
        if (value == null || "false".equalsIgnoreCase(value)) {
            return false;
        }
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        throw new IllegalStateException(property + " must be true or false; received '" + value + "'");
    }
}
