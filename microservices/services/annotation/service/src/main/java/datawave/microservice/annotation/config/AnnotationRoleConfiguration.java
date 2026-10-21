package datawave.microservice.annotation.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * Selects stream functions after external/bootstrap configuration is available and before any stream properties or bindings are instantiated.
 */
@Configuration(proxyBeanMethods = false)
public class AnnotationRoleConfiguration {
    @Bean
    public static BeanFactoryPostProcessor annotationRoleFunctions(ConfigurableEnvironment environment) {
        return beanFactory -> configureFunctions(environment);
    }

    static void configureFunctions(ConfigurableEnvironment environment) {
        AnnotationRole role = AnnotationRole.from(environment);
        String overlayMarker = "annotation.deployment.overlays." + role.profile();
        if (!environment.getProperty(overlayMarker, Boolean.class, false)) {
            throw new IllegalStateException("Profile " + role.profile() + " requires its matching Config Server overlay annotation-" + role.profile()
                            + ".yml; missing " + overlayMarker);
        }
        for (String property : List.of("spring.cloud.function.definition", "spring.cloud.stream.function.definition")) {
            if (environment.containsProperty(property)) {
                throw new IllegalStateException(property + " is managed by the annotation service; remove the override and select consumers with "
                                + "annotation.writers.log.enabled, annotation.writers.accumulo.enabled, and annotation.writers.dump.enabled");
            }
        }
        // Use the same flag semantics as the backend bean conditions.
        boolean logEnabled = enabled(environment, "annotation.writers.log.enabled");
        boolean accumuloEnabled = enabled(environment, "annotation.writers.accumulo.enabled");
        boolean dumpEnabled = enabled(environment, "annotation.writers.dump.enabled");
        Map<String,Boolean> annotationFunctions = new LinkedHashMap<>();
        annotationFunctions.put("annotationSource", role.includesApi());
        annotationFunctions.put("logAnnotationSink", role.includesWriter() && logEnabled);
        annotationFunctions.put("accumuloAnnotationSink", role.includesWriter() && accumuloEnabled);
        annotationFunctions.put("dumpAnnotationSink", role.includesWriter() && dumpEnabled);

        List<String> functions = annotationFunctions.entrySet().stream().filter(Map.Entry::getValue).map(Map.Entry::getKey)
                        .collect(Collectors.toCollection(ArrayList::new));
        if (role == AnnotationRole.WRITER && functions.isEmpty()) {
            throw new IllegalStateException("Profile writer requires an enabled log, accumulo, or dump queue consumer");
        }
        if (environment.getProperty("spring.cloud.bus.enabled", Boolean.class, true)) {
            functions.add("busConsumer");
        }

        // Bind aliases exactly as Stream does, including bracket notation in property sources.
        Map<String,String> aliases = Binder.get(environment).bind("spring.cloud.stream.function.bindings", Bindable.mapOf(String.class, String.class))
                        .orElse(Map.of());
        for (String name : annotationFunctions.keySet()) {
            String binding = name + ("annotationSource".equals(name) ? "-out-0" : "-in-0");
            String alias = aliases.get(binding);
            if (alias != null && !binding.equals(alias)) {
                throw new IllegalStateException(
                                "spring.cloud.stream.function.bindings." + binding + " must not rename the fixed annotation binding; received '" + alias + "'");
            }
        }

        Map<String,Object> properties = Map.of("spring.cloud.function.definition", String.join(";", functions), "spring.cloud.stream.function.definition",
                        String.join(";", functions), "spring.cloud.stream.function.autodetect", false);
        environment.getPropertySources().addFirst(new MapPropertySource("annotationRoleFunctions", properties));
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
