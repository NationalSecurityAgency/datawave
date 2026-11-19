package datawave.visibility;

import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Suppliers;

final class VisibilityProviderLoader {
    private static final Logger log = LoggerFactory.getLogger(VisibilityProviderLoader.class);
    private static final Supplier<VisibilityFunctions> INSTANCE = Suppliers.memoize(() -> load(VisibilityFunctions.class.getClassLoader()));

    private VisibilityProviderLoader() {}

    static VisibilityFunctions getInstance() {
        return INSTANCE.get();
    }

    static VisibilityFunctions load(ClassLoader loader) {
        try {
            List<ServiceLoader.Provider<VisibilityFunctions>> providers = ServiceLoader.load(VisibilityFunctions.class, loader).stream()
                            .collect(Collectors.toList());
            if (providers.isEmpty()) {
                throw new IllegalStateException("No VisibilityFunctions provider registered. Deploy visibility-provider-standard or a custom provider JAR "
                                + "with META-INF/services/datawave.visibility.VisibilityFunctions.");
            }
            if (providers.size() != 1) {
                throw new IllegalStateException("Expected one VisibilityFunctions provider, found: "
                                + providers.stream().map(p -> p.type().getName()).collect(Collectors.joining(", ")));
            }
            VisibilityFunctions provider = providers.get(0).get();
            log.info("Using VisibilityFunctions provider {}", provider.getClass().getName());
            return provider;
        } catch (ServiceConfigurationError e) {
            throw new IllegalStateException("Failed to load a registered VisibilityFunctions provider", e);
        }
    }
}
