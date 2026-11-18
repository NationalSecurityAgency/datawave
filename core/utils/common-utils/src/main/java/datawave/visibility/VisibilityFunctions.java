package datawave.visibility;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.access.ParsedAccessExpression;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functions for combining and flattening Accumulo column visibilities.
 * <p>
 * This is a service provider interface. Deployments that need different combination rules (for example, rules that simplify classification-style expressions)
 * can supply their own implementation, which is discovered at runtime by {@link #getInstance()}:
 * <ol>
 * <li>If the system property {@value #CLASS_OVERRIDE_PROPERTY} is set, the named class is instantiated.</li>
 * <li>Otherwise a single provider registered in {@code META-INF/services/datawave.visibility.VisibilityFunctions} is used. Providers are looked up with the
 * class loader that loaded this interface, so on tablet servers the provider jar must be deployed alongside the DataWave jars (for example in
 * {@code lib/ext}).</li>
 * <li>Otherwise {@link Default} is used and a warning is logged.</li>
 * </ol>
 * Implementations must be thread-safe and have a public no-argument constructor. Instances are not managed by any container, so implementations are responsible
 * for their own configuration and caching.
 */
public interface VisibilityFunctions {

    /**
     * System property naming a {@link VisibilityFunctions} implementation class to use instead of {@link ServiceLoader} discovery.
     */
    String CLASS_OVERRIDE_PROPERTY = "datawave.visibility.functions.class";

    /**
     * Combine visibilities into a single visibility that is at least as restrictive as every input. Null and empty inputs are ignored.
     *
     * @param visibilities
     *            the visibilities to combine; may contain nulls
     * @return the combined visibility; never null. An empty {@link ColumnVisibility} is returned when there are no non-empty inputs.
     */
    ColumnVisibility combine(Collection<ColumnVisibility> visibilities);

    /**
     * Combine two visibilities. Unlike {@link #combine(Collection)}, this method preserves the absence of a visibility: if both inputs are null then null is
     * returned, and if only one input is null the other is returned unchanged. This matches how per-field visibilities are combined during ingest, where null
     * means "use the event visibility".
     *
     * @param first
     *            the first visibility, may be null
     * @param second
     *            the second visibility, may be null
     * @return the combined visibility, or null if both inputs are null
     */
    default ColumnVisibility combine(ColumnVisibility first, ColumnVisibility second) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        return combine(Arrays.asList(first, second));
    }

    /**
     * Produce the flattened (normalized) byte form of a visibility, as written to Accumulo keys.
     *
     * @param visibility
     *            the visibility to flatten
     * @return the flattened visibility bytes
     */
    byte[] flatten(ColumnVisibility visibility);

    /**
     * @return the process-wide {@link VisibilityFunctions} instance, resolved once on first use
     */
    static VisibilityFunctions getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * Default implementation: combines visibilities by AND-ing them together and normalizing the result, and flattens using {@link ColumnVisibility#flatten()}
     * (cached).
     */
    class Default implements VisibilityFunctions {

        @Override
        public ColumnVisibility combine(Collection<ColumnVisibility> visibilities) {
            if (visibilities == null) {
                return new ColumnVisibility();
            }

            // @formatter:off
            Set<String> expressions = visibilities.stream()
                            .filter(Objects::nonNull)
                            .map(cv -> AccessExpressionUtil.toAccessExpression(cv).getExpression())
                            .filter(expression -> !expression.isEmpty())
                            .collect(Collectors.toCollection(LinkedHashSet::new));
            // @formatter:on

            if (expressions.isEmpty()) {
                return new ColumnVisibility();
            }

            String combined;
            if (expressions.size() == 1) {
                combined = expressions.iterator().next();
            } else {
                combined = expressions.stream().map(expression -> "(" + expression + ")").collect(Collectors.joining("&"));
            }

            ParsedAccessExpression parsed = AccessExpressionUtil.getAccess().newParsedExpression(combined);
            return new ColumnVisibility(AccessExpressionUtil.normalize(parsed).expression);
        }

        @Override
        public byte[] flatten(ColumnVisibility visibility) {
            return FlattenedVisibilityCache.flatten(visibility);
        }
    }

    /**
     * Lazily resolves the {@link VisibilityFunctions} instance returned by {@link #getInstance()}.
     */
    final class Holder {
        private static final Logger log = LoggerFactory.getLogger(VisibilityFunctions.class);

        static final VisibilityFunctions INSTANCE = load(System.getProperty(CLASS_OVERRIDE_PROPERTY), VisibilityFunctions.class.getClassLoader());

        private Holder() {}

        /**
         * Resolve an implementation.
         *
         * @param classOverride
         *            implementation class name from {@link #CLASS_OVERRIDE_PROPERTY}, or null
         * @param classLoader
         *            the class loader used to load the override class or discover providers
         * @return the resolved implementation
         * @throws IllegalStateException
         *             if the override cannot be instantiated, a registered provider cannot be loaded, or more than one provider is registered
         */
        static VisibilityFunctions load(String classOverride, ClassLoader classLoader) {
            if (classOverride != null && !classOverride.isBlank()) {
                VisibilityFunctions functions = instantiate(classOverride.trim(), classLoader);
                log.info("Using VisibilityFunctions implementation {} from system property {}", functions.getClass().getName(), CLASS_OVERRIDE_PROPERTY);
                return functions;
            }

            List<VisibilityFunctions> providers = new ArrayList<>();
            try {
                ServiceLoader.load(VisibilityFunctions.class, classLoader).forEach(providers::add);
            } catch (ServiceConfigurationError e) {
                throw new IllegalStateException("Failed to load a registered VisibilityFunctions provider", e);
            }

            if (providers.size() > 1) {
                throw new IllegalStateException("Multiple VisibilityFunctions providers registered: "
                                + providers.stream().map(p -> p.getClass().getName()).collect(Collectors.joining(", ")));
            }

            if (providers.size() == 1) {
                VisibilityFunctions functions = providers.get(0);
                log.info("Using VisibilityFunctions provider {}", functions.getClass().getName());
                return functions;
            }

            log.warn("No VisibilityFunctions provider found; using {}. Visibilities will be combined by AND-ing them together.", Default.class.getName());
            return new Default();
        }

        private static VisibilityFunctions instantiate(String className, ClassLoader classLoader) {
            try {
                Class<?> clazz = Class.forName(className, true, classLoader);
                if (!VisibilityFunctions.class.isAssignableFrom(clazz)) {
                    throw new IllegalStateException(className + " does not implement " + VisibilityFunctions.class.getName());
                }
                return (VisibilityFunctions) clazz.getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException | LinkageError e) {
                throw new IllegalStateException("Failed to instantiate VisibilityFunctions implementation " + className, e);
            }
        }
    }
}
