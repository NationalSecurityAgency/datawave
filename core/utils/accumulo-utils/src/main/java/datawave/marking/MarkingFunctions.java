package datawave.marking;

import static datawave.marking.AccessExpressionMarkings.ACCESS;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.access.AccessExpression;
import org.apache.accumulo.access.ParsedAccessExpression;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Accumulo marks all data with a columnVisibility that declares and controls access. MarkingFunctions provide a pattern for mapping a user's preferred means of
 * declaring access controls with the Accumulo columnVisibility pattern. As an example, James Bond might use MarkingFunctions to translate a the
 * columnVisibility FYEO into the song "For Your Eyes Only" A hospital might use a columnVisibility like 'PPI' but prefer to mark data input and results with a
 * human-readable marking like "Patient Privileged Information"
 */

public interface MarkingFunctions<T extends Markings<?>> {

    ColumnVisibility combineVisibilities(Collection<ColumnVisibility> visibilities) throws MarkingFunctions.Exception;

    T combine(Collection<Markings<?>> markings) throws MarkingFunctions.Exception;

    T combine(Markings<?> markings1, Markings<?> markings2) throws MarkingFunctions.Exception;

    Markings<?> translateFromColumnVisibility(ColumnVisibility columnVisibility) throws MarkingFunctions.Exception;

    Markings<?> translateFromColumnVisibilityForAuths(ColumnVisibility columnVisibility, Collection<Authorizations> authorizations)
                    throws MarkingFunctions.Exception;

    Markings<?> translateFromColumnVisibilityForAuths(ColumnVisibility columnVisibility, Authorizations authorizations) throws MarkingFunctions.Exception;

    ColumnVisibility translateToColumnVisibility(Markings<?> markings) throws MarkingFunctions.Exception;

    byte[] flatten(ColumnVisibility vis);

    class Exception extends java.lang.Exception {

        public Exception() {
            super();
        }

        public Exception(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }

        public Exception(String message, Throwable cause) {
            super(message, cause);
        }

        public Exception(String message) {
            super(message);
        }

        public Exception(Throwable cause) {
            super(cause);
        }
    }

    class Default implements MarkingFunctions<AccessExpressionMarkings> {

        @Override
        public ColumnVisibility combineVisibilities(Collection<ColumnVisibility> visibilities) {
            AccessExpressionMarkings accessExpressionMarkings = combine(
                            visibilities.stream().filter(Objects::nonNull).map(AccessExpressionMarkings::createMarkings).collect(Collectors.toList()));

            if (null == accessExpressionMarkings) {
                // is this correct behavior, does it match other implementations?
                // switching to null causes lots of NPEs so leaving this as is
                return new ColumnVisibility();
            }

            return accessExpressionMarkings.toColumnVisibility();
        }

        @Override
        public AccessExpressionMarkings combine(Collection<Markings<?>> markings) {

            Set<AccessExpressionMarkings> uniqueMarkings = new HashSet<>();

            for (Markings<?> marking : markings) {
                if (null == marking) {
                    continue;
                }
                if (!(marking instanceof AccessExpressionMarkings)) {
                    throw new RuntimeException(String.format("Unknown markings class %s", marking.getClass().getName()));
                }
                uniqueMarkings.add((AccessExpressionMarkings) marking);
            }

            if (uniqueMarkings.isEmpty()) {
                return null;
            }

            if (uniqueMarkings.size() == 1) {
                return uniqueMarkings.stream().findFirst().get();
            }

            Set<AccessExpression> uniqueExpressions = uniqueMarkings.stream().map(AccessExpressionMarkings::getMarkings).collect(Collectors.toSet());

            // filter out any empty expressions and concatenate with '&'
            // @formatter:off
            ParsedAccessExpression expression = ACCESS.newParsedExpression(uniqueExpressions
                                                .stream()
                                                .map(AccessExpression::getExpression)
                                                .filter(str -> !str.isEmpty())
                                                .map(str -> "(" + str + ")")
                                                .collect(Collectors.joining("&"))
                                                );
            // @formatter:on

            // normalize the Parsed Expression and then return a copy without the parse tree
            return AccessExpressionMarkings.builder().columnVisibility(AccessExpressionUtil.normalize(expression).expression).build();
        }

        @Override
        public AccessExpressionMarkings combine(Markings<?> markings1, Markings<?> markings2) {

            if (markings1 == null && markings2 == null) {
                return null;
            }

            if (markings2 == null) {
                return (AccessExpressionMarkings) markings1;
            }

            if (markings1 == null) {
                return (AccessExpressionMarkings) markings2;
            }

            if (markings1 instanceof AccessExpressionMarkings && markings2 instanceof AccessExpressionMarkings) {
                AccessExpressionMarkings aem1 = (AccessExpressionMarkings) markings1;
                AccessExpressionMarkings aem2 = (AccessExpressionMarkings) markings2;
                return combine(List.of(aem1, aem2));
            }

            throw new RuntimeException(String.format("Unknown markings class %s or %s", markings1.getClass().getName(), markings2.getClass().getName()));

        }

        @Override
        public Markings<?> translateFromColumnVisibility(ColumnVisibility expression) {
            String cv = new String(expression.getExpression(), StandardCharsets.UTF_8);
            return AccessExpressionMarkings.builder().accessExpression(AccessExpressionUtil.normalize(cv)).build();
        }

        @Override
        public Markings<?> translateFromColumnVisibilityForAuths(ColumnVisibility columnVisibility, Collection<Authorizations> authorizations) {
            return translateFromColumnVisibility(columnVisibility);
        }

        @Override
        public Markings<?> translateFromColumnVisibilityForAuths(ColumnVisibility columnVisibility, Authorizations authorizations) {
            return translateFromColumnVisibility(columnVisibility);
        }

        @Override
        public ColumnVisibility translateToColumnVisibility(Markings<?> markings) {
            if (markings == null) {
                return null;
            }

            if (markings instanceof AccessExpressionMarkings) {
                AccessExpressionMarkings aem = (AccessExpressionMarkings) markings;
                return aem.toColumnVisibility();
            }

            throw new RuntimeException(String.format("Unknown markings class %s or %s", markings.getClass().getName()));
        }

        @Override
        public byte[] flatten(ColumnVisibility vis) {
            return FlattenedVisibilityCache.flatten(vis);
        }
    }

    /**
     * this Factory for MarkingFunctions is designed to be used on the tservers, where there is a vfs-classloader
     */
    class Factory {
        public static final Logger log = LoggerFactory.getLogger(Factory.class);

        private static MarkingFunctions<?> markingFunctions;

        public static synchronized MarkingFunctions<?> createMarkingFunctions() {
            if (markingFunctions != null)
                return markingFunctions;
            ClassLoader thisClassLoader = Factory.class.getClassLoader();

            // ignore calls to close as this blows away the cache manager
            ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext();
            try {
                // To prevent failure when this is run on the tservers:
                // The VFS ClassLoader has been created and has been made the current thread's context classloader, but its resource paths are empty at this
                // time.
                // The spring ApplicationContext will prefer the current thread's context classloader, so the spring context would fail to find
                // any classes or context files to load.
                // Instead, set the classloader on the ApplicationContext to be the one that is loading this class.
                // It is a VFSClassLoader that has the accumulo lib/ext jars set as its resources.
                // After setting the classloader, then set the config locations and refresh the context.
                context.setClassLoader(thisClassLoader);
                context.setConfigLocations("classpath*:/MarkingFunctionsContext.xml");
                context.refresh();
                markingFunctions = context.getBean("markingFunctions", MarkingFunctions.class);
            } catch (Throwable t) {
                // got here because the VFSClassLoader on the tservers does not implement findResources
                // none of the spring wiring will work.
                log.warn("Could not load spring context files. got " + t);
            }

            return markingFunctions;
        }
    }
}
