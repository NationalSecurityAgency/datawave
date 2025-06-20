package datawave.springframework.integration;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to compare two Spring application contexts for bean definitions and values. Provides methods to: - Verify both contexts contain the same bean
 * names - Recursively compare bean field values between contexts - Return a structured list of differences with context
 */
public class SpringContextComparator {

    private static final Logger log = LoggerFactory.getLogger(SpringContextComparator.class);

    /**
     * Represents a single difference found during context comparison.
     */
    public static class ComparisonDifference {
        public final String beanName;
        public final String fieldPath;
        public final Object value1;
        public final Object value2;
        public final String description;

        public ComparisonDifference(String beanName, String fieldPath, Object value1, Object value2, String description) {
            this.beanName = beanName;
            this.fieldPath = fieldPath;
            this.value1 = value1;
            this.value2 = value2;
            this.description = description;
        }

        @Override
        public String toString() {
            return String.format("[%s.%s] Context1: %s | Context2: %s | %s", beanName, fieldPath, value1, value2, description);
        }
    }

    public static List<ComparisonDifference> compareObjectsRecursive(String beanName, Object o1, Object o2, Collection<Class<?>> ignoredClasses) {
        return compareObjectsRecursive(beanName, o1, o2, "", new HashSet<>(), ignoredClasses);
    }

    private static List<ComparisonDifference> compareObjectsRecursive(String beanName, Object o1, Object o2, String fieldPath, Set<String> visitedObjects,
                    Collection<Class<?>> ignoredClasses) {
        List<ComparisonDifference> differences = new ArrayList<>();
        // Base case: null checks
        if (o1 == null && o2 == null) {
            log.trace("{}.{} both null - EQUAL", beanName, fieldPath);
            return differences;
        }
        if (o1 == null || o2 == null) {
            String msg = String.format("Null mismatch: o1=%s, o2=%s", o1, o2);
            log.debug("{}.{} {}", beanName, fieldPath, msg);
            differences.add(new ComparisonDifference(beanName, fieldPath, o1, o2, msg));
            return differences;
        }

        // Base case: prevent infinite recursion on circular references
        String objectIdentity = System.identityHashCode(o1) + ":" + System.identityHashCode(o2);
        if (visitedObjects.contains(objectIdentity)) {
            log.trace("{}.{} circular reference detected - SKIPPING", beanName, fieldPath);
            return differences;
        }
        visitedObjects.add(objectIdentity);

        // Base case: skip ignored classes
        if (isIgnoredClass(o1.getClass(), ignoredClasses)) {
            log.debug("{}.{} skipped - ignored class: {}", beanName, fieldPath, o1.getClass().getSimpleName());
            return differences;
        }

        // Base case: skip primitive wrappers and immutable types (leaf nodes)
        if (isPrimitiveOrImmutable(o1.getClass())) {
            differences.addAll(compareLeafValues(beanName, o1, o2, fieldPath));
            return differences;
        }

        // Recursive case: introspect object fields
        try {
            for (Field f : o1.getClass().getDeclaredFields()) {
                try {
                    f.setAccessible(true);
                    Object fieldValue1 = f.get(o1);
                    Object fieldValue2 = f.get(o2);

                    String newFieldPath = fieldPath.isEmpty() ? f.getName() : fieldPath + "." + f.getName();

                    // Skip null fields and ignored state classes
                    if (fieldValue1 == null && fieldValue2 == null) {
                        log.trace("{}.{} both null - EQUAL", beanName, newFieldPath);
                        continue;
                    }
                    if (fieldValue1 != null && isIgnoredClass(fieldValue1.getClass(), ignoredClasses)) {
                        log.debug("{}.{} skipped - ignored class: {}", beanName, newFieldPath, fieldValue1.getClass().getSimpleName());
                        continue;
                    }

                    // Convert AtomicInteger to Integer for comparison
                    if (fieldValue1 instanceof AtomicInteger) {
                        fieldValue1 = ((AtomicInteger) fieldValue1).get();
                        fieldValue2 = ((AtomicInteger) fieldValue2).get();
                    }

                    // Compare leaf values
                    if (isPrimitiveOrImmutable(fieldValue1 != null ? fieldValue1.getClass() : fieldValue2.getClass())) {
                        differences.addAll(compareLeafValues(beanName, fieldValue1, fieldValue2, newFieldPath));
                    } else if (fieldValue1 != null && Map.class.isAssignableFrom(fieldValue1.getClass())) {
                        differences.addAll(compareMaps(beanName, (Map<?,?>) fieldValue1, (Map<?,?>) fieldValue2, newFieldPath, visitedObjects, ignoredClasses));
                    } else if (fieldValue1 != null && Collection.class.isAssignableFrom(fieldValue1.getClass())) {
                        differences.addAll(compareCollections(beanName, (Collection<?>) fieldValue1, (Collection<?>) fieldValue2, newFieldPath, visitedObjects,
                                        ignoredClasses));
                    } else {
                        // Recursively compare objects
                        differences.addAll(compareObjectsRecursive(beanName, fieldValue1, fieldValue2, newFieldPath, visitedObjects, ignoredClasses));
                    }
                } catch (IllegalAccessException e) {
                    log.warn("{}.{} could not access field: {}", beanName, fieldPath, e.getMessage());
                }
            }
        } catch (Exception e) {
            String msg = String.format("Exception during recursive comparison: %s", e.getMessage());
            log.error("{}.{} {}", beanName, fieldPath, msg, e);
            differences.add(new ComparisonDifference(beanName, fieldPath, null, null, msg));
        }
        return differences;
    }

    private static List<ComparisonDifference> compareLeafValues(String beanName, Object value1, Object value2, String fieldPath) {
        List<ComparisonDifference> differences = new ArrayList<>();
        boolean equal = objectsEqual(value1, value2);
        if (equal) {
            log.trace("{}.{} = {} - EQUAL", beanName, fieldPath, value1);
        } else {
            log.debug("{}.{} DIFFERENCE - Context1: {} | Context2: {}", beanName, fieldPath, value1, value2);
            differences.add(new ComparisonDifference(beanName, fieldPath, value1, value2, "Value mismatch"));
        }
        return differences;
    }

    private static List<ComparisonDifference> compareCollections(String beanName, Collection<?> value1, Collection<?> value2, String fieldPath,
                    Set<String> visitedObjects, Collection<Class<?>> ignoredClasses) {
        List<ComparisonDifference> differences = new ArrayList<>();

        // Handle null cases
        if (value1 == null && value2 == null) {
            log.trace("{}.{} both null - EQUAL", beanName, fieldPath);
            return differences;
        }
        if (value1 == null || value2 == null) {
            String msg = String.format("Null mismatch: value1=%s, value2=%s", value1, value2);
            log.debug("{}.{} {}", beanName, fieldPath, msg);
            differences.add(new ComparisonDifference(beanName, fieldPath, value1, value2, msg));
            return differences;
        }

        // Check size mismatch
        if (value1.size() != value2.size()) {
            String msg = String.format("Collection size mismatch: size1=%d, size2=%d", value1.size(), value2.size());
            log.debug("{}.{} {}", beanName, fieldPath, msg);
            differences.add(new ComparisonDifference(beanName, fieldPath, value1, value2, msg));
            return differences;
        }

        // Compare collection elements
        List<?> list1 = new ArrayList<>(value1);
        List<?> list2 = new ArrayList<>(value2);

        for (int i = 0; i < list1.size(); i++) {
            Object elem1 = list1.get(i);
            Object elem2 = list2.get(i);
            String elementPath = fieldPath + "[" + i + "]";

            Class clazz = elem1 != null ? elem1.getClass() : (elem2 != null ? elem2.getClass() : Object.class);
            if (isIgnoredClass(clazz, ignoredClasses)) {
                log.debug("{}.{} skipped - ignored class: {}", beanName, elementPath, elem1.getClass().getSimpleName());
            } else {
                if (isPrimitiveOrImmutable(clazz)) {
                    differences.addAll(compareLeafValues(beanName, elem1, elem2, elementPath));
                } else {
                    differences.addAll(compareObjectsRecursive(beanName, elem1, elem2, elementPath, visitedObjects, ignoredClasses));
                }
            }
        }
        return differences;
    }

    private static List<ComparisonDifference> compareMaps(String beanName, Map<?,?> value1, Map<?,?> value2, String fieldPath, Set<String> visitedObjects,
                    Collection<Class<?>> ignoredClasses) {
        List<ComparisonDifference> differences = new ArrayList<>();

        // Handle null cases
        if (value1 == null && value2 == null) {
            log.trace("{}.{} both null - EQUAL", beanName, fieldPath);
            return differences;
        }
        if (value1 == null || value2 == null) {
            String msg = String.format("Null mismatch: value1=%s, value2=%s", value1, value2);
            log.debug("{}.{} {}", beanName, fieldPath, msg);
            differences.add(new ComparisonDifference(beanName, fieldPath, value1, value2, msg));
            return differences;
        }

        // Check size mismatch
        if (value1.size() != value2.size()) {
            String msg = String.format("Map size mismatch: size1=%d, size2=%d", value1.size(), value2.size());
            log.debug("{}.{} {}", beanName, fieldPath, msg);
            differences.add(new ComparisonDifference(beanName, fieldPath, value1, value2, msg));
            return differences;
        }

        // Check if all keys match
        Set<?> keys1 = value1.keySet();
        Set<?> keys2 = value2.keySet();
        if (!keys1.equals(keys2)) {
            String msg = String.format("Map keys mismatch: keys1=%s, keys2=%s", keys1, keys2);
            log.debug("{}.{} {}", beanName, fieldPath, msg);
            differences.add(new ComparisonDifference(beanName, fieldPath, value1, value2, msg));
            return differences;
        }

        // Compare map entries
        for (Object key : keys1) {
            Object mapValue1 = value1.get(key);
            Object mapValue2 = value2.get(key);
            String elementPath = fieldPath + "[" + key + "]";

            Class clazz = mapValue1 != null ? mapValue1.getClass() : (mapValue2 != null ? mapValue2.getClass() : Object.class);
            if (isIgnoredClass(clazz, ignoredClasses)) {
                log.debug("{}.{} skipped - ignored class: {}", beanName, elementPath, mapValue1.getClass().getSimpleName());
            } else {
                if (isPrimitiveOrImmutable(clazz)) {
                    differences.addAll(compareLeafValues(beanName, mapValue1, mapValue2, elementPath));
                } else {
                    differences.addAll(compareObjectsRecursive(beanName, mapValue1, mapValue2, elementPath, visitedObjects, ignoredClasses));
                }
            }
        }
        return differences;
    }

    private static boolean objectsEqual(Object o1, Object o2) {
        if (o1 == null || o2 == null) {
            return o1 == o2;
        }
        return o1.equals(o2);
    }

    private static boolean isPrimitiveOrImmutable(Class<?> clazz) {
        return clazz.isPrimitive() || clazz == String.class || clazz == Integer.class || clazz == Long.class || clazz == Double.class || clazz == Float.class
                        || clazz == Boolean.class || clazz == Short.class || clazz == Byte.class || clazz == Character.class;
    }

    private static boolean isIgnoredClass(Class<?> clazz, Collection<Class<?>> ignoredClasses) {
        for (Class<?> ignoredClass : ignoredClasses) {
            if (ignoredClass.isAssignableFrom(clazz) || clazz.getCanonicalName() == null || clazz.getCanonicalName().startsWith("jdk.internal")
                            || clazz.getCanonicalName().startsWith("java.lang.module") || clazz.getCanonicalName().startsWith("java.lang.invoke")) {
                return true;
            }
        }
        return false;
    }
}
