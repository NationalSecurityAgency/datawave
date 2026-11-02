package datawave.query.jexl;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl3.JexlArithmetic;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlOperator;
import org.apache.commons.jexl3.introspection.JexlPermissions;

import datawave.query.attributes.ValueTuple;
import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.functions.ContentFunctions;
import datawave.query.jexl.functions.EvaluationPhaseFilterFunctions;
import datawave.query.jexl.functions.GeoFunctions;
import datawave.query.jexl.functions.GeoWaveFunctions;
import datawave.query.jexl.functions.GroupingRequiredFilterFunctions;
import datawave.query.jexl.functions.NormalizationFunctions;
import datawave.query.jexl.functions.QueryFunctions;
import datawave.query.util.Tuple1;
import datawave.query.util.Tuple2;
import datawave.query.util.Tuple3;

/**
 * The execution permissions for DataWave queries. Only named query functions, read operations on query values, and arithmetic operators are exposed.
 * Constructors and public fields are never exposed. Adding a function or a value method requires an explicit change to this policy.
 * <p>
 * These permissions constrain JEXL introspection, not Java calls inside an allowed function. Function implementations and objects supplied by the server
 * remain trusted code.
 */
public final class DatawaveJexlPermissions implements JexlPermissions {
    public static final DatawaveJexlPermissions INSTANCE = new DatawaveJexlPermissions();

    private static final JexlPermissions ANNOTATIONS = JexlPermissions.parse();
    private static final Map<Class<?>,Set<String>> FUNCTIONS = functions();
    private static final Set<String> OBJECT_METHODS = Set.of("equals", "hashCode", "toString");
    private static final Set<String> COLLECTION_METHODS = Set.of("size", "isEmpty", "contains", "containsAll", "iterator", "toArray");
    private static final Set<String> MAP_METHODS = Set.of("size", "isEmpty", "containsKey", "containsValue", "get", "keySet", "values", "entrySet");
    private static final Set<String> SET_METHODS = Set.of("getValueForGroup", "getValuesForGroups", "getGroupsForValue", "min", "max", "compareWith", "lessThan",
                    "greaterThan");
    private static final Set<String> TUPLE_METHODS = Set.of("getFieldName", "getValue", "getNormalizedValue", "first", "second", "third");
    private static final Set<String> STRING_METHODS = Set.of("length", "isEmpty", "charAt", "substring", "subSequence", "contains", "startsWith", "endsWith",
                    "indexOf", "lastIndexOf", "matches", "equalsIgnoreCase", "compareTo", "compareToIgnoreCase", "toLowerCase", "toUpperCase", "trim", "strip",
                    "stripLeading", "stripTrailing", "isBlank", "replace", "replaceAll", "replaceFirst", "split", "concat");
    private static final Set<String> NUMBER_METHODS = Set.of("byteValue", "shortValue", "intValue", "longValue", "floatValue", "doubleValue", "compareTo");
    private static final Set<String> DATE_METHODS = Set.of("getTime", "before", "after", "compareTo");
    private static final Set<String> OTHER_VALUE_METHODS = Set.of("get", "getKey", "hasNext", "next", "booleanValue", "charValue");
    private static final Set<String> PACKAGES = Set.of("java.lang", "java.math", "java.util", "org.apache.commons.jexl3", "datawave.query.jexl",
                    "datawave.query.jexl.functions", "datawave.query.collections", "datawave.query.attributes", "datawave.query.util");
    private static final Set<Class<?>> SCALAR_CLASSES = Set.of(String.class, Boolean.class, Character.class, Byte.class, Short.class, Integer.class,
                    Long.class, Float.class, Double.class, Number.class, BigInteger.class, BigDecimal.class, Date.class);
    private static final Set<String> ARITHMETIC_METHODS = arithmeticMethods();

    private DatawaveJexlPermissions() {}

    /**
     * All DataWave engine constructors use this policy, including constructors accepting a caller-supplied builder.
     */
    static JexlBuilder configure(JexlBuilder builder) {
        if (builder.uberspect() != null) {
            throw new IllegalArgumentException("A custom JEXL uberspect would bypass DataWave query permissions");
        }
        if (builder.namespaces() != null) {
            for (Map.Entry<String,Object> entry : builder.namespaces().entrySet()) {
                if (!(entry.getValue() instanceof Class<?>) || !FUNCTIONS.containsKey(entry.getValue())) {
                    throw new IllegalArgumentException("JEXL function namespace is not permitted: " + entry.getKey());
                }
            }
        }
        return builder.permissions(INSTANCE);
    }

    public static boolean allowsFunction(Object functionClass, String name) {
        return FUNCTIONS.getOrDefault(functionClass, Collections.emptySet()).contains(name);
    }

    public static boolean allowsQueryMethod(String name) {
        return OBJECT_METHODS.contains(name) || COLLECTION_METHODS.contains(name) || MAP_METHODS.contains(name) || SET_METHODS.contains(name)
                        || TUPLE_METHODS.contains(name) || STRING_METHODS.contains(name) || NUMBER_METHODS.contains(name) || DATE_METHODS.contains(name)
                        || OTHER_VALUE_METHODS.contains(name);
    }

    @Override
    public boolean allow(Package pack) {
        return ANNOTATIONS.allow(pack) && PACKAGES.contains(pack.getName());
    }

    @Override
    public boolean allow(Class<?> clazz) {
        return ANNOTATIONS.allow(clazz) && (FUNCTIONS.containsKey(clazz) || SCALAR_CLASSES.contains(clazz) || clazz == Object.class || clazz.isArray()
                        || FunctionalSet.class.isAssignableFrom(clazz) || clazz == ValueTuple.class || clazz == Tuple1.class || clazz == Tuple2.class
                        || clazz == Tuple3.class
                        || JexlArithmetic.class.isAssignableFrom(clazz) || isJavaContainer(clazz));
    }

    @Override
    public boolean allow(Constructor<?> constructor) {
        return false;
    }

    @Override
    public boolean allow(Field field) {
        return false;
    }

    @Override
    public boolean allow(Method method) {
        if (!ANNOTATIONS.allow(method)) {
            return false;
        }
        Class<?> clazz = method.getDeclaringClass();
        String name = method.getName();
        if (FUNCTIONS.containsKey(clazz)) {
            return FUNCTIONS.get(clazz).contains(name);
        }
        if (JexlArithmetic.class.isAssignableFrom(clazz)) {
            return ARITHMETIC_METHODS.contains(name);
        }
        if (clazz == Object.class) {
            return OBJECT_METHODS.contains(name);
        }
        if (clazz == FunctionalSet.class) {
            return COLLECTION_METHODS.contains(name) || SET_METHODS.contains(name) || OBJECT_METHODS.contains(name);
        }
        if (clazz == ValueTuple.class || clazz == Tuple1.class || clazz == Tuple2.class || clazz == Tuple3.class) {
            return TUPLE_METHODS.contains(name) || OBJECT_METHODS.contains(name);
        }
        if (isJavaContainer(clazz)) {
            return OBJECT_METHODS.contains(name) || (Collection.class.isAssignableFrom(clazz) && COLLECTION_METHODS.contains(name))
                            || (List.class.isAssignableFrom(clazz) && name.equals("get"))
                            || (Map.class.isAssignableFrom(clazz) && MAP_METHODS.contains(name))
                            || (Map.Entry.class.isAssignableFrom(clazz) && Set.of("getKey", "getValue").contains(name))
                            || (Iterator.class.isAssignableFrom(clazz) && Set.of("hasNext", "next").contains(name));
        }
        if (SCALAR_CLASSES.contains(clazz)) {
            return OBJECT_METHODS.contains(name) || (clazz == String.class && STRING_METHODS.contains(name))
                            || (Number.class.isAssignableFrom(clazz) && NUMBER_METHODS.contains(name))
                            || (clazz == Date.class && DATE_METHODS.contains(name)) || (clazz == Boolean.class && name.equals("booleanValue"))
                            || (clazz == Character.class && name.equals("charValue"));
        }
        return false;
    }

    @Override
    public JexlPermissions compose(String... source) {
        throw new UnsupportedOperationException("DataWave query permissions cannot be widened");
    }

    private static boolean isJavaContainer(Class<?> clazz) {
        return clazz.getPackageName().equals("java.util") && (Collection.class.isAssignableFrom(clazz) || Map.class.isAssignableFrom(clazz)
                        || Map.Entry.class.isAssignableFrom(clazz) || Iterator.class.isAssignableFrom(clazz));
    }

    private static Set<String> arithmeticMethods() {
        Set<String> methods = new HashSet<>();
        for (JexlOperator operator : JexlOperator.values()) {
            methods.add(operator.getMethodName());
        }
        methods.addAll(Set.of("toBoolean", "toInteger", "toLong", "toBigInteger", "toBigDecimal", "toDouble", "toString", "narrow"));
        return Collections.unmodifiableSet(methods);
    }

    private static Map<Class<?>,Set<String>> functions() {
        Map<Class<?>,Set<String>> functions = new HashMap<>();
        functions.put(ContentFunctions.class, Set.of("within", "adjacent", "phrase", "scoredPhrase"));
        functions.put(NormalizationFunctions.class, Set.of("ipv4"));
        functions.put(EvaluationPhaseFilterFunctions.class,
                        Set.of("occurrence", "isNull", "isNotNull", "excludeRegex", "includeRegex", "matchesAtLeastCountOf", "getAllMatches", "afterLoadDate",
                                        "beforeLoadDate", "betweenLoadDates", "afterDate", "beforeDate", "betweenDates", "timeFunction", "getMaxTime",
                                        "getMinTime", "getMaxValue", "getMinValue", "compare"));
        functions.put(GroupingRequiredFilterFunctions.class, Set.of("getGroupsForMatchesInGroup", "matchesInGroup", "matchesInGroupLeft", "atomValuesMatch"));
        functions.put(QueryFunctions.class, Set.of("length", "between", "matchRegex", "includeText"));
        functions.put(GeoFunctions.class, Set.of("within_bounding_box", "within_circle"));
        functions.put(GeoWaveFunctions.class, Set.of("contains", "covers", "covered_by", "crosses", "intersects", "overlaps", "within"));
        return Collections.unmodifiableMap(functions);
    }
}
