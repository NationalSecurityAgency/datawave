package datawave.ingest.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.DataTypeOverrideHelper;
import datawave.ingest.data.config.filter.KeyValueFilter;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.marking.MarkingFunctions;
import datawave.util.StringUtils;

public class TypeRegistry extends HashMap<String,Type> {

    private static final Logger log = ThreadConfigurableLogger.getLogger(TypeRegistry.class);

    public static final String ALL_PREFIX = "all";

    public static final String ERROR_PREFIX = "error";

    public static final String METRICS_SUMMARY = "metrics-summary";

    public static final String FILE_LEVEL = "file-level";

    public static final String INGEST_DATA_TYPES = "ingest.data.types";

    public static final String INGEST_HELPER = ".ingest.helper.class";

    public static final String RAW_READER = ".reader.class";

    public static final String HANDLER_CLASSES = ".handler.classes";
    public static final String EXCLUDED_HANDLER_CLASSES = "excluded.handler.classes";

    public static final String FILTER_CLASSES = ".filter.classes";

    public static final String FILTER_PRIORITY = ".filter.priority";

    public static final String OUTPUT_NAME = ".output.name";

    private static final long serialVersionUID = 1L;

    private static TypeRegistry registry = null;

    private static final Object lock = new Object();

    private static Multimap<String,String> handlerExpansions = HashMultimap.create();

    public static TypeRegistry getInstance(Configuration config) {
        if (null == registry) {
            synchronized (lock) {
                if (null == registry) {
                    registry = new TypeRegistry(config);
                }
            }
        }
        return registry;
    }

    /**
     * Helps determine whether or not the registry instance has been instantiated.
     *
     * @return true if the registry exists, false otherwise
     */
    public static boolean hasInstance() {
        return registry != null;
    }

    /**
     * @param key
     *            name of type
     * @return type object
     * @throws NoSuchElementException
     *             if type does not exist.
     */
    @Override
    public Type get(Object key) {
        if (!containsKey(key))
            throw new NoSuchElementException("Type " + key + " does not exist in the registry");
        return super.get(key);
    }

    /**
     *
     * @param name
     *            of type
     * @return type object
     * @throws IllegalStateException
     *             if registry not initialized
     */
    public static Type getType(String name) {
        if (null == registry) {
            IllegalStateException e = new IllegalStateException("TypeRegistry has not been initialized.");
            log.error("TypeRegistry has not been initialized.", e);
            throw e;
        } else
            return registry.get(name);
    }

    public static Collection<Type> getTypes() {
        if (null == registry) {
            IllegalStateException e = new IllegalStateException("TypeRegistry has not been initialized.");
            log.error("TypeRegistry has not been initialized.", e);
            throw e;
        } else {

            return registry.values();
        }

    }

    public static Collection<String> getTypeNames() {
        if (null == registry) {

            IllegalStateException e = new IllegalStateException("TypeRegistry has not been initialized.");
            log.error("TypeRegistry has not been initialized.", e);
            throw e;
        } else {
            return registry.keySet();
        }
    }

    public static String getContents() {
        if (null != registry)
            return registry.toString();
        else {
            IllegalStateException e = new IllegalStateException("TypeRegistry has not been initialized.");
            log.error("TypeRegistry has not been initialized.", e);
            throw e;
        }
    }

    /**
     * Method to reset the TypeRegistry, this will be used mostly in testing.
     */
    public static void reset() {
        registry = null;
    }

    private TypeRegistry(Configuration config) {
        super();

        // Ensure the marking functions are initialized before initializing any helper classes, since
        // they may in turn use marking functions (and related features) that depend on this having
        // been initialized already.
        MarkingFunctions.Factory.createMarkingFunctions();

        if (null == config)
            throw new IllegalArgumentException("Cannot pass null configuration to TypeRegistry");

        Set<String> names = new HashSet<>();

        // Be sure to add the all and the error operators
        names.add(ALL_PREFIX);
        names.add(ERROR_PREFIX);
        names.add(METRICS_SUMMARY);
        names.add(FILE_LEVEL);

        // Now add the configured data types to be processed. If no configured types then try em all.
        String[] tempNames = config.getStrings(INGEST_DATA_TYPES);
        if (tempNames != null) {
            tempNames = expandVariables(StringUtils.trimAndRemoveEmptyStrings(tempNames));
        }
        if (tempNames != null && tempNames.length != 0) {
            names.addAll(Arrays.asList(tempNames));
        } else {
            names.addAll(getAllPossibleNames(config));
        }

        // Now iterate over the list of names and check create Type
        // objects
        for (String typeName : names) {
            String helperClassName = null;
            try {
                helperClassName = ConfigurationHelper.isNull(config, typeName + INGEST_HELPER, String.class);
            } catch (IllegalArgumentException e) {
                log.debug("No helper class defined for type: " + typeName);
            }
            String readerClassName = null;
            try {
                readerClassName = ConfigurationHelper.isNull(config, typeName + RAW_READER, String.class);
            } catch (IllegalArgumentException e) {
                log.debug("No reader class defined for type: " + typeName);
            }
            String[] handlerClassNames = null;
            try {
                String[] handlerClasses = StringUtils.trimAndRemoveEmptyStrings(ConfigurationHelper.isNull(config, typeName + HANDLER_CLASSES, String[].class));
                handlerClasses = StringUtils.deDupStringArray(handlerClasses);
                handlerClassNames = expandVariables(handlerClasses);

                Collection<String> exclusions = Arrays
                                .asList(StringUtils.trimAndRemoveEmptyStrings(ConfigurationHelper.isNull(config, EXCLUDED_HANDLER_CLASSES, String[].class)));
                handlerClassNames = getClassnamesWithoutExclusions(handlerClassNames, exclusions);
            } catch (IllegalArgumentException e) {
                log.debug("No handler classes defined for type: " + typeName);
            }

            String[] filterClassNames = null;
            int filterPriority = Integer.MAX_VALUE;
            try {
                filterClassNames = expandVariables(
                                StringUtils.trimAndRemoveEmptyStrings(ConfigurationHelper.isNull(config, typeName + FILTER_CLASSES, String[].class)));
                filterPriority = config.getInt(typeName + FILTER_PRIORITY, Integer.MAX_VALUE);
            } catch (IllegalArgumentException e) {
                log.debug("No filter classes defined for type: " + typeName);
            }

            String outputName = config.get(typeName + OUTPUT_NAME, typeName);

            try {
                Class<? extends IngestHelperInterface> helperClass = null;
                if (null != helperClassName)
                    helperClass = getHelperClass(helperClassName);
                Class<? extends RecordReader<?,?>> readerClass = null;
                if (null != readerClassName)
                    readerClass = getReaderClass(readerClassName);

                if (helperClass != null || readerClass != null || handlerClassNames != null || filterClassNames != null) {
                    // This is a Type we want to add. Assert that a datatype does not contain a period.
                    // Handlers often cannot determine Datatype during setup() from configuration parameters short of
                    // performing `configurationKey.split(".")[0]`. Using a period inside datatype name muddies later code
                    // due to the manner than Hadoop Configurations operate.
                    if (typeName.indexOf('.') != -1) {
                        log.error("Datatypes ('" + INGEST_DATA_TYPES + "') cannot contain a period. Offending datatype: '" + typeName + "'");
                        throw new IllegalArgumentException(
                                        "Datatypes ('" + INGEST_DATA_TYPES + "') cannot contain a period. Offending datatype: '" + typeName + "'");
                    }

                    Type t = new Type(typeName, outputName, helperClass, readerClass, handlerClassNames, filterPriority, filterClassNames);
                    log.debug("Registered type " + t);
                    this.put(typeName, t);

                    if (null != config.get(typeName + DataTypeOverrideHelper.Properties.DATA_TYPE_VALUES)) {
                        for (String type : config.getStrings(typeName + DataTypeOverrideHelper.Properties.DATA_TYPE_VALUES)) {
                            outputName = config.get(type + OUTPUT_NAME, outputName);
                            t = new Type(type, outputName, helperClass, readerClass, handlerClassNames, filterPriority, filterClassNames);
                            log.debug("Registered child type:" + type);
                            this.put(type, t);
                        }
                    }
                }

            } catch (ClassNotFoundException cnfe) {
                log.error("Unable to create supporting class for type " + typeName, cnfe);
            }

        }
    }

    private String[] getClassnamesWithoutExclusions(String[] classnames, Collection<String> exclusions) {
        ArrayList<String> scrubbedClassnames = new ArrayList<>();
        for (String classname : classnames) {
            if (!exclusions.contains(classname)) {
                scrubbedClassnames.add(classname);
            }
        }
        return scrubbedClassnames.toArray(new String[scrubbedClassnames.size()]);
    }

    private Set<String> getAllPossibleNames(Configuration config) {
        // Loop through the all of the property names in the configuration
        // and extract parts of the property name up to the first period and second periods
        Set<String> names = new HashSet<>();
        for (Map.Entry<String,String> entry : config) {
            int idx = entry.getKey().indexOf('.');
            if (-1 != idx) {
                names.add(entry.getKey().substring(0, idx));
                idx = entry.getKey().indexOf('.', idx + 1);
                if (-1 != idx) {
                    names.add(entry.getKey().substring(0, idx));
                }
            }
        }

        // Remove some of the names that exist in the HADOOP configuration by default
        names.remove("data");
        names.remove("job");
        names.remove("tasktracker");
        names.remove("io");
        names.remove("hadoop");
        names.remove("mapred");
        names.remove("jobclient");
        names.remove("ipc");
        names.remove("fs");
        names.remove("map");
        names.remove("topology");
        names.remove("local");
        names.remove("keep");
        names.remove("webinterface");
        names.remove("file");
        names.remove("accumulo");
        names.remove("num");
        names.remove("yarn.timeline-service");

        return names;
    }

    @SuppressWarnings("unchecked")
    private Class<? extends IngestHelperInterface> getHelperClass(String className) throws ClassNotFoundException {
        return (Class<? extends IngestHelperInterface>) Class.forName(className);
    }

    @SuppressWarnings("unchecked")
    private Class<? extends RecordReader<?,?>> getReaderClass(String className) throws ClassNotFoundException {
        return (Class<? extends RecordReader<?,?>>) Class.forName(className);
    }

    @SuppressWarnings("unchecked")
    public static Class<? extends DataTypeHandler<?>> getHandlerClass(String className) throws ClassNotFoundException {
        return (Class<? extends DataTypeHandler<?>>) Class.forName(className);
    }

    @SuppressWarnings("unchecked")
    public static Class<? extends KeyValueFilter<?,?>> getFilterClass(String className) throws ClassNotFoundException {
        return (Class<? extends KeyValueFilter<?,?>>) Class.forName(className);
    }

    /**
     * A mechanism to configure an expansion of a variable into a list of handlers. This must be called before the TypeRegistry is initialized.
     *
     * @param variable
     *            the name of a variable to expand into a list of handlers
     * @param handlers
     *            the list of handler class names which should replace references to {@code variable}
     */
    public static void setVariableExpansion(String variable, String[] handlers) {
        variable = getVariableName(variable);
        for (String handler : handlers) {
            handlerExpansions.put(variable, handler);
        }
    }

    public static Collection<String> getVariableExpansion(String variable) {
        String variableBase = getVariableName(variable);
        if (variable.charAt(0) == '$' || handlerExpansions.containsKey(variableBase)) {
            return handlerExpansions.get(variableBase);
        }
        return null;
    }

    /**
     * We have some standard handler environment variable expansions
     *
     * @param variables
     *            the environment variables to expand
     * @return the input array, with variable expansion performed
     */
    private String[] expandVariables(String[] variables) {
        List<String> expandedVariables = null;
        for (int i = 0; i < variables.length; i++) {
            Collection<String> expansions = getVariableExpansion(variables[i]);
            if (expansions != null) {
                if (expandedVariables == null) {
                    expandedVariables = new ArrayList<>();
                    expandedVariables.addAll(Arrays.asList(variables).subList(0, i));
                }
                expandedVariables.addAll(expansions);
            } else if (expandedVariables != null) {
                expandedVariables.add(variables[i]);
            }
        }
        if (expandedVariables != null) {
            return expandedVariables.toArray(new String[expandedVariables.size()]);
        } else {
            return variables;
        }
    }

    private static String getVariableName(String variableName) {
        if (variableName.charAt(0) == '$') {
            if (variableName.charAt(1) == '{') {
                if (variableName.charAt(variableName.length() - 1) == '}') {
                    variableName = variableName.substring(2, variableName.length() - 1);
                } else {
                    variableName = variableName.substring(2);
                }
            } else {
                variableName = variableName.substring(1);
            }
        }
        return variableName;
    }

}
