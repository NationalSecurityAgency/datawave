package datawave.ingest.mapreduce.job;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import datawave.ingest.config.TableConfigCache;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.filter.KeyValueFilter;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.job.metrics.MetricsConfiguration;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.iterators.PropogatingIterator;

/**
 * This class serves as the liaison between datawave job configuration and accumulo tables. Most of this was ripped out of IngestJob for more convenient reuse
 * in other Jobs
 **/
public class TableConfigurationUtil {

    protected static final Logger log = Logger.getLogger(TableConfigurationUtil.class.getName());
    public static final String ITERATOR_CLASS_MARKER = "iterClass";
    public static final String TABLES_CONFIGS_TO_CACHE = "tables.configs.to.cache";
    public static final String TABLE_PROPERTIES_TO_CACHE = "cache.table.properties";
    public static final String TABLE_CONFIGURATION_PROPERTY = ".table.accumulo.configuration";
    public static final String JOB_INPUT_TABLE_NAMES = "job.input.table.names";
    public static final String JOB_OUTPUT_TABLE_NAMES = "job.output.table.names";
    private AccumuloHelper accumuloHelper;
    private TreeMap<String,Map<Integer,Map<String,String>>> combiners = new TreeMap<>();
    private TreeMap<String,Map<Integer,Map<String,String>>> aggregators = new TreeMap<>();
    Configuration conf;
    private TableConfigCache tableConfigCache;

    // for testing
    private boolean usingFileCache = false;

    public TableConfigurationUtil(Configuration conf) {
        accumuloHelper = new AccumuloHelper();
        accumuloHelper.setup(conf);
        this.conf = conf;
        tableConfigCache = TableConfigCache.getCurrentCache(conf);

    }

    public static Set<String> getJobOutputTableNames(Configuration conf) {
        HashSet tableNames = new HashSet<>();

        String[] outputTables = conf.getStrings(JOB_OUTPUT_TABLE_NAMES);

        if (null != outputTables && outputTables.length > 0) {
            tableNames = new HashSet(Arrays.asList(outputTables));
        }

        return tableNames;
    }

    /**
     *
     * @param tableNames
     *            - a comma separated string of table names
     * @param conf
     *            a configuration
     */
    public static void addOutputTables(String tableNames, Configuration conf) {
        String outputTables = conf.get(JOB_OUTPUT_TABLE_NAMES);
        if (null != outputTables) {
            outputTables = outputTables + "," + tableNames;
        } else {
            outputTables = tableNames;
        }
        conf.set(JOB_OUTPUT_TABLE_NAMES, outputTables);
    }

    /**
     * @param conf
     *            configuration file that contains data handler types and other information necessary for determining the set of tables required
     * @return true if a non-empty comma separated list of table names was properly set to conf's job table.names property
     */
    public static boolean registerTableNamesFromConfigFiles(Configuration conf) {
        Set<String> tables = extractTableNames(conf);

        if (tables.isEmpty()) {
            log.error("Configured tables for configured data types is empty");
            return false;
        }
        addOutputTables(org.apache.hadoop.util.StringUtils.join(",", tables), conf);
        return true;
    }

    /**
     * Extract the table names for the job as specified in DataTypeHandlers, Filters, and conf
     *
     * @param conf
     *            hadoop configuration
     * @return map of table names to priorities
     */
    public static Set<String> extractTableNames(Configuration conf) throws IllegalArgumentException {
        TypeRegistry.getInstance(conf);

        Set<String> tables = new HashSet<>();
        for (Type type : TypeRegistry.getTypes()) {
            if (type.getDefaultDataTypeHandlers() != null) {
                for (String handlerClassName : type.getDefaultDataTypeHandlers()) {
                    Class<? extends DataTypeHandler<?>> handlerClass;
                    try {
                        handlerClass = TypeRegistry.getHandlerClass(handlerClassName);
                    } catch (ClassNotFoundException e) {
                        throw new IllegalArgumentException("Unable to find " + handlerClassName, e);
                    }
                    DataTypeHandler<?> handler;
                    try {
                        handler = handlerClass.newInstance();
                    } catch (InstantiationException e) {
                        throw new IllegalArgumentException("Unable to instantiate " + handlerClassName, e);
                    } catch (IllegalAccessException e) {
                        throw new IllegalArgumentException("Unable to access default constructor for " + handlerClassName, e);
                    }
                    String[] handlerTableNames = handler.getTableNames(conf);
                    Collections.addAll(tables, handlerTableNames);
                }
            }
            if (type.getDefaultDataTypeFilters() != null) {
                for (String filterClassNames : type.getDefaultDataTypeFilters()) {
                    Class<? extends KeyValueFilter<?,?>> filterClass;
                    try {
                        filterClass = TypeRegistry.getFilterClass(filterClassNames);
                    } catch (ClassNotFoundException e) {
                        throw new IllegalArgumentException("Unable to find " + filterClassNames, e);
                    }
                    KeyValueFilter<?,?> filter;
                    try {
                        filter = filterClass.getDeclaredConstructor().newInstance();
                    } catch (InstantiationException | InvocationTargetException e) {
                        throw new IllegalArgumentException("Unable to instantiate " + filterClassNames, e);
                    } catch (IllegalAccessException | NoSuchMethodException e) {
                        throw new IllegalArgumentException("Unable to access default constructor for " + filterClassNames, e);
                    }
                    String[] filterTableNames = filter.getTableNames(conf);
                    Collections.addAll(tables, filterTableNames);
                }
            }
        }

        if (MetricsConfiguration.isEnabled(conf)) {
            String metricsTable = MetricsConfiguration.getTable(conf);
            if (org.apache.commons.lang.StringUtils.isNotBlank(metricsTable)) {
                tables.add(metricsTable);
            }
        }

        Set<String> extraTables = getTablesToAddToCache(conf);
        tables.addAll(extraTables);

        return tables;
    }

    public static Set<String> getTablesToAddToCache(Configuration conf) {
        Set<String> tables = new HashSet<>();
        String[] extraTables = conf.getStrings(TABLES_CONFIGS_TO_CACHE);
        if (null != extraTables) {
            for (String t : extraTables) {
                tables.add(t);
            }
        }
        return tables;

    }

    public static String[] getTablePropertiesToCache(Configuration conf) {
        String[] properties = conf.getStrings(TABLE_PROPERTIES_TO_CACHE);

        return properties;
    }

    /**
     * Configure the accumulo tables (create and set aggregators etc)
     *
     * @param conf
     *            the configuration
     * @throws AccumuloSecurityException
     *             if there is an issue with authentication
     * @throws AccumuloException
     *             if there is a general accumulo issue
     * @throws TableNotFoundException
     *             if the table could not be found
     * @return boolean of if the tables were configured
     */
    public boolean configureTables(Configuration conf) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        try (AccumuloClient client = accumuloHelper.newClient()) {
            // Check to see if the tables exist
            TableOperations tops = client.tableOperations();
            NamespaceOperations namespaceOperations = client.namespaceOperations();
            createAndConfigureTablesIfNecessary(getJobOutputTableNames(conf), tops, namespaceOperations, conf, log, false);
        }
        return true;
    }

    /**
     * Creates the tables that are needed to load data using this ingest job if they don't already exist. If a table is created, it is configured with the
     * appropriate iterators, aggregators, and locality groups that are required for ingest and query functionality to work correctly.
     *
     * @param tableNames
     *            the names of the table to create if they don't exist
     * @param tops
     *            accumulo table operations helper for checking/creating tables
     * @param conf
     *            the Hadoop {@link Configuration} for retrieving table configuration information
     * @param namespaceOperations
     *            the namespaces if needed for the table
     * @param log
     *            a logger for diagnostic messages
     * @param enableBloomFilters
     *            an indication of whether bloom filters should be enabled in the configuration
     * @throws AccumuloSecurityException
     *             if there is an issue with authentication
     * @throws AccumuloException
     *             if there is a general accumulo issue
     * @throws TableNotFoundException
     *             if the table could not be found
     */
    protected void createAndConfigureTablesIfNecessary(Set<String> tableNames, TableOperations tops, NamespaceOperations namespaceOperations,
                    Configuration conf, Logger log, boolean enableBloomFilters) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        for (String table : tableNames) {
            createNamespaceIfNecessary(namespaceOperations, table);
            // If the tables don't exist, then create them.
            try {
                if (!tops.exists(table)) {
                    tops.create(table);
                }
            } catch (TableExistsException te) {
                // in this case, somebody else must have created the table after our existence check
                log.info("Tried to create " + table + " but somebody beat us to the punch");
            }
        }

        // Pass along the enabling of bloom filters using the configuration
        conf.setBoolean(ShardTableConfigHelper.ENABLE_BLOOM_FILTERS, enableBloomFilters);

        configureTablesIfNecessary(tableNames, tops, conf, log);
    }

    private void createNamespaceIfNecessary(NamespaceOperations namespaceOperations, String table) throws AccumuloException, AccumuloSecurityException {
        // if the table has a namespace in it that doesn't already exist, create it
        if (table.contains(".")) {
            String namespace = table.split("\\.")[0];
            try {
                if (!namespaceOperations.exists(namespace)) {
                    namespaceOperations.create(namespace);
                }
            } catch (NamespaceExistsException e) {
                // in this case, somebody else must have created the namespace after our existence check
                log.info("Tried to create " + namespace + " but somebody beat us to the punch");
            }
        }
    }

    /**
     * Configures tables that are needed to load data using this ingest job, only if they don't already have the required configuration.
     *
     * @param tableNames
     *            the names of the tables to configure
     * @param tops
     *            accumulo table operations helper for configuring tables
     * @param conf
     *            the Hadoop {@link Configuration} for retrieving ingest table configuration information
     * @param log
     *            a {@link Logger} for diagnostic messages
     * @throws AccumuloSecurityException
     *             if there is an issue with authentication
     * @throws AccumuloException
     *             if there is a general accumulo issue
     * @throws TableNotFoundException
     *             if the table could not be found
     */
    private void configureTablesIfNecessary(Set<String> tableNames, TableOperations tops, Configuration conf, Logger log)
                    throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Map<String,TableConfigHelper> tableConfigs = setupTableConfigHelpers(log, conf, tableNames);

        for (String table : tableNames) {
            TableConfigHelper tableHelper = tableConfigs.get(table);
            if (tableHelper != null) {
                tableHelper.configure(tops);
            } else {
                log.info("No configuration supplied for table " + table);
            }
        }
    }

    /**
     * Instantiates TableConfigHelper classes for tables as defined in the configuration
     *
     * @param log
     *            a {@link Logger} for diagnostic messages
     * @param conf
     *            the Hadoop {@link Configuration} for retrieving ingest table configuration information
     * @param tableNames
     *            the names of the tables to configure
     * @return Map&lt;String,TableConfigHelper&gt; map from table names to their setup TableConfigHelper classes
     */
    private Map<String,TableConfigHelper> setupTableConfigHelpers(Logger log, Configuration conf, Set<String> tableNames) {

        Map<String,TableConfigHelper> helperMap = new HashMap<>(tableNames.size());

        for (String table : tableNames) {
            helperMap.put(table, TableConfigHelperFactory.create(table, conf, log));
        }

        return helperMap;
    }

    /**
     * Get the table priorities
     *
     * @param conf
     *            hadoop configuration
     * @return map of table names to priorities
     */
    public static Map<String,Integer> getTablePriorities(Configuration conf) {
        TypeRegistry.getInstance(conf);
        Map<String,Integer> tablePriorities = new HashMap<>();
        for (Type type : TypeRegistry.getTypes()) {
            if (null != type.getDefaultDataTypeHandlers()) {
                for (String handlerClassName : type.getDefaultDataTypeHandlers()) {
                    Class<? extends DataTypeHandler<?>> handlerClass;
                    try {
                        handlerClass = TypeRegistry.getHandlerClass(handlerClassName);
                    } catch (ClassNotFoundException e) {
                        throw new IllegalArgumentException("Unable to find " + handlerClassName, e);
                    }
                    DataTypeHandler<?> handler;
                    try {
                        handler = handlerClass.newInstance();
                    } catch (InstantiationException e) {
                        throw new IllegalArgumentException("Unable to instantiate " + handlerClassName, e);
                    } catch (IllegalAccessException e) {
                        throw new IllegalArgumentException("Unable to access default constructor for " + handlerClassName, e);
                    }
                    String[] handlerTableNames = handler.getTableNames(conf);
                    int[] handlerTablePriorities = handler.getTableLoaderPriorities(conf);
                    for (int i = 0; i < handlerTableNames.length; i++) {
                        tablePriorities.put(handlerTableNames[i], handlerTablePriorities[i]);
                    }
                }
            }
            if (null != type.getDefaultDataTypeFilters()) {
                for (String filterClassNames : type.getDefaultDataTypeFilters()) {
                    Class<? extends KeyValueFilter<?,?>> filterClass;
                    try {
                        filterClass = TypeRegistry.getFilterClass(filterClassNames);
                    } catch (ClassNotFoundException e) {
                        throw new IllegalArgumentException("Unable to find " + filterClassNames, e);
                    }
                    KeyValueFilter<?,?> filter;
                    try {
                        filter = filterClass.getDeclaredConstructor().newInstance();
                    } catch (InstantiationException e) {
                        throw new IllegalArgumentException("Unable to instantiate " + filterClassNames, e);
                    } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                        throw new IllegalArgumentException("Unable to access default constructor for " + filterClassNames, e);
                    }
                    String[] filterTableNames = filter.getTableNames(conf);
                    int[] filterTablePriorities = filter.getTableLoaderPriorities(conf);
                    for (int i = 0; i < filterTableNames.length; i++) {
                        tablePriorities.put(filterTableNames[i], filterTablePriorities[i]);
                    }
                }
            }
        }

        if (MetricsConfiguration.isEnabled(conf)) {
            String metricsTable = MetricsConfiguration.getTable(conf);
            int priority = MetricsConfiguration.getTablePriority(conf);
            if (org.apache.commons.lang.StringUtils.isNotBlank(metricsTable)) {
                tablePriorities.put(metricsTable, priority);
            }
        }

        return tablePriorities;
    }

    /**
     * Populate the table configuration cache directly from accumulo or from the cached properties file, as configured
     *
     * @param conf
     *            the Hadoop configuration
     * @throws IOException
     *             if there is an issue with read or write
     *
     */
    void setupTableConfigurationsCache(Configuration conf) throws IOException {

        if (conf.getBoolean(TableConfigCache.ACCUMULO_CONFIG_FILE_CACHE_ENABLE_PROPERTY, false)) {

            try {
                setTableConfigsFromCacheFile();
                usingFileCache = true;
                return;
            } catch (Exception e) {
                log.error("Unable to read accumulo config cache at " + tableConfigCache.getCacheFilePath() + "\n " + e.getCause()
                                + ". Proceeding to read directly from Accumulo.");
            }
        }
        try {
            setTableConfigsFromAccumulo(conf);
            usingFileCache = false;
        } catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
            log.error("Unable to read desired table properties from accumulo.  Please verify your configuration. ");
            throw new IOException(e);
        }

    }

    private void setTableConfigsFromCacheFile() throws IOException {
        tableConfigCache.read();

    }

    private void setTableConfigsFromAccumulo(Configuration conf) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        tableConfigCache.setTableConfigs(getTablePropertiesFromAccumulo(accumuloHelper, log, conf));

    }

    private void setTableConfigsFromConf(Configuration conf) throws IOException {
        tableConfigCache.setTableConfigs(deserializeTableConfigs(conf));
    }

    private static Map<String,Map<String,String>> getTablePropertiesFromAccumulo(AccumuloHelper accumuloHelper, Logger log, Configuration conf)
                    throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        String propertyRegex = null;
        Pattern pattern = null;
        Map<String,Map<String,String>> configMap = new HashMap<>();

        try (AccumuloClient client = accumuloHelper.newClient()) {
            TableOperations tops = client.tableOperations();

            String[] propertiesToCache = getTablePropertiesToCache(conf);
            Set<String> tableNames = getTablesToAddToCache(conf);

            if (null != propertiesToCache && propertiesToCache.length > 0) {
                propertyRegex = String.join("|", propertiesToCache);
                pattern = Pattern.compile(propertyRegex);
            }

            // if no table names are specified, assume we should cache properties for all of them
            if (null == tableNames || tableNames.isEmpty()) {
                tableNames = tops.tableIdMap().keySet();
            }

            for (String table : tableNames) {
                Map<String,String> tempmap = new HashMap<>();
                Iterator it = tops.getProperties(table).iterator();

                while (it.hasNext()) {
                    Map.Entry<String,String> entry = (Map.Entry) it.next();
                    if (null != entry.getValue() && !entry.getValue().isEmpty()) {
                        if (null != pattern) {
                            Matcher m = pattern.matcher(entry.getKey());
                            if (m.find()) {
                                tempmap.put(entry.getKey(), entry.getValue().replaceAll("\n|\r", ""));
                            }
                        }
                        // if we haven't limited the properties we want to cache, add them all
                        else {
                            tempmap.put(entry.getKey(), entry.getValue().replaceAll("\n|\r", ""));
                        }
                    }

                }
                configMap.put(table, tempmap);
            }
        }

        return configMap;
    }

    private void setTableCombiners(String tableName, Map<Integer,Map<String,String>> list) {
        this.combiners.put(tableName, list);
    }

    private void setTableAggregators(String tableName, Map<Integer,Map<String,String>> list) {
        this.aggregators.put(tableName, list);
    }

    public Map<Integer,Map<String,String>> getTableCombiners(String tableName) {
        return this.combiners.get(tableName);
    }

    public Map<Integer,Map<String,String>> getTableAggregators(String tableName) {
        return this.aggregators.get(tableName);
    }

    public boolean isUsingFileCache() {
        return usingFileCache;
    }

    public void setTableItersPrioritiesAndOpts() throws IOException {
        if (!tableConfigCache.isInitialized()) {
            try {
                setTableConfigsFromConf(conf);
            } catch (IOException e) {
                setupTableConfigurationsCache(conf);
            }
        }

        // This used to be set arbitrarily to scan scope. We have since decided it is more appropriate to use the minc scope. Some unsuspecting dev might think
        // that setting a scan time aggregator would have no effect on data being written into the database, but ingest was being affected in this somewhat
        // covert manner. We feel this new approach to be much less accident-prone.
        IteratorUtil.IteratorScope scope = IteratorUtil.IteratorScope.minc;

        // Go through all of the configuration properties of this table and figure out which
        // properties represent iterator configuration. For those that do, store the iterator
        // setup and options in a map so that we can group together all of the options for each
        // iterator.

        Set<String> configuredTables = getJobOutputTableNames(conf);

        if (!configuredTables.isEmpty()) {
            for (String table : configuredTables) {
                Map<Integer,Map<String,String>> aggregatorMap = new HashMap<>();
                Map<Integer,Map<String,String>> combinerMap = new HashMap<>();
                HashMap<String,Map<String,String>> allOptions = new HashMap<>();
                ArrayList<IteratorSetting> iters = new ArrayList<>();

                Map<String,String> tableProps = tableConfigCache.getTableProperties(table);
                if (null == tableProps || tableProps.isEmpty()) {
                    log.warn("No table properties found for " + table);
                    continue;
                }
                for (Map.Entry<String,String> entry : tableProps.entrySet()) {

                    if (entry.getKey().startsWith(Property.TABLE_ITERATOR_PREFIX.getKey())) {

                        String suffix = entry.getKey().substring(Property.TABLE_ITERATOR_PREFIX.getKey().length());
                        String suffixSplit[] = suffix.split("\\.", 4);

                        if (!suffixSplit[0].equals(scope.name())) {
                            continue;
                        }

                        if (suffixSplit.length == 2) {
                            // get the Iterator priority and class
                            String sa[] = entry.getValue().split(",");
                            int prio = Integer.parseInt(sa[0]);
                            String className = sa[1];
                            iters.add(new IteratorSetting(prio, suffixSplit[1], className));

                        } else if (suffixSplit.length == 4 && suffixSplit[2].equals("opt")) {
                            // get the iterator options
                            String iterName = suffixSplit[1];
                            String optName = suffixSplit[3];

                            Map<String,String> options = allOptions.get(iterName);
                            if (options == null) {
                                options = new HashMap<>();
                                allOptions.put(iterName, options);
                            }

                            options.put(optName, entry.getValue());

                        } else {
                            log.warn("Unrecognizable option: " + entry.getKey());
                        }
                    }
                }

                // Now go through all of the iterators, and for those that are aggregators, store
                try {
                    for (IteratorSetting iter : iters) {

                        Class<?> klass = Class.forName(iter.getIteratorClass());
                        if (PropogatingIterator.class.isAssignableFrom(klass)) {
                            Map<String,String> options = allOptions.get(iter.getName());
                            if (null != options) {
                                aggregatorMap.put(iter.getPriority(), options);
                            } else
                                log.trace("Skipping iterator class " + iter.getIteratorClass() + " since it doesn't have options.");

                        }
                        if (Combiner.class.isAssignableFrom(klass)) {
                            Map<String,String> options = allOptions.get(iter.getName());
                            if (null != options) {
                                options.put(ITERATOR_CLASS_MARKER, iter.getIteratorClass());
                                combinerMap.put(iter.getPriority(), options);
                            } else {
                                log.trace("Skipping iterator class " + iter.getIteratorClass() + " since it doesn't have options.");
                            }
                        } else {
                            log.trace("Skipping iterator class " + iter.getIteratorClass() + " since it doesn't appear to be a combiner.");

                        }
                    }

                    if (!aggregatorMap.isEmpty()) {
                        setTableAggregators(table, aggregatorMap);
                    }
                    if (!combinerMap.isEmpty()) {
                        setTableCombiners(table, combinerMap);
                    }

                } catch (ClassNotFoundException e) {
                    throw new IOException("Unable to configure iterators for " + table, e);
                }
            }
        } else {
            log.warn("No output tables configured.");
        }

    }

    public Map<String,String> getTableProperties(String tableName) throws IOException {
        if (!tableConfigCache.isInitialized()) {
            try {
                setTableConfigsFromConf(conf);
            } catch (IOException e) {
                setupTableConfigurationsCache(conf);
            }
        }
        return tableConfigCache.getTableProperties(tableName);

    }

    public Map<String,Set<Text>> getLocalityGroups(String tableName) throws IOException {

        String prefix = Property.TABLE_LOCALITY_GROUP_PREFIX.getKey();
        Map<String,Set<Text>> groupsNfams = new HashMap<>();
        for (Map.Entry<String,String> entry : tableConfigCache.getTableProperties(tableName).entrySet()) {
            if (entry.getKey().startsWith(prefix)) {

                String group = entry.getKey().substring(prefix.length());
                String[] parts = group.split("\\.");
                String[] famStr = entry.getValue().split(",");
                Set<Text> colFams = new HashSet<>();
                for (String fam : famStr) {
                    colFams.add(new Text(fam));

                }
                groupsNfams.put(parts[0], colFams);

            }

        }
        return groupsNfams;

    }

    public void serializeTableConfgurationIntoConf(Configuration conf) throws IOException {

        Set<String> tables = getJobOutputTableNames(conf);
        setupTableConfigurationsCache(conf);

        if (null == tables || tables.isEmpty()) {
            log.warn("No output tables configured for job");
        } else {
            for (String table : tables) {
                Map<String,String> tableConfig = TableConfigCache.getCurrentCache(conf).getTableProperties(table);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(tableConfig);
                oos.close();
                String encodedTableConfig = Base64.getEncoder().encodeToString(baos.toByteArray());
                conf.set(table + TABLE_CONFIGURATION_PROPERTY, encodedTableConfig);
            }
        }
    }

    public static Map<String,Map<String,String>> deserializeTableConfigs(Configuration conf) throws IOException {
        Long start = System.currentTimeMillis();
        Set<String> tables = getJobOutputTableNames(conf);
        Map<String,Map<String,String>> tableConfigMap = new HashMap<>();
        if (null == tables || tables.isEmpty()) {
            log.warn("No output tables configured for job");
        } else {
            for (String tableName : tables) {
                String tableConfigString = conf.get(tableName + TABLE_CONFIGURATION_PROPERTY);
                Map<String,String> tableConf = null;

                if (null != tableConfigString) {
                    try {
                        byte[] data = Base64.getDecoder().decode(tableConfigString);
                        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
                        Object o = ois.readObject();
                        ois.close();
                        tableConf = (HashMap<String,String>) o;
                        tableConfigMap.put(tableName, tableConf);
                    } catch (Exception e) {
                        log.error("Unable to deserialize configuration for table " + tableName);
                        throw new IOException(e);
                    }

                } else {
                    throw new IOException("Job has requested a table which was not configured: " + tableName);
                }
            }
        }
        Long end = System.currentTimeMillis();

        log.info("Time deserializing table configs:" + (end - start) + "ms");
        return tableConfigMap;
    }

    public void updateCacheFile() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        setTableConfigsFromAccumulo(conf);
        this.tableConfigCache.update();
    }
}
