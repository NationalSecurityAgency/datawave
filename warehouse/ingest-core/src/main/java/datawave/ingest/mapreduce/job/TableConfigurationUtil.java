package datawave.ingest.mapreduce.job;

import datawave.ingest.config.TableConfigCache;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.filter.KeyValueFilter;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.job.metrics.MetricsConfiguration;
import datawave.ingest.mapreduce.job.reduce.AggregatingReducer;
import datawave.ingest.table.config.ShardTableConfigHelper;
import datawave.ingest.table.config.TableConfigHelper;
import datawave.iterators.PropogatingIterator;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NamespaceOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TableConfigurationUtil {
    
    protected static final Logger log = Logger.getLogger(TableConfigurationUtil.class.getName());
    private String[] tableNames;
    private AccumuloHelper accumuloHelper;
    
    public TableConfigurationUtil(Configuration conf) {
        registerTableNames(conf);
        accumuloHelper = new AccumuloHelper();
        accumuloHelper.setup(conf);
    }
    
    public String[] getTableNames() {
        return tableNames;
    }
    
    /**
     * @param conf
     *            configuration file that contains data handler types and other information necessary for determining the set of tables required
     * @return true if a non-empty comma separated list of table names was properly set to conf's job table.names property
     */
    private boolean registerTableNames(Configuration conf) {
        Set<String> tables = getTables(conf);
        
        if (tables.isEmpty()) {
            log.error("Configured tables for configured data types is empty");
            return false;
        }
        tableNames = tables.toArray(new String[tables.size()]);
        conf.set("job.table.names", org.apache.hadoop.util.StringUtils.join(",", tableNames));
        return true;
    }
    
    /**
     * Get the table names
     *
     * @param conf
     *            hadoop configuration
     * @return map of table names to priorities
     */
    public static Set<String> getTables(Configuration conf) throws IllegalArgumentException {
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
                        filter = filterClass.newInstance();
                    } catch (InstantiationException e) {
                        throw new IllegalArgumentException("Unable to instantiate " + filterClassNames, e);
                    } catch (IllegalAccessException e) {
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
        
        return tables;
    }
    
    /**
     * Configure the accumulo tables (create and set aggregators etc)
     *
     * @param conf
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    public boolean configureTables(Configuration conf) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        try (AccumuloClient client = accumuloHelper.newClient()) {
            // Check to see if the tables exist
            TableOperations tops = client.tableOperations();
            NamespaceOperations namespaceOperations = client.namespaceOperations();
            createAndConfigureTablesIfNecessary(tableNames, tops, namespaceOperations, conf, log, false);
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
     * @param log
     *            a logger for diagnostic messages
     * @param enableBloomFilters
     *            an indication of whether bloom filters should be enabled in the configuration
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    protected void createAndConfigureTablesIfNecessary(String[] tableNames, TableOperations tops, NamespaceOperations namespaceOperations, Configuration conf,
                    Logger log, boolean enableBloomFilters) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
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
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    private void configureTablesIfNecessary(String[] tableNames, TableOperations tops, Configuration conf, Logger log) throws AccumuloSecurityException,
                    AccumuloException, TableNotFoundException {
        
        Map<String,TableConfigHelper> tableConfigs = getTableConfigs(log, conf, tableNames);
        
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
    private Map<String,TableConfigHelper> getTableConfigs(Logger log, Configuration conf, String[] tableNames) {
        
        Map<String,TableConfigHelper> helperMap = new HashMap<>(tableNames.length);
        
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
                        filter = filterClass.newInstance();
                    } catch (InstantiationException e) {
                        throw new IllegalArgumentException("Unable to instantiate " + filterClassNames, e);
                    } catch (IllegalAccessException e) {
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
     * Looks up aggregator configuration for all of the tables in {@code tableNames} and serializes the configuration into {@code conf}, so that it is available
     * for retrieval and use in mappers or reducers. Currently, this is used in {@link AggregatingReducer} and its subclasses to aggregate output key/value
     * pairs rather than making accumulo do it at scan or major compaction time on the resulting rfile.
     *
     * @param accumuloHelper
     *            for accessing tableOperations
     * @param conf
     *            the Hadoop configuration into which serialized aggregator configuration is placed
     * @param log
     *            a logger for sending diagnostic information
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     * @throws ClassNotFoundException
     */
    void serializeAggregatorConfiguration(AccumuloHelper accumuloHelper, Configuration conf, Logger log) throws AccumuloException, ClassNotFoundException,
                    TableNotFoundException, AccumuloSecurityException {
        
        if (conf.getBoolean(TableConfigCache.ACCUMULO_CONFIG_CACHE_ENABLE_PROPERTY, false)) {
            TableConfigCache cache = new TableConfigCache(conf);
            try {
                cache.read();
                return;
            } catch (Exception e) {
                log.error("Unable to read accumulo config cache at " + cache.getCacheFilePath() + ". Proceeding to read directly from Accumulo.");
            }
        }
        Map<String,String> configMap = getTableAggregatorConfigs(accumuloHelper, log, tableNames);
        for (Map.Entry entry : configMap.entrySet()) {
            conf.set(entry.getKey().toString(), entry.getValue().toString());
        }
        
    }
    
    public Map<String,String> getTableAggregatorConfigs() throws AccumuloException, ClassNotFoundException, TableNotFoundException, AccumuloSecurityException {
        return getTableAggregatorConfigs(accumuloHelper, log, tableNames);
    }
    
    private static Map<String,String> getTableAggregatorConfigs(AccumuloHelper accumuloHelper, Logger log, String[] tableNames)
                    throws AccumuloSecurityException, AccumuloException, TableNotFoundException, ClassNotFoundException {
        
        Map<String,String> configMap = new HashMap<>();
        try (AccumuloClient client = accumuloHelper.newClient()) {
            TableOperations tops = client.tableOperations();
            
            // We're arbitrarily choosing the scan scope for gathering aggregator information.
            // For the aggregators configured in this job, that's ok since they are added to all
            // scopes. If someone manually added another aggregator and didn't apply it to scan
            // time, then we wouldn't pick that up here, but the chances of that are very small
            // since any aggregation we care about in the reducer doesn't make sense unless the
            // aggregator is a scan aggregator.
            IteratorUtil.IteratorScope scope = IteratorUtil.IteratorScope.scan;
            for (String table : tableNames) {
                ArrayList<IteratorSetting> iters = new ArrayList<>();
                HashMap<String,Map<String,String>> allOptions = new HashMap<>();
                
                // Go through all of the configuration properties of this table and figure out which
                // properties represent iterator configuration. For those that do, store the iterator
                // setup and options in a map so that we can group together all of the options for each
                // iterator.
                for (Map.Entry<String,String> entry : tops.getProperties(table)) {
                    
                    if (entry.getKey().startsWith(Property.TABLE_ITERATOR_PREFIX.getKey())) {
                        
                        String suffix = entry.getKey().substring(Property.TABLE_ITERATOR_PREFIX.getKey().length());
                        String suffixSplit[] = suffix.split("\\.", 4);
                        
                        if (!suffixSplit[0].equals(scope.name())) {
                            continue;
                        }
                        
                        if (suffixSplit.length == 2) {
                            String sa[] = entry.getValue().split(",");
                            int prio = Integer.parseInt(sa[0]);
                            String className = sa[1];
                            iters.add(new IteratorSetting(prio, suffixSplit[1], className));
                        } else if (suffixSplit.length == 4 && suffixSplit[2].equals("opt")) {
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
                // the options in the Hadoop config so that we can parse it back out in the reducer.
                for (IteratorSetting iter : iters) {
                    Class<?> klass = Class.forName(iter.getIteratorClass());
                    if (PropogatingIterator.class.isAssignableFrom(klass)) {
                        Map<String,String> options = allOptions.get(iter.getName());
                        if (null != options) {
                            for (Map.Entry<String,String> option : options.entrySet()) {
                                String key = String.format("aggregator.%s.%d.%s", table, iter.getPriority(), option.getKey());
                                configMap.put(key, option.getValue());
                            }
                        } else
                            log.trace("Skipping iterator class " + iter.getIteratorClass() + " since it doesn't have options.");
                        
                    } else {
                        log.trace("Skipping iterator class " + iter.getIteratorClass() + " since it doesn't appear to be a combiner.");
                    }
                }
                
                for (IteratorSetting iter : iters) {
                    Class<?> klass = Class.forName(iter.getIteratorClass());
                    if (Combiner.class.isAssignableFrom(klass)) {
                        Map<String,String> options = allOptions.get(iter.getName());
                        if (null != options) {
                            String key = String.format("combiner.%s.%d.iterClazz", table, iter.getPriority());
                            configMap.put(key, iter.getIteratorClass());
                            for (Map.Entry<String,String> option : options.entrySet()) {
                                key = String.format("combiner.%s.%d.%s", table, iter.getPriority(), option.getKey());
                                configMap.put(key, option.getValue());
                            }
                        } else
                            log.trace("Skipping iterator class " + iter.getIteratorClass() + " since it doesn't have options.");
                    }
                }
            }
        }
        return configMap;
    }
}
