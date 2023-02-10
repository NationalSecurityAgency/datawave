package datawave.ingest.mapreduce.job;

import datawave.ingest.mapreduce.partition.DelegatePartitioner;
import datawave.ingest.mapreduce.partition.MultiTableRangePartitioner;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles the creation of partitioner instances to be used by the DelegatingPartitioner.
 */
public class PartitionerCache {
    protected static final Logger log = Logger.getLogger(DelegatingPartitioner.class);
    static final String PREFIX_DEDICATED_PARTITIONER = "partitioner.dedicated.";
    static final String PREFIX_SHARED_MEMBERSHIP = "partitioner.category.member.";
    static final String PREFIX_CATEGORY_PARTITIONER = "partitioner.category.";
    static final String DEFAULT_DELEGATE_PARTITIONER = "partitioner.default.delegate";
    
    private final Configuration conf;
    private Partitioner<BulkIngestKey,Value> defaultDelegatePartitioner = null;
    // configuredPartitionerCache contains:
    // table name -> partitioner instance
    // category name -> partitioner instance
    // so category names must not collide with table names
    // multiple keys point to the same partitioner instance for categories
    private final Map<Text,Partitioner<BulkIngestKey,Value>> configuredPartitionerCache;
    
    public PartitionerCache(Configuration conf) {
        this.conf = conf;
        this.configuredPartitionerCache = new HashMap<>();
    }
    
    /**
     * Filters a list of table names, returning only ones with valid partitioners.
     * 
     * @param tableNames
     *            an array of table names, it's expected to include non-configured table names
     * @param job
     *            the job
     * @return only the table names that were configured with valid partitioners.
     */
    public List<String> validatePartitioners(String[] tableNames, Job job) {
        ArrayList<String> validTableNames = new ArrayList<>();
        for (String tableName : tableNames) {
            if (hasPartitionerOverride(new Text(tableName))) {
                try {
                    Partitioner<BulkIngestKey,Value> partitionerForTable = cachePartitioner(new Text(tableName));
                    initializeJob(job, partitionerForTable);
                    validTableNames.add(tableName);
                } catch (Exception e) {
                    log.warn("Unable to create the partitioner for " + tableName + " despite its configuration."
                                    + "Will use the default partitioner for this table.", e);
                    lazyInitializeDefaultPartitioner(job);
                }
            } else {
                lazyInitializeDefaultPartitioner(job);
            }
        }
        return validTableNames;
    }
    
    private void initializeJob(Job job, Partitioner<BulkIngestKey,Value> partitionerForTable) {
        if (partitionerForTable instanceof DelegatePartitioner) {
            ((DelegatePartitioner) partitionerForTable).initializeJob(job);
        }
    }
    
    private void lazyInitializeDefaultPartitioner(Job job) {
        // if this fails, the job should fail
        if (null == defaultDelegatePartitioner) {
            defaultDelegatePartitioner = getDefaultPartitioner();
            log.debug("Configuring default delegate partitioner with job");
            initializeJob(job, defaultDelegatePartitioner);
            log.debug("Default delegate partitioner set up.");
        }
    }
    
    /**
     * Instantiates each of the partitioners that were requested and puts them into the cache for future retrieval
     * 
     * @param tableNames
     *            list of table names, each should be configured to point to a valid partitioner
     * @throws ClassNotFoundException
     *             when the configured partitioner could not be created.
     */
    public void createAndCachePartitioners(List<Text> tableNames) throws ClassNotFoundException {
        for (Text tableName : tableNames) {
            cachePartitioner(tableName);
        }
    }
    
    /**
     * @param tableName
     *            the table name
     * @return the cached partitioner for this table name (which may be a dedicated or shared partitioner)
     */
    public Partitioner<BulkIngestKey,Value> getPartitioner(Text tableName) {
        if (log.isDebugEnabled())
            log.debug("Looking up partitioner for " + tableName);
        
        Partitioner<BulkIngestKey,Value> cachedPartitioner = configuredPartitionerCache.get(tableName);
        if (null != cachedPartitioner) {
            if (log.isTraceEnabled()) {
                log.trace("Found partitioner in cache for table " + tableName + ": " + cachedPartitioner.getClass().getName());
            }
            return cachedPartitioner;
        } else {
            return getDefaultPartitioner();
        }
    }
    
    /**
     * @param conf
     *            the configuration
     * @param tableName
     *            the table name
     * @return category name to which this table belongs or null if the table name does not belong a table
     */
    public static Text getCategory(Configuration conf, Text tableName) {
        String categoryName = conf.get(PREFIX_SHARED_MEMBERSHIP + tableName);
        return categoryName == null ? null : new Text(categoryName);
    }
    
    /**
     * Lazily initializes the default delegate partitioner
     * 
     * @return the cached instance
     */
    private Partitioner<BulkIngestKey,Value> getDefaultPartitioner() {
        if (defaultDelegatePartitioner == null) {
            Class<? extends Partitioner<BulkIngestKey,Value>> clazz = getPartitionerClass(DEFAULT_DELEGATE_PARTITIONER, MultiTableRangePartitioner.class,
                            Partitioner.class);
            defaultDelegatePartitioner = createConfiguredPartitioner(clazz, null);
            log.info("Created default Partitioner: " + clazz.getName());
        }
        return defaultDelegatePartitioner;
    }
    
    private boolean hasPartitionerOverride(Text tableName) {
        return isMemberOfACategory(tableName) || hasDedicatedPartitioner(tableName);
    }
    
    /**
     * returns the classname for this table's partitioner, either dedicated or shared
     * 
     * @param conf
     *            the configuration
     * @param tableName
     *            the table name
     * @return the class name of the partitioner
     */
    private static String getPartitionerClassNameForTableName(Configuration conf, String tableName) {
        String partitionerClassName;
        String categoryName = conf.get(PREFIX_SHARED_MEMBERSHIP + tableName);
        if (null != categoryName) {
            log.info("Found partitioner category for " + tableName + ": " + categoryName);
            partitionerClassName = conf.get(PREFIX_CATEGORY_PARTITIONER + categoryName);
        } else {
            partitionerClassName = conf.get(PREFIX_DEDICATED_PARTITIONER + tableName);
        }
        return partitionerClassName;
    }
    
    // creates an instance of the partitioner for this table name
    private Partitioner<BulkIngestKey,Value> cachePartitioner(Text tableName) throws ClassNotFoundException {
        if (isMemberOfACategory(tableName)) {
            return updateCacheForCategoryMember(tableName, getCategory(conf, tableName));
        } else if (hasDedicatedPartitioner(tableName)) {
            return updateCacheForDedicatedPartitioner(tableName);
        } else {
            throw new IllegalStateException(tableName + " is not configured properly for a partitioner.  " + "It shouldn't have made it into the list at all.");
        }
    }
    
    private boolean isMemberOfACategory(Text tableName) {
        return null != getCategory(conf, tableName);
    }
    
    private Partitioner<BulkIngestKey,Value> updateCacheForCategoryMember(Text tableName, Text categoryName) throws ClassNotFoundException {
        Partitioner<BulkIngestKey,Value> partitionerForCategory = configuredPartitionerCache.get(new Text(categoryName));
        if (null != partitionerForCategory) {
            addPartitionerForTableIfMissing(tableName, partitionerForCategory);
        } else {
            partitionerForCategory = cachePartitionerForCategoryAndTable(tableName, categoryName);
        }
        return partitionerForCategory;
    }
    
    private void addPartitionerForTableIfMissing(Text tableName, Partitioner<BulkIngestKey,Value> partitionerForCategory) {
        if (null == configuredPartitionerCache.get(new Text(tableName))) {
            addToCache(tableName, partitionerForCategory);
        }
    }
    
    private Partitioner<BulkIngestKey,Value> cachePartitionerForCategoryAndTable(Text tableName, Text categoryName) throws ClassNotFoundException {
        Partitioner<BulkIngestKey,Value> partitionerForCategory;
        partitionerForCategory = getConfiguredPartitioner(PREFIX_CATEGORY_PARTITIONER, categoryName.toString());
        addToCache(categoryName, partitionerForCategory);
        addToCache(tableName, partitionerForCategory);
        return partitionerForCategory;
    }
    
    private boolean hasDedicatedPartitioner(Text tableName) {
        return null != conf.get(PREFIX_DEDICATED_PARTITIONER + tableName);
    }
    
    private Partitioner<BulkIngestKey,Value> updateCacheForDedicatedPartitioner(Text tableName) throws ClassNotFoundException {
        Partitioner<BulkIngestKey,Value> partitioner = getConfiguredPartitioner(PREFIX_DEDICATED_PARTITIONER, tableName.toString());
        addToCache(tableName, partitioner);
        return partitioner;
    }
    
    private void addToCache(Text identifier, Partitioner<BulkIngestKey,Value> partitioner) {
        log.info("Partitioner registered for " + identifier + " : " + partitioner.getClass().getName());
        configuredPartitionerCache.put(new Text(identifier), partitioner);
    }
    
    // gets the partitioner class with a default class
    private Class<? extends Partitioner<BulkIngestKey,Value>> getPartitionerClass(String propertyName,
                    Class<? extends Partitioner<BulkIngestKey,Value>> defaultClass, Class xface) {
        return conf.getClass(propertyName, defaultClass, xface);
    }
    
    // gets the partitioner class without using a default class
    private Class<? extends Partitioner<BulkIngestKey,Value>> getPartitionerClass(String propertyName) throws ClassNotFoundException {
        return (Class<? extends Partitioner<BulkIngestKey,Value>>) conf.getClassByName(conf.get(propertyName));
    }
    
    // creates an instance of the specified class and configures it both for the generic case and if it supports by table configuration, also configures for
    // that
    private Partitioner<BulkIngestKey,Value> createConfiguredPartitioner(Class<? extends Partitioner<BulkIngestKey,Value>> clazz, String prefix) {
        try {
            Partitioner<BulkIngestKey,Value> partitioner = clazz.newInstance();
            if (partitioner instanceof Configurable) {
                ((Configurable) partitioner).setConf(conf);
            }
            // If this supports by-table configurations, attempt to use it
            if (prefix != null && partitioner instanceof DelegatePartitioner) {
                ((DelegatePartitioner) partitioner).configureWithPrefix(prefix);
            }
            return partitioner;
        } catch (Exception e) {
            throw new RuntimeException("Unable to instantiate delegate partitioner class: " + e.getMessage(), e);
        }
    }
    
    private Partitioner<BulkIngestKey,Value> getConfiguredPartitioner(String prefixCategoryPartitioner, String identifier) throws ClassNotFoundException {
        Class<? extends Partitioner<BulkIngestKey,Value>> partitionerClassForTable = getPartitionerClass(prefixCategoryPartitioner + identifier);
        
        if (log.isDebugEnabled())
            log.debug("Found partitioner for " + prefixCategoryPartitioner + identifier + ": " + partitionerClassForTable);
        return createConfiguredPartitioner(partitionerClassForTable, identifier);
    }
}
