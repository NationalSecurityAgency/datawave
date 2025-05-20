package datawave.ingest.mapreduce.job;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

/**
 * This partitioner delegates the partitioning logic to other partitioners based on table name. * The table may have its own dedicated partitioner or may share
 * a partitioner with other tables. * Note that the same partitioner class can have multiple instances, and each can be configured differently for each table.
 * e.g. tableA uses LimitedKeyPartitioner with max of 5 partitions, tableB uses LimitedKeyPartitioner with a max of 7 partitions. See DelegatePartitioner's
 * configureWithPrefix * To avoid hotspotting for tables that output to a subset of the available partitions, this will offset partitions. e.g., two
 * partitioners each limit their output to 10 partitioners. The first will go to 0-9 and the other to 10-19. See DelegatePartitioner's getNumPartitions
 */
public class DelegatingPartitioner extends Partitioner<BulkIngestKey,Value> implements Configurable {
    protected static final Logger log = Logger.getLogger(DelegatingPartitioner.class);

    // this gets populated with the table names that have non-default partitioners defined
    static final String TABLE_NAMES_WITH_CUSTOM_PARTITIONERS = "DelegatingPartitioner.custom.delegate._tablenames";
    //
    // append your table name to create this property name. property value is the partitioner class name
    static final String PREFIX_DEDICATED_PARTITIONER = PartitionerCache.PREFIX_DEDICATED_PARTITIONER;;
    // append your table name to create this property name. property value is the category name
    static final String PREFIX_SHARED_MEMBERSHIP = PartitionerCache.PREFIX_SHARED_MEMBERSHIP;
    // append your category name to create this property name. property value is the partitioner class name
    static final String PREFIX_CATEGORY_PARTITIONER = PartitionerCache.PREFIX_CATEGORY_PARTITIONER; // add the category here
    // exactly one partitioner class
    static final String DEFAULT_DELEGATE_PARTITIONER = PartitionerCache.DEFAULT_DELEGATE_PARTITIONER;

    private Configuration conf;
    private Map<Text,Integer> tableOffsets; // mapping from table name to its offset (see DelegatePartitioner)
    private PartitionerCache partitionerCache;

    public DelegatingPartitioner() {}

    public DelegatingPartitioner(Configuration configuration) {
        this.conf = configuration;
    }

    // adds configuration properties
    public static void configurePartitioner(Job job, Configuration conf, String[] tableNames) {
        PartitionerCache partitionerCache = new PartitionerCache(conf);
        validateAndRegisterPartitioners(job, conf, tableNames, partitionerCache); // fault tolerant for misconfigured tables
        job.setPartitionerClass(DelegatingPartitioner.class);
    }

    /**
     * Is fault tolerant: if a given table is misconfigured, it will not honor its override DelegatingPartitioner and log a warning
     *
     * @param job
     *            the job
     * @param conf
     *            the configuration
     * @param tableNames
     *            the table names
     * @param partitionerCache
     *            the partitioner cache
     */
    private static void validateAndRegisterPartitioners(Job job, Configuration conf, String[] tableNames, PartitionerCache partitionerCache) {
        String commaSeparatedTableNames = StringUtils.join(",", partitionerCache.validatePartitioners(tableNames, job));
        if (!commaSeparatedTableNames.isEmpty()) {
            conf.set(TABLE_NAMES_WITH_CUSTOM_PARTITIONERS, commaSeparatedTableNames);
        }
    }

    @Override
    // delegates partitioning
    public int getPartition(BulkIngestKey key, Value value, int numPartitions) {
        Text tableName = key.getTableName();

        Partitioner<BulkIngestKey,Value> partitioner = partitionerCache.getPartitioner(tableName);

        int partition = partitioner.getPartition(key, value, numPartitions);
        Integer offset = this.tableOffsets.get(tableName);

        if (null != offset) {
            return (offset + partition) % numPartitions;
        } else {
            return partition % numPartitions;
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        this.partitionerCache = new PartitionerCache(conf);

        try {
            createDelegatesForTables();
        } catch (ClassNotFoundException e) {
            log.error(e);
            // the validation step during the job set up identifies missing classes, so fail the mapper
            throw new RuntimeException(e);
        }
    }

    // looks at the property that specifies which tables have custom partitioners and registers each of those partitioners
    private void createDelegatesForTables() throws ClassNotFoundException {
        tableOffsets = new HashMap<>();
        String commaSeparatedTableNames = conf.get(TABLE_NAMES_WITH_CUSTOM_PARTITIONERS);
        if (commaSeparatedTableNames == null) {
            log.info("No custom partitioners found");
            return;
        }
        try {
            ArrayList<Text> tableNames = getTableNames(commaSeparatedTableNames);
            partitionerCache.createAndCachePartitioners(tableNames);
            this.tableOffsets = new TablePartitionerOffsets(conf, tableNames, partitionerCache);
        } catch (Exception e) {
            throw new IllegalArgumentException("Problem with tableNames or their partitioner class names. " + commaSeparatedTableNames, e);
        }
    }

    private ArrayList<Text> getTableNames(String commaSeparatedTableNames) {
        ArrayList<Text> result = new ArrayList<>();
        for (String tableName : commaSeparatedTableNames.split(",")) {
            Text tableNameText = new Text(tableName);
            result.add(tableNameText);
        }
        return result;
    }

}
