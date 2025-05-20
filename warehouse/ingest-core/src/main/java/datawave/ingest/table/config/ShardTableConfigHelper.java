package datawave.ingest.table.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.table.aggregator.CombinerConfiguration;
import datawave.ingest.table.aggregator.GlobalIndexUidAggregator;
import datawave.ingest.table.aggregator.KeepCountOnlyNoUidAggregator;
import datawave.ingest.table.aggregator.KeepCountOnlyUidAggregator;
import datawave.ingest.table.balancer.ShardedTableTabletBalancer;
import datawave.ingest.table.bloomfilter.ShardIndexKeyFunctor;
import datawave.ingest.table.bloomfilter.ShardKeyFunctor;

public class ShardTableConfigHelper extends AbstractTableConfigHelper {

    protected static final String SHARDED_TABLET_BALANCER_CLASS = ShardedTableTabletBalancer.class.getName();

    public static final String KEEP_COUNT_ONLY_INDEX_ENTRIES = "index.tables.keep.count.only.entries";

    public static final String KEEP_COUNT_ONLY_INDEX_NO_UIDS = "index.tables.keep.count.only.no.uids";

    public static final String SHARD_TABLE_BALANCER_CONFIG = "shard.table.balancer.class";
    protected String shardTableBalancerClass = SHARDED_TABLET_BALANCER_CLASS;

    public static final String ENABLE_BLOOM_FILTERS = "shard.enable.bloom.filters";
    protected boolean enableBloomFilters = false;

    public static final String MARKINGS_SETUP_ITERATOR_ENABLED = "markings.setup.iterator.enabled";
    private boolean markingsSetupIteratorEnabled = false;

    public static final String MARKINGS_SETUP_ITERATOR_CONFIG = "markings.setup.iterator.config";
    private String markingsSetupIteratorConfig;

    public static final String LOCALITY_GROUPS = "shard.table.locality.groups";
    protected HashMap<String,Set<Text>> localityGroups = new HashMap<>();

    protected static final String SHARD_KEY_FUNCTOR_CLASS = ShardKeyFunctor.class.getName();

    protected Logger log;

    public enum ShardTableType {
        SHARD, GIDX, GRIDX, DINDX
    }

    protected Configuration conf;
    protected String tableName;
    protected String shardTableName; // shard table
    protected String shardGidxTableName; // global index
    protected String shardGridxTableName; // global reverse index
    protected String shardDictionaryTableName;
    protected ShardTableType tableType;

    @Override
    public void setup(String tableName, Configuration config, Logger log) throws IllegalArgumentException {

        this.log = log;
        this.conf = config;

        shardTableName = conf.get(ShardedDataTypeHandler.SHARD_TNAME, null);
        shardGidxTableName = conf.get(ShardedDataTypeHandler.SHARD_GIDX_TNAME, null);
        shardGridxTableName = conf.get(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, null);
        shardDictionaryTableName = conf.get(ShardedDataTypeHandler.SHARD_DINDX_NAME, null);
        markingsSetupIteratorEnabled = conf.getBoolean(MARKINGS_SETUP_ITERATOR_ENABLED, markingsSetupIteratorEnabled);
        markingsSetupIteratorConfig = conf.get(MARKINGS_SETUP_ITERATOR_CONFIG, markingsSetupIteratorConfig);

        if (shardTableName == null && shardGidxTableName == null && shardGridxTableName == null && shardDictionaryTableName == null) {
            throw new IllegalArgumentException("No Shard Tables Defined");
        }

        shardTableBalancerClass = conf.get(SHARD_TABLE_BALANCER_CONFIG, SHARDED_TABLET_BALANCER_CLASS);

        if (markingsSetupIteratorEnabled) {
            if (null == markingsSetupIteratorConfig || markingsSetupIteratorConfig.equals("")) {
                throw new IllegalArgumentException("No '" + MARKINGS_SETUP_ITERATOR_CONFIG + "' Option Defined");
            }
        }

        enableBloomFilters = conf.getBoolean(ENABLE_BLOOM_FILTERS, enableBloomFilters);

        String localityGroupsConf = null;
        if (tableName.equals(shardTableName)) {
            localityGroupsConf = conf.get(shardTableName + LOCALITY_GROUPS,
                            ExtendedDataTypeHandler.FULL_CONTENT_LOCALITY_NAME + ':' + ExtendedDataTypeHandler.FULL_CONTENT_COLUMN_FAMILY + ','
                                            + ExtendedDataTypeHandler.TERM_FREQUENCY_LOCALITY_NAME + ':'
                                            + ExtendedDataTypeHandler.TERM_FREQUENCY_COLUMN_FAMILY);
            for (String localityGroupDefConf : StringUtils.split(localityGroupsConf)) {
                String[] localityGroupDef = StringUtils.split(localityGroupDefConf, '\\', ':');
                Set<Text> families = localityGroups.get(localityGroupDef[0]);
                if (families == null) {
                    families = new HashSet<>();
                    localityGroups.put(localityGroupDef[0], families);
                }
                families.add(new Text(localityGroupDef[1]));
            }
        } else if (tableName.equals(shardDictionaryTableName)) {
            localityGroupsConf = conf.get(shardDictionaryTableName + LOCALITY_GROUPS,
                            ShardedDataTypeHandler.SHARD_DINDX_FLABEL_LOCALITY_NAME + ':' + ShardedDataTypeHandler.SHARD_DINDX_FLABEL + ','
                                            + ShardedDataTypeHandler.SHARD_DINDX_RLABEL_LOCALITY_NAME + ':' + ShardedDataTypeHandler.SHARD_DINDX_RLABEL);
            for (String localityGroupDefConf : StringUtils.split(localityGroupsConf)) {
                String[] localityGroupDef = StringUtils.split(localityGroupDefConf, '\\', ':');
                Set<Text> families = localityGroups.get(localityGroupDef[0]);
                if (families == null) {
                    families = new HashSet<>();
                    localityGroups.put(localityGroupDef[0], families);
                }
                families.add(new Text(localityGroupDef[1]));
            }

        }

        if (shardTableName != null && tableName.equals(shardTableName)) {
            this.tableType = ShardTableType.SHARD;
        } else if (shardGidxTableName != null && tableName.equals(shardGidxTableName)) {
            this.tableType = ShardTableType.GIDX;
        } else if (shardGridxTableName != null && tableName.equals(shardGridxTableName)) {
            this.tableType = ShardTableType.GRIDX;
        } else if (shardDictionaryTableName != null && tableName.equals(shardDictionaryTableName)) {
            this.tableType = ShardTableType.DINDX;
        } else {
            throw new IllegalArgumentException("Invalid Shard Table Definition For: " + tableName);
        }
        this.tableName = tableName;
    }

    @Override
    public void configure(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

        switch (this.tableType) {
            case SHARD:
                configureShardTable(tops);
                break;
            case GIDX:
                configureGidxTable(tops);
                break;
            case GRIDX:
                configureGridxTable(tops);
                break;

            case DINDX:
                configureDictionaryTable(tops);

                break;
            default:
                // Technically, this is dead code. If 'Configure' is called prior to 'Setup'
                // tableType is null and throws a NullPointerException in the switch statement.
                // If 'Setup' successfully runs to completion then tableType is assigned one
                // of the three other values.
                throw new TableNotFoundException(null, tableName, "Table is not a Shard Type Table");
        }
    }

    protected void configureShardTable(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        // Set a text index aggregator on the "tf" (Term Frequency) column family
        CombinerConfiguration tfConf = new CombinerConfiguration(new Column("tf"),
                        new IteratorSetting(10, "TF", datawave.ingest.table.aggregator.TextIndexAggregator.class.getName()));

        setAggregatorConfigurationIfNecessary(tableName, Collections.singletonList(tfConf), tops, conf, log);

        if (markingsSetupIteratorEnabled) {
            for (IteratorScope scope : IteratorScope.values()) {
                // we want the markings setup iterator init method to be called up front
                String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name(), "MarkingsLoader");
                setPropertyIfNecessary(tableName, stem, markingsSetupIteratorConfig, tops, log);
            }
        }

        // Set the locality group for the full content column family
        setLocalityGroupConfigurationIfNecessary(tableName, localityGroups, tops, log);

        // Set up the bloom filters for faster queries on the index portion
        if (enableBloomFilters) {
            setPropertyIfNecessary(tableName, Property.TABLE_BLOOM_KEY_FUNCTOR.getKey(), SHARD_KEY_FUNCTOR_CLASS, tops, log);
        }
        setPropertyIfNecessary(tableName, Property.TABLE_BLOOM_ENABLED.getKey(), Boolean.toString(enableBloomFilters), tops, log);

        // Set up the table balancer for shards
        setPropertyIfNecessary(tableName, Property.TABLE_LOAD_BALANCER.getKey(), shardTableBalancerClass, tops, log);
    }

    protected void configureGidxTable(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        // Add the UID aggregator
        for (IteratorScope scope : IteratorScope.values()) {
            String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name(), "UIDAggregator");
            setPropertyIfNecessary(tableName, stem, "19,datawave.iterators.TotalAggregatingIterator", tops, log);
            stem += ".opt.";

            String aggClass = GlobalIndexUidAggregator.class.getName();
            if (conf.getBoolean(KEEP_COUNT_ONLY_INDEX_ENTRIES, false)) {
                aggClass = KeepCountOnlyUidAggregator.class.getName();
            }

            if (conf.getBoolean(KEEP_COUNT_ONLY_INDEX_NO_UIDS, false)) {
                aggClass = KeepCountOnlyNoUidAggregator.class.getName();
            }

            setPropertyIfNecessary(tableName, stem + "*", aggClass, tops, log);

            if (markingsSetupIteratorEnabled) {
                // we want the markings setup iterator init method to be called up front
                stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name(), "MarkingsLoader");
                setPropertyIfNecessary(tableName, stem, markingsSetupIteratorConfig, tops, log);
            }
        }

        // Set up the bloom filters for faster queries on the index portion
        if (enableBloomFilters) {
            setPropertyIfNecessary(tableName, Property.TABLE_BLOOM_KEY_FUNCTOR.getKey(), ShardIndexKeyFunctor.class.getName(), tops, log);
        }
        setPropertyIfNecessary(tableName, Property.TABLE_BLOOM_ENABLED.getKey(), Boolean.toString(enableBloomFilters), tops, log);

    }

    protected void configureGridxTable(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        // Add the UID aggregator
        for (IteratorScope scope : IteratorScope.values()) {
            String stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name(), "UIDAggregator");
            setPropertyIfNecessary(tableName, stem, "19,datawave.iterators.TotalAggregatingIterator", tops, log);
            stem += ".opt.";

            String aggClass = GlobalIndexUidAggregator.class.getName();
            if (conf.getBoolean(KEEP_COUNT_ONLY_INDEX_ENTRIES, false)) {
                aggClass = KeepCountOnlyUidAggregator.class.getName();
            }

            if (conf.getBoolean(KEEP_COUNT_ONLY_INDEX_NO_UIDS, false)) {
                aggClass = KeepCountOnlyNoUidAggregator.class.getName();
            }

            setPropertyIfNecessary(tableName, stem + "*", aggClass, tops, log);

            if (markingsSetupIteratorEnabled) {
                // we want the markings setup iterator init method to be called up front
                stem = String.format("%s%s.%s", Property.TABLE_ITERATOR_PREFIX, scope.name(), "MarkingsLoader");
                setPropertyIfNecessary(tableName, stem, markingsSetupIteratorConfig, tops, log);
            }
        }

        // Set up the bloom filters for faster queries on the index portion
        if (enableBloomFilters) {
            setPropertyIfNecessary(tableName, Property.TABLE_BLOOM_KEY_FUNCTOR.getKey(), ShardIndexKeyFunctor.class.getName(), tops, log);
        }
        setPropertyIfNecessary(tableName, Property.TABLE_BLOOM_ENABLED.getKey(), Boolean.toString(enableBloomFilters), tops, log);

    }

    protected void configureDictionaryTable(TableOperations tops) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

        setLocalityGroupConfigurationIfNecessary(tableName, localityGroups, tops, log);

    }
}
