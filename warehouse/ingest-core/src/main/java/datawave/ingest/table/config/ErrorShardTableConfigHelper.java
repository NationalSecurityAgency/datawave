package datawave.ingest.table.config;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.handler.error.ErrorShardedDataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;

/**
 * TableConfigHelper implementation for the "sharded" error tables. This class should perform the majority of the same operations that the
 * ShardTableConfigHelper performs (minus any content specific operations).
 *
 *
 */
public class ErrorShardTableConfigHelper extends ShardTableConfigHelper {

    public static final String SHARD_ERROR_ENABLE_BLOOM_FILTERS = ErrorShardedDataTypeHandler.ERROR_PROP_PREFIX + ENABLE_BLOOM_FILTERS;
    public static final String ERROR_LOCALITY_GROUPS = ErrorShardedDataTypeHandler.ERROR_PROP_PREFIX + LOCALITY_GROUPS;

    @Override
    public void setup(String tableName, Configuration config, Logger log) throws IllegalArgumentException {
        this.log = log;
        this.conf = config;

        this.shardTableName = conf.get(ErrorShardedDataTypeHandler.ERROR_PROP_PREFIX + ShardedDataTypeHandler.SHARD_TNAME, null);
        this.shardGidxTableName = conf.get(ErrorShardedDataTypeHandler.ERROR_PROP_PREFIX + ShardedDataTypeHandler.SHARD_GIDX_TNAME, null);
        this.shardGridxTableName = conf.get(ErrorShardedDataTypeHandler.ERROR_PROP_PREFIX + ShardedDataTypeHandler.SHARD_GRIDX_TNAME, null);
        this.shardDictionaryTableName = conf.get(ErrorShardedDataTypeHandler.ERROR_PROP_PREFIX + ShardedDataTypeHandler.SHARD_DINDX_NAME, null);

        if (shardTableName == null && shardGidxTableName == null && shardGridxTableName == null) {
            throw new IllegalArgumentException("No Shard Tables Defined");
        }

        enableBloomFilters = conf.getBoolean(SHARD_ERROR_ENABLE_BLOOM_FILTERS, enableBloomFilters);

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
            throw new IllegalArgumentException("Invalid Shard Error Table Definition For: " + tableName);
        }

        this.tableName = tableName;
    }
}
