package datawave.query.testframework;

import datawave.ingest.data.TypeRegistry;
import datawave.ingest.mapreduce.EventMapper;
import datawave.ingest.mapreduce.handler.dateindex.DateIndexDataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.job.writer.AggregatingContextWriter;
import datawave.ingest.mapreduce.job.writer.ChainedContextWriter;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.mapreduce.job.writer.DedupeContextWriter;
import datawave.ingest.mapreduce.job.writer.LiveContextWriter;
import datawave.query.QueryTestTableHelper;
import datawave.util.TableName;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Defines Hadoop configuration data for unit testing.
 */
public class HadoopTestConfiguration extends Configuration {
    
    private static final Logger log = Logger.getLogger(HadoopTestConfiguration.class);
    
    private static final Map<String,String> DefaultTables = new HashMap<>();
    
    static {
        DefaultTables.put(ShardedDataTypeHandler.METADATA_TABLE_NAME, QueryTestTableHelper.METADATA_TABLE_NAME);
        DefaultTables.put(ShardedDataTypeHandler.SHARD_TNAME, TableName.SHARD);
        DefaultTables.put(ShardedDataTypeHandler.SHARD_GIDX_TNAME, TableName.SHARD_INDEX);
        DefaultTables.put(ShardedDataTypeHandler.SHARD_GRIDX_TNAME, TableName.SHARD_RINDEX);
        DefaultTables.put(ShardedDataTypeHandler.SHARD_DINDX_NAME, QueryTestTableHelper.SHARD_DICT_INDEX_NAME);
        DefaultTables.put(DateIndexDataTypeHandler.DATEINDEX_TNAME, TableName.DATE_INDEX);
    }
    
    public HadoopTestConfiguration(DataTypeHadoopConfig dataType) {
        this(dataType, DefaultTables);
    }
    
    public HadoopTestConfiguration(DataTypeHadoopConfig dataType, Map<String,String> tables) {
        // add datatype configuration
        this.addResource(dataType.getHadoopConfiguration());
        
        // add tables to configuration
        for (final Map.Entry entry : tables.entrySet()) {
            this.set((String) entry.getKey(), (String) entry.getValue());
        }
        
        // update datatypes
        String types = this.get(TypeRegistry.INGEST_DATA_TYPES);
        this.set(TypeRegistry.INGEST_DATA_TYPES, (null == types) ? dataType.dataType() : types + "," + dataType.dataType());
        
        // additional settings
        this.set("multiple.numshards.enable", "false");
        
        // mapper options
        this.set(EventMapper.DISCARD_INTERVAL, Long.toString(Long.MAX_VALUE));
        this.set(EventMapper.CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS, Boolean.TRUE.toString());
        this.set(EventMapper.CONTEXT_WRITER_OUTPUT_TABLE_COUNTERS, Boolean.FALSE.toString());
        this.set(ShardedDataTypeHandler.NUM_SHARDS, "1");
        this.set("mapreduce.map.output.value.class", Mutation.class.getName());
        this.setClass(DedupeContextWriter.CONTEXT_WRITER_CLASS, AggregatingContextWriter.class, ChainedContextWriter.class);
        this.setClass(AggregatingContextWriter.CONTEXT_WRITER_CLASS, LiveContextWriter.class, ContextWriter.class);
    }
    
    public HadoopTestConfiguration(Configuration conf) {
        super(conf);
    }
}
