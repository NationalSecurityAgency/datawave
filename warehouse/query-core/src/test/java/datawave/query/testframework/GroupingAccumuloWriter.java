package datawave.query.testframework;

import com.google.common.collect.Multimap;
import datawave.data.ColumnFamilyConstants;
import datawave.data.hash.UID;
import datawave.data.normalizer.LcNoDiacriticsNormalizer;
import datawave.data.normalizer.Normalizer;
import datawave.data.normalizer.NumberNormalizer;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.helpers.PrintUtility;
import datawave.ingest.protobuf.Uid;
import datawave.query.QueryTestTableHelper;
import datawave.util.TableName;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static datawave.query.testframework.AbstractDataTypeConfig.YMD_DateFormat;

/**
 * Populates Accumulo with data utilizes the Grouping format.
 */
class GroupingAccumuloWriter {
    
    private static final Logger log = Logger.getLogger(GroupingAccumuloWriter.class);
    
    private static final String NULL_SEP = "\u0000";
    private static final String FIELD_INDEX = "fi" + NULL_SEP;
    private static final Value EMPTY_VALUE = new Value(new byte[0]);
    
    private final String shardField;
    private final String dataType;
    private final Connector conn;
    private final ConfigData cfgData;
    private final FieldConfig fieldConfig;
    
    /**
     *
     * @param type
     *            datatype name
     * @param accConn
     *            accumulo connection object
     * @param field
     *            field name that contains the shard date
     * @param idx
     *            indexes applied to the data
     * @param data
     *            raw data to be written into accumulo
     */
    GroupingAccumuloWriter(final String type, final Connector accConn, final String field, final FieldConfig idx, final ConfigData data) {
        this.dataType = type;
        this.conn = accConn;
        this.fieldConfig = idx;
        this.cfgData = data;
        this.shardField = field;
    }
    
    /**
     * Adds the raw data to accumulo.
     * 
     * @param data
     *            raw data (NOTE: the key values in the multimap are expected to be uppercase)
     * @throws MutationsRejectedException
     * @throws TableNotFoundException
     */
    void addData(final List<Map.Entry<Multimap<String,String>,UID>> data) throws MutationsRejectedException, TableNotFoundException {
        final BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(1000L).setMaxLatency(1, TimeUnit.SECONDS).setMaxWriteThreads(1);
        
        writeMetaData(bwConfig, data);
        writeShardKeys(bwConfig, data);
        writeShardIndexKeys(bwConfig, data, TableName.SHARD_INDEX, false);
        writeShardIndexKeys(bwConfig, data, TableName.SHARD_RINDEX, true);
        
        PrintUtility.printTable(this.conn, AbstractDataTypeConfig.getTestAuths(), QueryTestTableHelper.METADATA_TABLE_NAME);
        PrintUtility.printTable(this.conn, AbstractDataTypeConfig.getTestAuths(), TableName.SHARD_INDEX);
        PrintUtility.printTable(this.conn, AbstractDataTypeConfig.getTestAuths(), TableName.SHARD);
        PrintUtility.printTable(this.conn, AbstractDataTypeConfig.getTestAuths(), TableName.SHARD_RINDEX);
    }
    
    private void writeMetaData(BatchWriterConfig bwConfig, final List<Map.Entry<Multimap<String,String>,UID>> data) throws MutationsRejectedException,
                    TableNotFoundException {
        Text dtText = new Text(this.dataType);
        Map<String,RawMetaData> meta = this.cfgData.getMetadata();
        try (BatchWriter bw = this.conn.createBatchWriter(QueryTestTableHelper.METADATA_TABLE_NAME, bwConfig)) {
            for (Map.Entry<Multimap<String,String>,UID> entry : data) {
                Multimap<String,String> rawData = entry.getKey();
                String shardDate = extractShard(rawData);
                
                for (String column : rawData.keySet()) {
                    if (meta.containsKey(column.toLowerCase())) {
                        Mutation mut = new Mutation(column);
                        mut.put(ColumnFamilyConstants.COLF_E, dtText, EMPTY_VALUE);
                        Value colVal = new Value(SummingCombiner.VAR_LEN_ENCODER.encode((long) rawData.get(column).size()));
                        mut.put(ColumnFamilyConstants.COLF_F, new Text(this.dataType + NULL_SEP + shardDate), colVal);
                        if (this.fieldConfig.getIndexFields().contains(column)) {
                            mut.put(ColumnFamilyConstants.COLF_I, dtText, EMPTY_VALUE);
                        }
                        if (this.fieldConfig.getReverseIndexFields().contains(column)) {
                            mut.put(ColumnFamilyConstants.COLF_RI, dtText, EMPTY_VALUE);
                        }
                        Normalizer<?> norm = meta.get(column.toLowerCase()).normalizer;
                        String type = getNormalizerTypeName(norm);
                        mut.put(ColumnFamilyConstants.COLF_T, new Text(this.dataType + NULL_SEP + type), EMPTY_VALUE);
                        
                        bw.addMutation(mut);
                    } else {
                        log.debug("skipping col entry(" + column + ")");
                    }
                }
            }
        }
    }
    
    private void writeShardKeys(BatchWriterConfig bwConfig, final List<Map.Entry<Multimap<String,String>,UID>> data) throws MutationsRejectedException,
                    TableNotFoundException {
        Map<String,RawMetaData> meta = this.cfgData.getMetadata();
        try (BatchWriter bw = this.conn.createBatchWriter(TableName.SHARD, bwConfig)) {
            for (Map.Entry<Multimap<String,String>,UID> entry : data) {
                UID uid = entry.getValue();
                Multimap<String,String> rawData = entry.getKey();
                String shardId = extractShard(rawData);
                long timestamp = shardDateToMillis(shardId);
                shardId = shardId + "_0";
                
                for (String column : rawData.keySet()) {
                    if (meta.containsKey(column.toLowerCase())) {
                        Mutation mut = new Mutation(shardId);
                        int count = 0;
                        
                        int cardinality = rawData.get(column).size();
                        for (String val : rawData.get(column)) {
                            if (this.fieldConfig.getIndexFields().contains(column)) {
                                Normalizer<?> norm = meta.get(column.toLowerCase()).normalizer;
                                mut.put(FIELD_INDEX + column, norm.normalize(val) + NULL_SEP + this.dataType + NULL_SEP + uid,
                                                this.cfgData.getDefaultVisibility(), timestamp, EMPTY_VALUE);
                            }
                            mut.put(this.dataType + NULL_SEP + uid, column + "." + count + NULL_SEP + val, this.cfgData.getDefaultVisibility(), timestamp,
                                            EMPTY_VALUE);
                            count++;
                        }
                        bw.addMutation(mut);
                    } else {
                        log.debug("skipping column entry(" + column + ")");
                    }
                }
            }
        }
    }
    
    private void writeShardIndexKeys(BatchWriterConfig bwConfig, final List<Map.Entry<Multimap<String,String>,UID>> data, String table, boolean reverse)
                    throws MutationsRejectedException, TableNotFoundException {
        Map<String,RawMetaData> meta = this.cfgData.getMetadata();
        Set<String> fields;
        if (reverse) {
            fields = this.fieldConfig.getReverseIndexFields();
        } else {
            fields = this.fieldConfig.getIndexFields();
        }
        try (BatchWriter bw = this.conn.createBatchWriter(table, bwConfig)) {
            for (Map.Entry<Multimap<String,String>,UID> rawEntries : data) {
                UID uid = rawEntries.getValue();
                Multimap<String,String> rawData = rawEntries.getKey();
                String shardId = extractShard(rawData);
                long timestamp = shardDateToMillis(shardId);
                shardId = shardId + "_0";
                
                for (Map.Entry<String,String> entry : rawData.entries()) {
                    if (fields.contains(entry.getKey())) {
                        Normalizer<?> norm = meta.get(entry.getKey().toLowerCase()).normalizer;
                        String normVal = norm.normalize(entry.getValue());
                        if (reverse) {
                            normVal = new StringBuilder(normVal).reverse().toString();
                        }
                        Mutation mut = new Mutation(normVal);
                        Uid.List.Builder builder = Uid.List.newBuilder();
                        builder.addUID(uid.toString());
                        builder.setCOUNT(1);
                        builder.setIGNORE(false);
                        mut.put(entry.getKey().toUpperCase(), shardId + NULL_SEP + this.dataType, this.cfgData.getDefaultVisibility(), timestamp, new Value(
                                        builder.build().toByteArray()));
                        bw.addMutation(mut);
                    }
                }
            }
        }
    }
    
    private String extractShard(Multimap<String,String> data) {
        Collection<String> shard = data.get(this.shardField);
        Assert.assertNotNull("shard date field not found in raw data", shard);
        Assert.assertEquals("shard date is invalid", 1, shard.size());
        return shard.iterator().next();
    }
    
    private long shardDateToMillis(String date) {
        try {
            Calendar cal = Calendar.getInstance();
            Date d = YMD_DateFormat.parse(date);
            cal.setTime(d);
            return cal.getTimeInMillis();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
    
    /**
     * Converts the normalizer to the class name of the type.
     * 
     * @param norm
     *            normalizer object
     * @return class name of the type
     */
    private String getNormalizerTypeName(Normalizer<?> norm) {
        String clName = null;
        if (norm instanceof LcNoDiacriticsNormalizer) {
            clName = LcNoDiacriticsType.class.getName();
        } else if (norm instanceof NumberNormalizer) {
            clName = NumberType.class.getName();
        }
        
        // add others as needed
        Assert.assertNotNull(clName);
        return clName;
    }
}
