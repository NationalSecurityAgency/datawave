package datawave.mapreduce.shardStats;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static datawave.mapreduce.shardStats.StatsJob.HYPERLOG_NORMAL_DEFAULT_VALUE;
import static datawave.mapreduce.shardStats.StatsJob.HYPERLOG_SPARSE_DEFAULT_VALUE;

/**
 * Manager for test data for the {@link StatsHyperLogMapperTest} and {@link StatsHyperLogReducerTest}.
 */
enum StatsTestData {
    // format => date, field name, data type, value
    // see constructor
    FOne_VUno("20010911", "fn-one", "t-1", "v-uno"),
    FOne_VDos("20010911", "fn-one", "t-1", "v-dos"),
    FOne_VTres("20010911", "fn-one", "t-1", "v-tres"),
    FTwo_VUno_T2("20010911", "fn-two", "t-2", "v-uno"),
    FTwo_VDos_T2("20010911", "fn-two", "t-2", "v-dos"),
    FTwo_VTres_T2("20010911", "fn-two", "t-2", "v-tres"),
    FTwo_VUno_T200("20010911", "fn-two", "t-200", "v-uno"),
    FTwo_VDos_T201("20010911", "fn-two", "t-201", "v-dos"),
    FTwo_VTres_T202("20010911", "fn-two", "t-202", "v-tres"),
    FThree_VUno("20010911", "fn-three", "t-3", "v-uno"),
    FThree_VDos("20010911", "fn-three", "t-3", "v-dos"),
    FThree_VTres("20010911", "fn-three", "t-3", "v-tres");
    
    private static final Logger log = Logger.getLogger(StatsTestData.class);
    
    /**
     * Generates a list of keys for input into the mapper.
     * 
     * @param eValues
     *            enum value to include in test
     * @return list of mapper input keys
     */
    static List<Key> generateMapperInput(List<StatsTestData> eValues) {
        final List<Key> inKeys = new ArrayList<>();
        for (StatsTestData data : eValues) {
            inKeys.add(data.mapperInputKey);
        }
        return inKeys;
    }
    
    /**
     * Generates the expected output data based upon the enum values used for the test.
     * 
     * @param eValues
     *            enum values to include in test
     * @return mapping of {@link BulkIngestKey} to serialized {@link HyperLogLogPlus} object
     * @throws IOException
     */
    static Map<BulkIngestKey,Value> generateMapOutput(List<StatsTestData> eValues) throws IOException {
        final Map<BulkIngestKey,Value> output = new HashMap<>();
        
        // create values for test entries
        final Map<BulkIngestKey,List<String>> keyValues = new HashMap<>();
        final Map<BulkIngestKey,HyperLogLogPlus> logPlusEntries = new HashMap<>();
        for (final StatsTestData eVal : eValues) {
            BulkIngestKey key = new BulkIngestKey(StatsInit.TABLE, eVal.mapperOutputKey);
            List<String> values = keyValues.get(key);
            HyperLogLogPlus hllp = logPlusEntries.get(key);
            if (null == values) {
                values = new ArrayList<>();
                keyValues.put(key, values);
                hllp = new HyperLogLogPlus(HYPERLOG_NORMAL_DEFAULT_VALUE, HYPERLOG_SPARSE_DEFAULT_VALUE);
                logPlusEntries.put(key, hllp);
            }
            values.add(eVal.value);
            hllp.offer(eVal.value);
        }
        
        // set output data
        for (Map.Entry<BulkIngestKey,List<String>> entry : keyValues.entrySet()) {
            List<String> values = entry.getValue();
            Set<String> unique = new TreeSet<>(entry.getValue());
            HyperLogLogPlus logPlus = logPlusEntries.get(entry.getKey());
            StatsHyperLogSummary stats = new StatsHyperLogSummary(values.size(), logPlus, unique.size());
            log.debug("key(" + entry.getKey().getKey() + ") value(" + stats + ")");
            Value serialized = new Value(stats.toByteArray());
            output.put(entry.getKey(), serialized);
        }
        
        return output;
    }
    
    private final Key mapperInputKey;
    private final Key mapperOutputKey;
    private final String value;
    
    /**
     *
     * @param date
     *            date for rowid
     * @param fieldName
     *            field name is part of the column family
     * @param type
     *            field datatype is part of the column qualifier
     * @param fieldValue
     *            field value of part of the column qualifier
     */
    StatsTestData(String date, String fieldName, String type, String fieldValue) {
        // create mapper input key
        Text rowId = new Text(date + "_1");
        Text colFam = new Text("fi" + StatsInit.NUL_SEPERATOR + fieldName);
        Text colQual = new Text(fieldValue + StatsInit.NUL_SEPERATOR + type + StatsInit.NUL_SEPERATOR + "uid");
        this.mapperInputKey = new Key(rowId, colFam, colQual);
        
        // create mapper output key
        Text outRow = new Text(fieldName);
        Text outFam = new Text(date);
        Text outQual = new Text(type);
        this.mapperOutputKey = new Key(outRow, outFam, outQual, StatsInit.TEST_VISIBILITY, 0);
        
        this.value = fieldValue;
    }
    
    Key getMapperInputKey() {
        return mapperInputKey;
    }
    
    Key getMapperOutputKey() {
        return mapperOutputKey;
    }
}
