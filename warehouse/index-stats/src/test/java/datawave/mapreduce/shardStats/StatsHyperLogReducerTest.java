package datawave.mapreduce.shardStats;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StatsHyperLogReducerTest {
    
    private static final Logger log = Logger.getLogger(StatsHyperLogReducerTest.class);
    
    static {
        Logger.getLogger(StatsHyperLogReducer.class).setLevel(Level.DEBUG);
        Logger.getLogger(StatsHyperLogSummary.class).setLevel(Level.DEBUG);
        Logger.getLogger(StatsHyperLogReducerTest.class).setLevel(Level.DEBUG);
        Logger.getLogger(StatsTestData.class).setLevel(Level.DEBUG);
    }
    
    @Test
    public void testOneValue() throws IOException, InterruptedException {
        log.info("-----  testOneValue  ------");
        List<StatsTestData> testEntries = Arrays.asList(StatsTestData.FOne_VUno);
        runDriver(testEntries, 0);
    }
    
    @Test
    public void testDuplicateInputValues() throws IOException, InterruptedException {
        log.info("-----  testDuplicateInputValues  ------");
        List<StatsTestData> testEntries = Arrays.asList(StatsTestData.FOne_VUno);
        runDriver(testEntries, 3);
    }
    
    @Test
    public void testMultiValues() throws IOException, InterruptedException {
        log.info("-----  testMultiValues  ------");
        List<StatsTestData> testEntries = Arrays.asList(StatsTestData.FOne_VUno, StatsTestData.FTwo_VUno_T2);
        runDriver(testEntries, 0);
    }
    
    @Test
    public void testMultiValuesWithDups() throws IOException, InterruptedException {
        log.info("-----  testMultiValuesWithDups  ------");
        List<StatsTestData> testEntries = Arrays.asList(StatsTestData.FOne_VUno, StatsTestData.FTwo_VUno_T2, StatsTestData.FTwo_VDos_T2);
        runDriver(testEntries, 4);
    }
    
    @Test
    public void testAllWithDups() throws IOException, InterruptedException {
        log.info("-----  testAllWithDups  ------");
        List<StatsTestData> testEntries = new ArrayList<>(Arrays.asList(StatsTestData.values()));
        runDriver(testEntries, 2);
    }
    
    // =====================================
    // private methods
    private void runDriver(List<StatsTestData> entries, int dupCount) throws IOException, InterruptedException {
        final Reducer<BulkIngestKey,Value,BulkIngestKey,Value> reducer = new StatsHyperLogReducer();
        final MockReduceDriver<BulkIngestKey,Value,BulkIngestKey,Value> driver = new MockReduceDriver(reducer);
        
        // set the output table name
        Configuration conf = driver.getConfiguration();
        conf.set(StatsJob.OUTPUT_TABLE_NAME, StatsInit.TEST_TABLE);
        conf.set(StatsHyperLogReducer.STATS_REDUCER_LOG_LEVEL, Level.DEBUG.toString());
        conf.set(StatsHyperLogReducer.STATS_REDUCER_VALUE_INTERVAL, "5");
        
        log.debug("=====  REDUCER INPUT  =====");
        // generate input
        Map<BulkIngestKey,Value> input = StatsTestData.generateMapOutput(entries);
        Map<BulkIngestKey,StatsCounters> output = new HashMap<>();
        
        for (Map.Entry<BulkIngestKey,Value> entry : input.entrySet()) {
            BulkIngestKey inKey = entry.getKey();
            List<Value> values = new ArrayList<>();
            values.add(entry.getValue());
            
            HyperLogLogPlus hllp = new HyperLogLogPlus(StatsJob.HYPERLOG_NORMAL_DEFAULT_VALUE, StatsJob.HYPERLOG_SPARSE_DEFAULT_VALUE);
            FieldSummary summary = new HyperLogFieldSummary(hllp);
            summary.add(entry.getValue());
            for (int n = 0; n < dupCount; n++) {
                values.add(entry.getValue());
                summary.add(entry.getValue());
            }
            
            log.debug("key(" + inKey.getKey() + ") value(" + summary.toString() + ")");
            driver.addInput(inKey, values);
            output.put(inKey, summary.toStatsCounters());
        }
        
        log.debug("=====  EXPECTED REDUCER OUTPUT  =====");
        // generate output data
        for (Map.Entry<BulkIngestKey,StatsCounters> entry : output.entrySet()) {
            log.debug("key(" + entry.getKey().getKey() + ") value(" + entry.getValue() + ")");
        }
        
        List<MRPair<BulkIngestKey,Value>> fullResults = driver.run();
        log.debug("=====  RESULTS  =====");
        Assert.assertEquals("result size does not match expected", output.size(), fullResults.size());
        for (MRPair<BulkIngestKey,Value> result : fullResults) {
            BulkIngestKey rKey = result.key;
            // reset timestamp to 0
            // hashcode for BulkIngestKey is cached - this will not reset hashcode
            // however equals will work correctly
            Key key = rKey.getKey();
            key.setTimestamp(0);
            
            Value rVal = result.value;
            StatsCounters counts = new StatsCounters();
            try (InputStream bis = new ByteArrayInputStream(rVal.get())) {
                DataInput is = new DataInputStream(bis);
                counts.readFields(is);
            }
            log.debug("key(" + key + ") value(" + counts.toString() + ")");
            
            // iterate output to find matching BulkIngestKey
            for (BulkIngestKey oKey : output.keySet()) {
                // equals will work with correctly
                if (oKey.equals(rKey)) {
                    Assert.assertEquals(output.get(oKey), counts);
                    break;
                }
            }
        }
    }
}
