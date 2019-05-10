package datawave.mapreduce.shardStats;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static datawave.mapreduce.shardStats.StatsInit.EMPTY_VALUE;
import static datawave.mapreduce.shardStats.StatsInit.TEST_TABLE;

public class StatsHyperLogMapperTest {
    private static final Logger log = Logger.getLogger(StatsHyperLogMapperTest.class);
    
    static {
        Logger.getLogger(StatsHyperLogMapper.class).setLevel(Level.DEBUG);
        Logger.getLogger(StatsHyperLogSummary.class).setLevel(Level.DEBUG);
        Logger.getLogger(StatsHyperLogMapperTest.class).setLevel(Level.DEBUG);
        Logger.getLogger(StatsTestData.class).setLevel(Level.DEBUG);
    }
    
    @Test
    public void testOneValue() throws IOException, InterruptedException {
        log.info("-----  testOneValue  ------");
        List<StatsTestData> testEntries = Arrays.asList(StatsTestData.FOne_VUno);
        runDriver(testEntries);
    }
    
    @Test
    public void testDuplicateInputValues() throws IOException, InterruptedException {
        log.info("-----  testDuplicateInputValues  ------");
        List<StatsTestData> testEntries = Arrays.asList(StatsTestData.FOne_VUno, StatsTestData.FOne_VUno);
        runDriver(testEntries);
    }
    
    @Test
    public void testMultiValues() throws IOException, InterruptedException {
        log.info("-----  testMultiValues  ------");
        List<StatsTestData> testEntries = Arrays.asList(StatsTestData.FOne_VUno, StatsTestData.FTwo_VUno_T2);
        runDriver(testEntries);
    }
    
    @Test
    public void testMultiValuesWithDups() throws IOException, InterruptedException {
        log.info("-----  testMultiValuesWithDups  ------");
        List<StatsTestData> testEntries = Arrays.asList(StatsTestData.FOne_VUno, StatsTestData.FTwo_VUno_T2, StatsTestData.FTwo_VUno_T2,
                        StatsTestData.FTwo_VUno_T2);
        runDriver(testEntries);
    }
    
    @Test
    public void testAllValuesWithDups() throws IOException, InterruptedException {
        log.info("-----  testAllValuesWithDups  ------");
        List<StatsTestData> testEntries = new ArrayList<>();
        for (StatsTestData data : StatsTestData.values()) {
            testEntries.add(data);
            testEntries.add(data);
            testEntries.add(data);
        }
        runDriver(testEntries);
    }
    
    // =====================================
    // private methods
    private void runDriver(List<StatsTestData> entries) throws IOException, InterruptedException {
        final Mapper<Key,Value,BulkIngestKey,Value> mapper = new StatsHyperLogMapper();
        final MockMapDriver<Key,Value,BulkIngestKey,Value> driver = new MockMapDriver(mapper);
        
        // set output table name
        Configuration conf = driver.getConfiguration();
        conf.set(StatsJob.OUTPUT_TABLE_NAME, TEST_TABLE);
        conf.set(StatsHyperLogMapper.STATS_MAPPER_LOG_LEVEL, Level.DEBUG.toString());
        conf.set(StatsHyperLogMapper.STATS_MAPPER_INPUT_INTERVAL, "10");
        conf.set(StatsHyperLogMapper.STATS_MAPPER_OUTPUT_INTERVAL, "4");
        conf.set(StatsHyperLogMapper.STATS_MAPPER_UNIQUE_COUNT, "true");
        conf.set(StatsJob.STATS_VISIBILITY, "vis");
        
        log.debug("=====  MAPPER INPUT  =====");
        // generate input and output entries
        List<Key> inputKeys = StatsTestData.generateMapperInput(entries);
        for (Key key : inputKeys) {
            log.debug("key(" + key + ")");
            driver.addInput(key, EMPTY_VALUE);
        }
        
        log.debug("=====  EXPECTED MAPPER OUTPUT  =====");
        Map<BulkIngestKey,Value> output = StatsTestData.generateMapOutput(entries);
        Map<BulkIngestKey,StatsHyperLogSummary> summary = new HashMap<>();
        for (Map.Entry<BulkIngestKey,Value> entry : output.entrySet()) {
            StatsHyperLogSummary stats = new StatsHyperLogSummary(entry.getValue());
            summary.put(entry.getKey(), stats);
        }
        
        // run test
        List<MRPair<BulkIngestKey,Value>> results = driver.run();
        Assert.assertEquals("results size does not match expected", output.size(), results.size());
        
        for (MRPair<BulkIngestKey,Value> entry : results) {
            BulkIngestKey rKey = entry.key;
            
            Key k = rKey.getKey();
            k.setTimestamp(0);
            
            // timestamp has been altered - keys will not have same hashcode
            // find output key
            BulkIngestKey oKey = null;
            for (BulkIngestKey key : output.keySet()) {
                if (key.equals(rKey)) {
                    oKey = key;
                    break;
                }
            }
            Assert.assertNotNull(oKey);
            Value rVal = entry.value;
            StatsHyperLogSummary stats = new StatsHyperLogSummary(rVal);
            log.debug("key(" + oKey.getKey() + ") value(" + stats + ")");
            Assert.assertEquals("key(" + oKey.getKey() + ")", summary.get(oKey), stats);
            
            // for small sample size cardinality should math real value
            Assert.assertEquals(stats.getUniqueCount(), stats.getHyperLogPlus().cardinality());
        }
    }
}
