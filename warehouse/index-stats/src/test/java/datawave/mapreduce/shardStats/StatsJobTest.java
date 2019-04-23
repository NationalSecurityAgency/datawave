package datawave.mapreduce.shardStats;

import datawave.ingest.mapreduce.job.IngestJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StatsJobTest {
    private static final Logger log = Logger.getLogger(StatsJobTest.class);
    
    static {
        Logger.getLogger(IngestJob.class).setLevel(Level.DEBUG);
        Logger.getLogger(StatsJob.class).setLevel(Level.DEBUG);
        Logger.getLogger(StatsJobTest.class).setLevel(Level.DEBUG);
    }
    
    private StatsJobWrapper wrapper;
    
    @Before
    public void setup() {
        wrapper = new StatsJobWrapper();
        
    }
    
    @Test
    public void testParseArguments() throws Exception {
        Assume.assumeTrue(null != System.getenv("DATAWAVE_INGEST_HOME"));
        log.info("======  testParseArguments  =====");
        Map<String,Object> mapArgs = new HashMap<>();
        
        // mapper options
        mapArgs.put(StatsHyperLogMapper.STATS_MAPPER_INPUT_INTERVAL, 4);
        mapArgs.put(StatsHyperLogMapper.STATS_MAPPER_OUTPUT_INTERVAL, 8);
        mapArgs.put(StatsHyperLogMapper.STATS_MAPPER_LOG_LEVEL, "map-log");
        
        // reducer options
        mapArgs.put(StatsHyperLogReducer.STATS_REDUCER_VALUE_INTERVAL, 6);
        mapArgs.put(StatsHyperLogReducer.STATS_MIN_COUNT, 1);
        mapArgs.put(StatsHyperLogReducer.STATS_REDUCER_COUNTS, Boolean.FALSE);
        mapArgs.put(StatsHyperLogReducer.STATS_REDUCER_LOG_LEVEL, "red-log");
        
        String[] args = new String[mapArgs.size()];
        int n = 0;
        for (Map.Entry<String,Object> entry : mapArgs.entrySet()) {
            args[n++] = "-" + entry.getKey() + "=" + entry.getValue();
        }
        args = addRequiredSettings(args);
        
        wrapper.parseArguments(args, mapArgs);
    }
    
    private String[] addRequiredSettings(String[] args) {
        List<String> addArgs = new ArrayList<>();
        // this must be first
        addArgs.add("paths");
        
        for (String str : args) {
            addArgs.add(str);
        }
        
        addArgs.add("-workDir");
        addArgs.add("/tmp");
        addArgs.add(IngestJob.REDUCE_TASKS_ARG_PREFIX + "1");
        addArgs.add("-destHdfs");
        addArgs.add("hdfs");
        addArgs.add("-flagFileDir");
        addArgs.add("/flagDir");
        
        // add configuration settings
        wrapper.set(StatsJob.STATS_VISIBILITY, "vis");
        wrapper.set(StatsJob.INPUT_TABLE_NAME, "input");
        wrapper.set(StatsJob.OUTPUT_TABLE_NAME, "output");
        
        return addArgs.toArray(new String[addArgs.size()]);
    }
    
    private static class StatsJobWrapper {
        
        private final StatsJob job = new StatsJob();
        private final Configuration conf = new Configuration();
        
        void parseArguments(String[] args, Map<String,?> values) throws ClassNotFoundException, URISyntaxException, IllegalArgumentException {
            final Configuration parsed = job.parseArguments(args, conf);
            verifyConfig(parsed, values);
        }
        
        void set(String key, String value) {
            this.conf.set(key, value);
        }
        
        private void verifyConfig(Configuration conf, Map<String,?> values) {
            for (Map.Entry<String,?> entry : values.entrySet()) {
                Object val = null;
                if (entry.getValue() instanceof String) {
                    val = conf.get(entry.getKey());
                } else if (entry.getValue() instanceof Integer) {
                    val = conf.getInt(entry.getKey(), -1);
                } else if (entry.getValue() instanceof Boolean) {
                    Boolean bool = (Boolean) entry.getValue();
                    val = conf.getBoolean(entry.getKey(), !bool);
                } else {
                    Assert.fail("add instance handler for " + entry.getValue().getClass().getSimpleName());
                }
                
                Assert.assertNotNull(val);
                Assert.assertEquals(entry.getValue(), val);
            }
        }
    }
}
