package datawave.mapreduce.shardStats;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class StatsHyperLogSummaryTest {
    private static final Logger log = Logger.getLogger(StatsHyperLogSummaryTest.class);
    
    private static final int MAX_UNIQUE_VALUES = 60;
    private static final int MIN_UNIQUE_VALUES = 20;
    private static final int MAX_DUP_VALUES = 30;
    private static final int MIN_DUP_VALUES = 15;
    
    private static final Random rVal = new Random(System.currentTimeMillis());
    
    static {
        Logger.getLogger(StatsHyperLogSummary.class).setLevel(Level.DEBUG);
        Logger.getLogger(StatsHyperLogSummaryTest.class).setLevel(Level.DEBUG);
    }
    
    private int uniqueCount;
    
    @Test
    public void testSerialize() throws IOException {
        for (int n = 0; n < 10; n++) {
            HyperLogLogPlus logPlus = createHyperLog();
            final StatsHyperLogSummary before = new StatsHyperLogSummary(n, logPlus, this.uniqueCount);
            byte[] bytes = before.toByteArray();
            Value value = new Value(bytes);
            final StatsHyperLogSummary after = new StatsHyperLogSummary(value);
            
            log.debug("before(" + before + ")");
            log.debug("after(" + after + ")");
            
            Assert.assertEquals(before, after);
            Assert.assertEquals(0, before.compareTo(after));
            Assert.assertEquals(before.getCount(), after.getCount());
            
            HyperLogLogPlus logPlusBefore = before.getHyperLogPlus();
            HyperLogLogPlus logPlusAfter = after.getHyperLogPlus();
            
            Assert.assertEquals(logPlusBefore.cardinality(), logPlusAfter.cardinality());
            // may not be true for large sample set but for small sample it is correct
            Assert.assertEquals(this.uniqueCount, logPlusAfter.cardinality());
            Assert.assertEquals(this.uniqueCount, after.getUniqueCount());
            Assert.assertEquals(this.uniqueCount, before.getUniqueCount());
        }
    }
    
    /**
     * Randomly populates a {@link HyperLogLogPlus} object.
     */
    private HyperLogLogPlus createHyperLog() {
        Set<String> unique = new HashSet<>();
        HyperLogLogPlus logPlus = new HyperLogLogPlus(StatsJob.HYPERLOG_NORMAL_DEFAULT_VALUE, StatsJob.HYPERLOG_SPARSE_DEFAULT_VALUE);
        this.uniqueCount = rVal.nextInt(MAX_UNIQUE_VALUES - MIN_UNIQUE_VALUES) + MIN_UNIQUE_VALUES;
        for (int n = 0; n < this.uniqueCount;) {
            int len = 4 + rVal.nextInt(10);
            String str = RandomStringUtils.randomAlphabetic(len);
            if (unique.add(str)) {
                logPlus.offer(str);
                n++;
            }
        }
        
        log.debug("unique strings added to hyper log(" + this.uniqueCount + ")");
        
        // add duplicates
        List<String> values = new ArrayList<>(unique);
        int dups = rVal.nextInt(MAX_DUP_VALUES - MIN_DUP_VALUES) + MIN_DUP_VALUES;
        for (int n = 0; n < dups; n++) {
            int idx = rVal.nextInt(values.size());
            logPlus.offer(values.get(idx));
        }
        
        return logPlus;
    }
}
