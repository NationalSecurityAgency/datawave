package datawave.ingest.util.cache.watch;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import datawave.iterators.filter.AgeOffConfigParams;
import datawave.iterators.filter.ageoff.FilterRule;

public class FileRuleWatcherTest {
    private static final String FILTER_CONFIGURATION_FILE = "/test-filter-rules.xml";
    private static final String FILE_WITH_MISSING_FILTER_CLASS = "/missing-filter-class.xml";
    private static final String DEFAULT_UNITS = "d";
    private FileRuleWatcher watcher;
    private FileSystem fs;
    private Path filePath;
    private Collection<FilterRule> rules;
    private Map<String,TestFilter> rulesByMatchPattern;
    
    @Before
    public void before() throws IOException {
        rulesByMatchPattern = new HashMap<>();
        filePath = new Path(this.getClass().getResource(FILTER_CONFIGURATION_FILE).toString());
        fs = filePath.getFileSystem(new Configuration());
        watcher = new FileRuleWatcher(fs, filePath, 1);
        rules = watcher.loadContents(fs.open(filePath));
        Assert.assertEquals(5, rules.size());
        for (FilterRule rule : rules) {
            Assert.assertEquals(TestFilter.class, rule.getClass());
            TestFilter testFilter = (TestFilter) rule;
            String matchPattern = testFilter.options.getOption(AgeOffConfigParams.MATCHPATTERN);
            rulesByMatchPattern.put(matchPattern, testFilter);
        }
    }
    
    @Test
    public void verifyNoBleedOverOfTTlValue() throws IOException {
        Assert.assertEquals(10, rulesByMatchPattern.get("1").options.getTTL());
        Assert.assertEquals(-1, rulesByMatchPattern.get("A").options.getTTL());
        Assert.assertEquals(50, rulesByMatchPattern.get("B").options.getTTL());
        Assert.assertEquals(-1, rulesByMatchPattern.get("C").options.getTTL());
        Assert.assertEquals(10, rulesByMatchPattern.get("D").options.getTTL());
    }
    
    @Test
    public void verifyNoBleedOverOfTTlUnits() throws IOException {
        Assert.assertEquals("ms", rulesByMatchPattern.get("1").options.getTTLUnits());
        Assert.assertEquals(DEFAULT_UNITS, rulesByMatchPattern.get("A").options.getTTLUnits());
        Assert.assertEquals("d", rulesByMatchPattern.get("B").options.getTTLUnits());
        Assert.assertEquals(DEFAULT_UNITS, rulesByMatchPattern.get("C").options.getTTLUnits());
        Assert.assertEquals("ms", rulesByMatchPattern.get("D").options.getTTLUnits());
    }
    
    @Test
    public void verifyNoBleedOverOfExtendedOptions() throws IOException {
        Assert.assertEquals("false", rulesByMatchPattern.get("1").options.getOption("filtersWater"));
        Assert.assertNull(rulesByMatchPattern.get("A").options.getOption("filtersWater"));
        Assert.assertEquals("true", rulesByMatchPattern.get("B").options.getOption("filtersWater"));
        Assert.assertNull(rulesByMatchPattern.get("C").options.getOption("filtersWater"));
        Assert.assertEquals("false", rulesByMatchPattern.get("D").options.getOption("filtersWater"));
        
        Assert.assertEquals("1234", rulesByMatchPattern.get("1").options.getOption("myTagName.ttl"));
        Assert.assertNull(rulesByMatchPattern.get("A").options.getOption("myTagName.ttl"));
        Assert.assertNull(rulesByMatchPattern.get("B").options.getOption("myTagName.ttl"));
        Assert.assertNull(rulesByMatchPattern.get("C").options.getOption("myTagName.ttl"));
        Assert.assertNull(rulesByMatchPattern.get("D").options.getOption("myTagName.ttl"));
    }
    
    @Test(expected = IOException.class)
    public void verifyFilterClass() throws IOException {
        Path fileWithMissingClassname = new Path(this.getClass().getResource(FILE_WITH_MISSING_FILTER_CLASS).toString());
        rules = watcher.loadContents(fs.open(fileWithMissingClassname));
    }
    
    @Test
    public void verifyNumericFieldInOptions() throws IOException {
        // backwards compatibility
        Assert.assertEquals("2468", rulesByMatchPattern.get("D").options.getOption("last.ttl"));
        Assert.assertEquals(10, rulesByMatchPattern.get("D").options.getTTL());
        Assert.assertEquals("ms", rulesByMatchPattern.get("D").options.getTTLUnits());
        // revised options
        Assert.assertEquals("first,last", rulesByMatchPattern.get("D").options.getOption("fields"));
        Assert.assertEquals("1234", rulesByMatchPattern.get("D").options.getOption("field.middle.ttl"));
        Assert.assertEquals("m", rulesByMatchPattern.get("D").options.getOption("field.middle.ttlUnits"));
        Assert.assertEquals("10", rulesByMatchPattern.get("D").options.getOption("field.suffix.ttl"));
        Assert.assertEquals("d", rulesByMatchPattern.get("D").options.getOption("field.suffix.ttlUnits"));
        Assert.assertEquals("77", rulesByMatchPattern.get("D").options.getOption("datatype.012345.ttl"));
        Assert.assertEquals("ms", rulesByMatchPattern.get("D").options.getOption("datatype.012345.ttlUnits"));
    }
}
