package datawave.ingest.util.cache.watch;

import datawave.iterators.filter.AgeOffConfigParams;
import datawave.iterators.filter.ageoff.FilterRule;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class FileRuleWatcherTest {
    private static final String FILTER_CONFIGURATION_FILE = "/test-filter-rules.xml";
    private static final String FILE_WITH_MISSING_FILTER_CLASS = "/missing-filter-class.xml";
    private static final String DEFAULT_UNITS = "d";
    private FileRuleWatcher watcher;
    private FileSystem fs;
    private Path filePath;
    private Collection<FilterRule> rules;
    private Map<String,TestFilter> rulesByMatchPattern;
    
    @BeforeEach
    public void before() throws IOException {
        rulesByMatchPattern = new HashMap<>();
        filePath = new Path(this.getClass().getResource(FILTER_CONFIGURATION_FILE).toString());
        fs = filePath.getFileSystem(new Configuration());
        watcher = new FileRuleWatcher(fs, filePath, 1);
        rules = watcher.loadContents(fs.open(filePath));
        Assertions.assertEquals(5, rules.size());
        for (FilterRule rule : rules) {
            Assertions.assertEquals(TestFilter.class, rule.getClass());
            TestFilter testFilter = (TestFilter) rule;
            String matchPattern = testFilter.options.getOption(AgeOffConfigParams.MATCHPATTERN);
            rulesByMatchPattern.put(matchPattern, testFilter);
        }
    }
    
    @Test
    public void verifyNoBleedOverOfTTlValue() throws IOException {
        Assertions.assertEquals(10, rulesByMatchPattern.get("1").options.getTTL());
        Assertions.assertEquals(-1, rulesByMatchPattern.get("A").options.getTTL());
        Assertions.assertEquals(50, rulesByMatchPattern.get("B").options.getTTL());
        Assertions.assertEquals(-1, rulesByMatchPattern.get("C").options.getTTL());
        Assertions.assertEquals(10, rulesByMatchPattern.get("D").options.getTTL());
    }
    
    @Test
    public void verifyNoBleedOverOfTTlUnits() throws IOException {
        Assertions.assertEquals("ms", rulesByMatchPattern.get("1").options.getTTLUnits());
        Assertions.assertEquals(DEFAULT_UNITS, rulesByMatchPattern.get("A").options.getTTLUnits());
        Assertions.assertEquals("d", rulesByMatchPattern.get("B").options.getTTLUnits());
        Assertions.assertEquals(DEFAULT_UNITS, rulesByMatchPattern.get("C").options.getTTLUnits());
        Assertions.assertEquals("ms", rulesByMatchPattern.get("D").options.getTTLUnits());
    }
    
    @Test
    public void verifyNoBleedOverOfExtendedOptions() throws IOException {
        Assertions.assertEquals("false", rulesByMatchPattern.get("1").options.getOption("filtersWater"));
        Assertions.assertNull(rulesByMatchPattern.get("A").options.getOption("filtersWater"));
        Assertions.assertEquals("true", rulesByMatchPattern.get("B").options.getOption("filtersWater"));
        Assertions.assertNull(rulesByMatchPattern.get("C").options.getOption("filtersWater"));
        Assertions.assertEquals("false", rulesByMatchPattern.get("D").options.getOption("filtersWater"));
        
        Assertions.assertEquals("1234", rulesByMatchPattern.get("1").options.getOption("myTagName.ttl"));
        Assertions.assertNull(rulesByMatchPattern.get("A").options.getOption("myTagName.ttl"));
        Assertions.assertNull(rulesByMatchPattern.get("B").options.getOption("myTagName.ttl"));
        Assertions.assertNull(rulesByMatchPattern.get("C").options.getOption("myTagName.ttl"));
        Assertions.assertNull(rulesByMatchPattern.get("D").options.getOption("myTagName.ttl"));
    }
    
    @Test
    public void verifyFilterClass() throws IOException {
        Path fileWithMissingClassname = new Path(this.getClass().getResource(FILE_WITH_MISSING_FILTER_CLASS).toString());
        Assertions.assertThrows(IOException.class, () -> watcher.loadContents(fs.open(fileWithMissingClassname)));
    }
    
    @Test
    public void verifyNumericFieldInOptions() throws IOException {
        // backwards compatibility
        Assertions.assertEquals("2468", rulesByMatchPattern.get("D").options.getOption("last.ttl"));
        Assertions.assertEquals(10, rulesByMatchPattern.get("D").options.getTTL());
        Assertions.assertEquals("ms", rulesByMatchPattern.get("D").options.getTTLUnits());
        // revised options
        Assertions.assertEquals("first,last", rulesByMatchPattern.get("D").options.getOption("fields"));
        Assertions.assertEquals("1234", rulesByMatchPattern.get("D").options.getOption("field.middle.ttl"));
        Assertions.assertEquals("m", rulesByMatchPattern.get("D").options.getOption("field.middle.ttlUnits"));
        Assertions.assertEquals("10", rulesByMatchPattern.get("D").options.getOption("field.suffix.ttl"));
        Assertions.assertEquals("d", rulesByMatchPattern.get("D").options.getOption("field.suffix.ttlUnits"));
        Assertions.assertEquals("77", rulesByMatchPattern.get("D").options.getOption("datatype.012345.ttl"));
        Assertions.assertEquals("ms", rulesByMatchPattern.get("D").options.getOption("datatype.012345.ttlUnits"));
    }
}
