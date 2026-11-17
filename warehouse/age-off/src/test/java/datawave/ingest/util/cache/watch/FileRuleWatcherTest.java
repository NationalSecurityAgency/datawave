package datawave.ingest.util.cache.watch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.iterators.filter.AgeOffConfigParams;
import datawave.iterators.filter.ageoff.FilterRule;

public class FileRuleWatcherTest {
    private static final String FILTER_CONFIGURATION_FILE = "/filter/test-filter-rules.xml";
    private static final String FILE_WITH_MISSING_FILTER_CLASS = "/filter/missing-filter-class.xml";
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
        assertEquals(5, rules.size());
        for (FilterRule rule : rules) {
            assertEquals(TestFilter.class, rule.getClass());
            TestFilter testFilter = (TestFilter) rule;
            String matchPattern = testFilter.options.getOption(AgeOffConfigParams.MATCHPATTERN);
            rulesByMatchPattern.put(matchPattern, testFilter);
        }
    }

    @Test
    public void verifyNoBleedOverOfTTlValue() throws IOException {
        assertEquals(10, rulesByMatchPattern.get("1").options.getTTL());
        assertEquals(-1, rulesByMatchPattern.get("A").options.getTTL());
        assertEquals(50, rulesByMatchPattern.get("B").options.getTTL());
        assertEquals(-1, rulesByMatchPattern.get("C").options.getTTL());
        assertEquals(10, rulesByMatchPattern.get("D").options.getTTL());
    }

    @Test
    public void verifyNoBleedOverOfTTlUnits() throws IOException {
        assertEquals("ms", rulesByMatchPattern.get("1").options.getTTLUnits());
        assertEquals(DEFAULT_UNITS, rulesByMatchPattern.get("A").options.getTTLUnits());
        assertEquals("d", rulesByMatchPattern.get("B").options.getTTLUnits());
        assertEquals(DEFAULT_UNITS, rulesByMatchPattern.get("C").options.getTTLUnits());
        assertEquals("ms", rulesByMatchPattern.get("D").options.getTTLUnits());
    }

    @Test
    public void verifyNoBleedOverOfExtendedOptions() throws IOException {
        assertEquals("false", rulesByMatchPattern.get("1").options.getOption("filtersWater"));
        assertNull(rulesByMatchPattern.get("A").options.getOption("filtersWater"));
        assertEquals("true", rulesByMatchPattern.get("B").options.getOption("filtersWater"));
        assertNull(rulesByMatchPattern.get("C").options.getOption("filtersWater"));
        assertEquals("false", rulesByMatchPattern.get("D").options.getOption("filtersWater"));

        assertEquals("1234", rulesByMatchPattern.get("1").options.getOption("myTagName.ttl"));
        assertNull(rulesByMatchPattern.get("A").options.getOption("myTagName.ttl"));
        assertNull(rulesByMatchPattern.get("B").options.getOption("myTagName.ttl"));
        assertNull(rulesByMatchPattern.get("C").options.getOption("myTagName.ttl"));
        assertNull(rulesByMatchPattern.get("D").options.getOption("myTagName.ttl"));
    }

    @Test
    public void verifyFilterClass() throws IOException {
        Path fileWithMissingClassname = new Path(this.getClass().getResource(FILE_WITH_MISSING_FILTER_CLASS).toString());
        assertThrows(IOException.class, () -> watcher.loadContents(fs.open(fileWithMissingClassname)));
    }

    @Test
    public void verifyNumericFieldInOptions() throws IOException {
        // backwards compatibility
        assertEquals("2468", rulesByMatchPattern.get("D").options.getOption("last.ttl"));
        assertEquals(10, rulesByMatchPattern.get("D").options.getTTL());
        assertEquals("ms", rulesByMatchPattern.get("D").options.getTTLUnits());
        // revised options
        assertEquals("first,last", rulesByMatchPattern.get("D").options.getOption("fields"));
        assertEquals("1234", rulesByMatchPattern.get("D").options.getOption("field.middle.ttl"));
        assertEquals("m", rulesByMatchPattern.get("D").options.getOption("field.middle.ttlUnits"));
        assertEquals("10", rulesByMatchPattern.get("D").options.getOption("field.suffix.ttl"));
        assertEquals("d", rulesByMatchPattern.get("D").options.getOption("field.suffix.ttlUnits"));
        assertEquals("77", rulesByMatchPattern.get("D").options.getOption("datatype.012345.ttl"));
        assertEquals("ms", rulesByMatchPattern.get("D").options.getOption("datatype.012345.ttlUnits"));
    }
}
