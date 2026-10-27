package datawave.ingest.util.cache.watch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import datawave.iterators.filter.AgeOffConfigParams;
import datawave.iterators.filter.ageoff.AppliedRule;
import datawave.iterators.filter.ageoff.FilterRule;

public class FileRuleCacheValueTest {
    // Derived from original FileRuleWatcherTest unit tests

    private static final String FILTER_CONFIGURATION_FILE = "/filter/test-filter-rules.xml";
    private static final String FILE_WITH_MISSING_FILTER_CLASS = "/filter/missing-filter-class.xml";
    private static final String DEFAULT_UNITS = "d";
    private FileRuleCacheValue ruleValue;
    private FileSystem fs;
    private Path filePath;
    private Collection<FilterRule> rules;
    private Map<String,TestFilter> rulesByMatchPattern;

    @BeforeEach
    public void before() throws IOException {
        rulesByMatchPattern = new HashMap<>();
        filePath = new Path(this.getClass().getResource(FILTER_CONFIGURATION_FILE).toString());
        fs = filePath.getFileSystem(new Configuration());
        ruleValue = new FileRuleCacheValue(fs, filePath, 1);
        rules = ruleValue.loadFilterRules(null);
        assertEquals(5, rules.size());
        for (FilterRule rule : rules) {
            assertEquals(TestFilter.class, rule.getClass());
            TestFilter testFilter = (TestFilter) rule;
            String matchPattern = testFilter.options.getOption(AgeOffConfigParams.MATCHPATTERN);
            rulesByMatchPattern.put(matchPattern, testFilter);
        }
    }

    @Test
    public void verifyNoBleedOverOfTTlValue() {
        assertEquals(10, rulesByMatchPattern.get("1").options.getTTL());
        assertEquals(-1, rulesByMatchPattern.get("A").options.getTTL());
        assertEquals(50, rulesByMatchPattern.get("B").options.getTTL());
        assertEquals(-1, rulesByMatchPattern.get("C").options.getTTL());
        assertEquals(10, rulesByMatchPattern.get("D").options.getTTL());
    }

    @Test
    public void verifyNoBleedOverOfTTlUnits() {
        assertEquals("ms", rulesByMatchPattern.get("1").options.getTTLUnits());
        assertEquals(DEFAULT_UNITS, rulesByMatchPattern.get("A").options.getTTLUnits());
        assertEquals("d", rulesByMatchPattern.get("B").options.getTTLUnits());
        assertEquals(DEFAULT_UNITS, rulesByMatchPattern.get("C").options.getTTLUnits());
        assertEquals("ms", rulesByMatchPattern.get("D").options.getTTLUnits());
    }

    @Test
    public void verifyNoBleedOverOfExtendedOptions() {
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
    public void verifyDeepCopyWillSeeDifferentRules() throws IOException {
        Collection<AppliedRule> v1 = ruleValue.newRulesetView(0, null);
        FileRuleReference r1 = ruleValue.getRuleRef();
        Collection<AppliedRule> v2 = ruleValue.newRulesetView(0, null);
        FileRuleReference r2 = ruleValue.getRuleRef();

        // check to ensure the applied rules returned are different objects
        // but the underlying rule reference is not changing
        assertNotSame(v1, v2);
        assertSame(r1, r2);
    }

    @Test
    public void verifyFilterClass() throws IOException {
        Path fileWithMissingClassname = new Path(this.getClass().getResource(FILE_WITH_MISSING_FILTER_CLASS).toString());
        FileRuleCacheValue exValue = new FileRuleCacheValue(fs, fileWithMissingClassname, 1);
        assertThrows(IOException.class, () -> exValue.loadFilterRules(null));
    }

    @Test
    public void verifyNumericFieldInOptions() {
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

    @Test
    public void verifyHasChangesIfNotInitializedReturnsChanges() {
        FileSystem fs = Mockito.mock(FileSystem.class);
        Path path = new Path("hdfs://path/to/file");
        FileRuleCacheValue val = new FileRuleCacheValue(fs, path, 1L);
        assertTrue(val.hasChanges());
    }

    @Test
    public void verifyHasChangesIfThrowsReturnsChanges() throws IOException {
        FileSystem fsSpy = Mockito.spy(fs);
        Mockito.when(fsSpy.getFileStatus(filePath)).thenThrow(new IllegalStateException("Unable to fetch status"));
        FileRuleCacheValue val = new FileRuleCacheValue(fs, filePath, 1L);
        assertTrue(val.hasChanges());
    }

    @Test
    public void verifyHasChangesWhenChanges() throws IOException {
        long timestampBaseline = 0L;
        long configuredDiff = 1L;
        FileSystem fsSpy = Mockito.spy(fs);
        FileStatus statusBase = Mockito.mock(FileStatus.class);
        FileStatus statusUnchanged = Mockito.mock(FileStatus.class);
        FileStatus statusChanged = Mockito.mock(FileStatus.class);
        Mockito.when(statusBase.getModificationTime()).thenReturn(timestampBaseline);
        Mockito.when(statusUnchanged.getModificationTime()).thenReturn(timestampBaseline + configuredDiff);
        Mockito.when(statusChanged.getModificationTime()).thenReturn(timestampBaseline + configuredDiff + 1);
        Mockito.when(fsSpy.getFileStatus(filePath)).thenReturn(statusBase);

        FileRuleCacheValue val = new FileRuleCacheValue(fsSpy, filePath, configuredDiff);
        val.newRulesetView(0L, null);

        assertNotNull(val.getRuleRef());

        // assert no changes after loading
        assertFalse(val.hasChanges(), "Expected no changes initial evaluation");

        // reset status to be unchanged (more than baseline)
        Mockito.when(fsSpy.getFileStatus(filePath)).thenReturn(statusUnchanged);

        // assert no changes after loading
        assertFalse(val.hasChanges(), "Expected no changes at threshold evaluation");

        // reset status to be changed
        Mockito.when(fsSpy.getFileStatus(filePath)).thenReturn(statusChanged);

        // assert changes now detected when (modificationTime - baseline) > configuredDiff
        assertTrue(val.hasChanges(), "Expected evaluation has changes");
    }
}
