package datawave.ingest.util.cache.watch;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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

    @Before
    public void before() throws IOException {
        rulesByMatchPattern = new HashMap<>();
        filePath = new Path(this.getClass().getResource(FILTER_CONFIGURATION_FILE).toString());
        fs = filePath.getFileSystem(new Configuration());
        ruleValue = new FileRuleCacheValue(fs, filePath, 1);
        rules = ruleValue.loadFilterRules(null);
        Assert.assertEquals(5, rules.size());
        for (FilterRule rule : rules) {
            Assert.assertEquals(TestFilter.class, rule.getClass());
            TestFilter testFilter = (TestFilter) rule;
            String matchPattern = testFilter.options.getOption(AgeOffConfigParams.MATCHPATTERN);
            rulesByMatchPattern.put(matchPattern, testFilter);
        }
    }

    @Test
    public void verifyNoBleedOverOfTTlValue() {
        Assert.assertEquals(10, rulesByMatchPattern.get("1").options.getTTL());
        Assert.assertEquals(-1, rulesByMatchPattern.get("A").options.getTTL());
        Assert.assertEquals(50, rulesByMatchPattern.get("B").options.getTTL());
        Assert.assertEquals(-1, rulesByMatchPattern.get("C").options.getTTL());
        Assert.assertEquals(10, rulesByMatchPattern.get("D").options.getTTL());
    }

    @Test
    public void verifyNoBleedOverOfTTlUnits() {
        Assert.assertEquals("ms", rulesByMatchPattern.get("1").options.getTTLUnits());
        Assert.assertEquals(DEFAULT_UNITS, rulesByMatchPattern.get("A").options.getTTLUnits());
        Assert.assertEquals("d", rulesByMatchPattern.get("B").options.getTTLUnits());
        Assert.assertEquals(DEFAULT_UNITS, rulesByMatchPattern.get("C").options.getTTLUnits());
        Assert.assertEquals("ms", rulesByMatchPattern.get("D").options.getTTLUnits());
    }

    @Test
    public void verifyNoBleedOverOfExtendedOptions() {
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

    @Test
    public void verifyDeepCopyWillSeeDifferentRules() throws IOException {
        Collection<AppliedRule> v1 = ruleValue.newRulesetView(0, null);
        FileRuleReference r1 = ruleValue.getRuleRef();
        Collection<AppliedRule> v2 = ruleValue.newRulesetView(0, null);
        FileRuleReference r2 = ruleValue.getRuleRef();

        // check to ensure the applied rules returned are different objects
        // but the underlying rule reference is not changing
        Assert.assertNotSame(v1, v2);
        Assert.assertSame(r1, r2);
    }

    @Test(expected = IOException.class)
    public void verifyFilterClass() throws IOException {
        Path fileWithMissingClassname = new Path(this.getClass().getResource(FILE_WITH_MISSING_FILTER_CLASS).toString());
        FileRuleCacheValue exValue = new FileRuleCacheValue(fs, fileWithMissingClassname, 1);
        rules = exValue.loadFilterRules(null);
    }

    @Test
    public void verifyNumericFieldInOptions() {
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

    @Test
    public void verifyHasChangesIfNotInitializedReturnsChanges() {
        FileSystem fs = Mockito.mock(FileSystem.class);
        Path path = new Path("hdfs://path/to/file");
        FileRuleCacheValue val = new FileRuleCacheValue(fs, path, 1L);
        Assert.assertTrue(val.hasChanges());
    }

    @Test
    public void verifyHasChangesIfThrowsReturnsChanges() throws IOException {
        FileSystem fsSpy = Mockito.spy(fs);
        Mockito.when(fsSpy.getFileStatus(filePath)).thenThrow(new IllegalStateException("Unable to fetch status"));
        FileRuleCacheValue val = new FileRuleCacheValue(fs, filePath, 1L);
        Assert.assertTrue(val.hasChanges());
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

        Assert.assertNotNull(val.getRuleRef());

        // assert no changes after loading
        Assert.assertFalse("Expected no changes initial evaluation", val.hasChanges());

        // reset status to be unchanged (more than baseline)
        Mockito.when(fsSpy.getFileStatus(filePath)).thenReturn(statusUnchanged);

        // assert no changes after loading
        Assert.assertFalse("Expected no changes at threshold evaluation", val.hasChanges());

        // reset status to be changed
        Mockito.when(fsSpy.getFileStatus(filePath)).thenReturn(statusChanged);

        // assert changes now detected when (modificationTime - baseline) > configuredDiff
        Assert.assertTrue("Expected evaluation has changes", val.hasChanges());
    }
}
