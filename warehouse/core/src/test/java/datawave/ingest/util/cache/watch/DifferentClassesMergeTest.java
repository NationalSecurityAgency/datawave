package datawave.ingest.util.cache.watch;

import datawave.iterators.filter.ColumnVisibilityLabeledFilter;
import datawave.iterators.filter.EdgeColumnQualifierTokenFilter;
import datawave.iterators.filter.ageoff.AppliedRule;
import datawave.iterators.filter.ageoff.FilterRule;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests to verify capability of merging configs for rules that use different filters with overlapping matchPattern formats
 */
public class DifferentClassesMergeTest {
    private static final String ROOT_FILTER_CONFIGURATION_FILE = "/alternate-root.xml";
    private static final String CHILD_FILTER_CONFIGURATION_FILE = "/alternate-child.xml";
    private static final long VERY_OLD_TIMESTAMP = -1000L * 60 * 60 * 24 * 365 * 1000; // 1,000 years in the past
    private static final long TIMESTAMP_IN_FUTURE = 1000L * 60 * 60 * 24 * 365 * 1000; // 1,000 years in the future
    
    private FileRuleWatcher watcher;
    private ColumnVisibilityLabeledFilter parentFilter;
    // childFilter inherits matchPattern contents from parentFilter
    private EdgeColumnQualifierTokenFilter childFilter;
    
    @Before
    public void before() throws IOException {
        Path childPath = new Path(this.getClass().getResource(CHILD_FILTER_CONFIGURATION_FILE).toString());
        Path rootPath = new Path(this.getClass().getResource(ROOT_FILTER_CONFIGURATION_FILE).toString());
        FileSystem fs = childPath.getFileSystem(new Configuration());
        watcher = new FileRuleWatcher(fs, childPath, 1);
        parentFilter = (ColumnVisibilityLabeledFilter) loadRulesFromFile(watcher, fs, rootPath);
        childFilter = (EdgeColumnQualifierTokenFilter) loadRulesFromFile(watcher, fs, childPath);
    }
    
    @Test
    public void verifyInheritedParentConfigs() throws IOException {
        Key key = new Key("row", "cf", "coffeeGround", "coffeeGround", VERY_OLD_TIMESTAMP);
        assertThat(parentFilter.accept(key, new Value()), is(false));
        
        key = new Key("row", "cf", "2/coffeeGround/chocolate", "coffeeGround&chocolate", TIMESTAMP_IN_FUTURE);
        assertThat(parentFilter.accept(key, new Value()), is(true));
        
        key = new Key("row", "cf", "2/coffeeGround/chocolate", "coffeeGround&chocolate", VERY_OLD_TIMESTAMP);
        assertThat(childFilter.accept(key, new Value()), is(false));
        
        key = new Key("row", "cf", "2/coffeeGround/chocolate", "coffeeGround&chocolate", TIMESTAMP_IN_FUTURE);
        assertThat(childFilter.accept(key, new Value()), is(true));
    }
    
    @Test
    public void verifyOverrides() throws IOException {
        Key key = new Key("row", "cf", "1/bakingPowder/chocolate", "bakingPowder&chocolate", VERY_OLD_TIMESTAMP);
        assertThat(childFilter.accept(key, new Value()), is(false));
        
        // key = new Key("row", "cf", "1/bakingPowder/chocolate", "bakingPowder&chocolate", System.currentTimeMillis() - 1000*60*60*24*366);
        key = new Key("row", "cf", "1/bakingPowder/chocolate", "bakingPowder&chocolate", -1000L * 60 * 60 * 24 * 370);
        assertThat(childFilter.accept(key, new Value()), is(true));
        
        key = new Key("row", "cf", "1/bakingPowder/chocolate", "bakingPowder&chocolate", -1000L * 60 * 60 * 24 * 550);
        // key = new Key("row", "cf", "1/bakingPowder/chocolate", "bakingPowder&chocolate", System.currentTimeMillis() - 1000*60*60*24*549);
        assertThat(childFilter.accept(key, new Value()), is(false));
    }
    
    private static AppliedRule loadRulesFromFile(FileRuleWatcher watcher, FileSystem fs, Path filePath) throws IOException {
        Collection<FilterRule> rules = watcher.loadContents(fs.open(filePath));
        // should only have the single rule
        assertThat(rules.size(), is(1));
        return (AppliedRule) rules.iterator().next();
    }
}
