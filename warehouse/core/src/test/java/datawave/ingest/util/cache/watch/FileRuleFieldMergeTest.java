package datawave.ingest.util.cache.watch;

import datawave.iterators.filter.ageoff.FilterRule;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests to verify capability of merging configs that use filters that inherit from {@code FieldAgeOffFilter}
 */
public class FileRuleFieldMergeTest {
    private static final String ROOT_FILTER_CONFIGURATION_FILE = "/test-root-field.xml";
    private static final String CHILD_FILTER_CONFIGURATION_FILE = "/test-customized-field.xml";
    
    private FileRuleWatcher watcher;
    private TestFieldFilter parentFilter;
    // this one inherits defaults from parentFilter
    private TestFieldFilter childFilter;
    
    @BeforeEach
    public void before() throws IOException {
        Path childPath = new Path(this.getClass().getResource(CHILD_FILTER_CONFIGURATION_FILE).toString());
        Path rootPath = new Path(this.getClass().getResource(ROOT_FILTER_CONFIGURATION_FILE).toString());
        FileSystem fs = childPath.getFileSystem(new Configuration());
        watcher = new FileRuleWatcher(fs, childPath, 1);
        parentFilter = (TestFieldFilter) loadRulesFromFile(watcher, fs, rootPath);
        childFilter = (TestFieldFilter) loadRulesFromFile(watcher, fs, childPath);
    }
    
    @Test
    public void verifyIsIndexOnlyForChild() throws IOException {
        assertFalse(isIndexTable(parentFilter));
        
        assertTrue(isIndexTable(childFilter));
    }
    
    @Test
    public void verifyInheritedParentConfigs() throws IOException {
        // parent config fields
        // <fields>alpha,beta,gamma,delta</fields>
        // since child is index config, field should be in the column family
        Key key = new Key("row", "alpha", "cq", "vis", 0);
        assertFalse(childFilter.accept(key, new Value()));
        
        key = new Key("row", "beta", "cq", "vis", Long.MAX_VALUE);
        assertTrue(childFilter.accept(key, new Value()));
    }
    
    private Boolean isIndexTable(TestFieldFilter filter) {
        return Boolean.valueOf(filter.options.getOption("isindextable", "false"));
    }
    
    private static FilterRule loadRulesFromFile(FileRuleWatcher watcher, FileSystem fs, Path filePath) throws IOException {
        Collection<FilterRule> rules = watcher.loadContents(fs.open(filePath));
        // should only have the single rule
        assertEquals(rules.size(), 1);
        for (FilterRule rule : rules) {
            assertEquals(TestFieldFilter.class, rule.getClass());
        }
        return rules.iterator().next();
    }
}
