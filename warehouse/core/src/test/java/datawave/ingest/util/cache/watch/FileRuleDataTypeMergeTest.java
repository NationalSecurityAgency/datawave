package datawave.ingest.util.cache.watch;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import datawave.iterators.filter.ageoff.FilterRule;

/**
 * Tests to verify capability of merging configs that use filters that inherit from {@code FieldAgeOffFilter}
 */
public class FileRuleDataTypeMergeTest {
    private static final String ROOT_FILTER_CONFIGURATION_FILE = "/test-root-data-type.xml";
    private static final String CHILD_FILTER_CONFIGURATION_FILE = "/test-customized-data-type.xml";
    
    private FileRuleWatcher watcher;
    private TestDataTypeFilter parentFilter;
    // this one inherits defaults from parentFilter
    private TestDataTypeFilter childFilter;
    
    @Before
    public void before() throws IOException {
        Path childPath = new Path(this.getClass().getResource(CHILD_FILTER_CONFIGURATION_FILE).toString());
        Path rootPath = new Path(this.getClass().getResource(ROOT_FILTER_CONFIGURATION_FILE).toString());
        FileSystem fs = childPath.getFileSystem(new Configuration());
        watcher = new FileRuleWatcher(fs, childPath, 1);
        parentFilter = (TestDataTypeFilter) loadRulesFromFile(watcher, fs, rootPath);
        childFilter = (TestDataTypeFilter) loadRulesFromFile(watcher, fs, childPath);
    }
    
    @Test
    public void verifyOverridenValues() throws IOException {
        // there are 4 rules, 2 are overridden
        //
        // defaults are
        //
        // <ttl units="ms">1000</ttl>
        // <datatypes>foo,bar,baz,zip</datatypes>
        // <foo ttl="600"/>
        // <bar ttl="500"/>
        // <baz ttl="400"/>
        //
        // overrides are
        // <foo ttl="123"/>
        // <zip ttl="123"/>
        
        // verify original values
        assertThat(parentFilter.options.getOption("foo.ttl"), is("600"));
        assertThat(parentFilter.options.getOption("bar.ttl"), is("500"));
        assertThat(parentFilter.options.getOption("baz.ttl"), is("400"));
        
        // check overrides
        assertThat(childFilter.options.getOption("bar.ttl"), is("500"));
        assertThat(childFilter.options.getOption("baz.ttl"), is("400"));
        // these are overridden
        assertThat(childFilter.options.getOption("foo.ttl"), is("123"));
        assertThat(childFilter.options.getOption("zip.ttl"), is("123"));
    }
    
    private static FilterRule loadRulesFromFile(FileRuleWatcher watcher, FileSystem fs, Path filePath) throws IOException {
        Collection<FilterRule> rules = watcher.loadContents(fs.open(filePath));
        // should only have the single rule
        assertThat(rules.size(), is(1));
        for (FilterRule rule : rules) {
            assertEquals(TestDataTypeFilter.class, rule.getClass());
        }
        return rules.iterator().next();
    }
}
