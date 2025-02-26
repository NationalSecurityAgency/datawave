package datawave.ingest.util.cache.watch;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import datawave.iterators.filter.ageoff.FilterRule;

/**
 * Tests to verify capability of merging configs that use filters that inherit from {@code FieldAgeOffFilter}
 */
public class FileRuleFieldMergeTest {
    static final String ROOT_FILTER_CONFIGURATION_FILE = "/filter/test-root-field.xml";
    private static final String CHILD_FILTER_CONFIGURATION_FILE = "/filter/test-customized-field.xml";

    private TestFieldFilter parentFilter;
    // this one inherits defaults from parentFilter
    private TestFieldFilter childFilter;

    @Before
    public void before() throws IOException {
        Path childPath = new Path(this.getClass().getResource(CHILD_FILTER_CONFIGURATION_FILE).toString());
        Path rootPath = new Path(this.getClass().getResource(ROOT_FILTER_CONFIGURATION_FILE).toString());
        FileSystem fs = childPath.getFileSystem(new Configuration());
        parentFilter = (TestFieldFilter) loadRulesFromFile(fs, rootPath);
        childFilter = (TestFieldFilter) loadRulesFromFile(fs, childPath);
    }

    @Test
    public void verifyIsIndexOnlyForChild() throws IOException {
        assertEquals(isIndexTable(parentFilter), false);

        assertEquals(isIndexTable(childFilter), true);
    }

    @Test
    public void verifyInheritedParentConfigs() throws IOException {
        // parent config fields
        // <fields>alpha,beta,gamma,delta</fields>
        // since child is index config, field should be in the column family
        Key key = new Key("row", "alpha", "cq", "vis", 0);
        assertEquals(childFilter.accept(key, new Value()), false);

        key = new Key("row", "beta", "cq", "vis", Long.MAX_VALUE);
        assertEquals(childFilter.accept(key, new Value()), true);
    }

    private Boolean isIndexTable(TestFieldFilter filter) {
        return Boolean.valueOf(filter.options.getOption("isindextable", "false"));
    }

    private static FilterRule loadRulesFromFile(FileSystem fs, Path filePath) throws IOException {
        FileRuleCacheValue cacheValue = new FileRuleCacheValue(fs, filePath, 1);
        Collection<FilterRule> rules = cacheValue.loadFilterRules(null);
        // should only have the single rule
        assertEquals(rules.size(), 1);
        for (FilterRule rule : rules) {
            assertEquals(TestFieldFilter.class, rule.getClass());
        }
        return rules.iterator().next();
    }
}
