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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests to verify capability of merging configs for rules that use different filters with overlapping matchPattern formats
 */
public class DifferentClassesMergeTest {
    private static final String ROOT_FILTER_CONFIGURATION_FILE = "/alternate-root.xml";
    private static final String CHILD_FILTER_CONFIGURATION_FILE = "/alternate-child.xml";
    private static final long VERY_OLD_TIMESTAMP = -1000L * 60 * 60 * 24 * 365 * 1000; // 1,000 years in the past
    private static final long TIMESTAMP_IN_FUTURE = 1000L * 60 * 60 * 24 * 365 * 1000; // 1,000 years in the future
    public static final long DAYS_AGO = -1000L * 60 * 60 * 24;
    
    private FileRuleWatcher watcher;
    private ColumnVisibilityLabeledFilter parentFilter;
    // childFilter inherits matchPattern contents from parentFilter
    private EdgeColumnQualifierTokenFilter childFilter;
    
    @Before
    public void before() throws IOException {
        // create childFilter
        Path childPath = new Path(this.getClass().getResource(CHILD_FILTER_CONFIGURATION_FILE).toString());
        FileSystem fs = childPath.getFileSystem(new Configuration());
        watcher = new FileRuleWatcher(fs, childPath, 1);
        childFilter = (EdgeColumnQualifierTokenFilter) loadRulesFromFile(watcher, fs, childPath);
        
        // create parentFilter
        Path rootPath = new Path(this.getClass().getResource(ROOT_FILTER_CONFIGURATION_FILE).toString());
        parentFilter = (ColumnVisibilityLabeledFilter) loadRulesFromFile(watcher, fs, rootPath);
    }
    
    @Test
    public void inspectBakingPowder() {
        // parent: set to 365d
        String colVis = "bakingPowder&chocolate";
        assertParentAccepts(colVis, TIMESTAMP_IN_FUTURE);
        assertParentAccepts(colVis, 1 * DAYS_AGO);
        assertParentAccepts(colVis, 364 * DAYS_AGO);
        assertParentRejects(colVis, 365 * DAYS_AGO);
        assertParentRejects(colVis, 550 * DAYS_AGO);
        assertParentRejects(colVis, VERY_OLD_TIMESTAMP);
        
        // child: overrides to 548d
        String colQual = "1/bakingPowder/chocolate";
        assertChildAccepts(colQual, TIMESTAMP_IN_FUTURE);
        assertChildAccepts(colQual, 370 * DAYS_AGO);
        assertChildAccepts(colQual, 547 * DAYS_AGO);
        assertChildRejects(colQual, 548 * DAYS_AGO);
        assertChildRejects(colQual, 570 * DAYS_AGO);
        assertChildRejects(colQual, VERY_OLD_TIMESTAMP);
    }
    
    @Test
    public void inspectDriedBeans() {
        // parent: set to 548d
        String colVis = "driedBeans&chocolate";
        assertParentAccepts(colVis, TIMESTAMP_IN_FUTURE);
        assertParentAccepts(colVis, 80 * DAYS_AGO);
        assertParentAccepts(colVis, 547 * DAYS_AGO);
        assertParentRejects(colVis, 548 * DAYS_AGO);
        assertParentRejects(colVis, 550 * DAYS_AGO);
        assertParentRejects(colVis, VERY_OLD_TIMESTAMP);
        
        // child: overrides to 90d
        String colQual = "1/driedBeans/chocolate";
        assertChildAccepts(colQual, TIMESTAMP_IN_FUTURE);
        assertChildAccepts(colQual, 7 * DAYS_AGO);
        assertChildAccepts(colQual, 89 * DAYS_AGO);
        assertChildRejects(colQual, 90 * DAYS_AGO);
        assertChildRejects(colQual, 100 * DAYS_AGO);
        assertChildRejects(colQual, VERY_OLD_TIMESTAMP);
    }
    
    @Test
    public void inspectBakingSoda() {
        // parent: set to 720d
        String colVis = "bakingSoda&chocolate";
        assertParentAccepts(colVis, TIMESTAMP_IN_FUTURE);
        assertParentAccepts(colVis, 80 * DAYS_AGO);
        assertParentAccepts(colVis, 100 * DAYS_AGO);
        assertParentAccepts(colVis, 370 * DAYS_AGO);
        assertParentAccepts(colVis, 550 * DAYS_AGO);
        assertParentRejects(colVis, 750 * DAYS_AGO);
        assertParentRejects(colVis, VERY_OLD_TIMESTAMP);
        
        // child: overrides to 365d
        String colQual = "1/bakingSoda/chocolate";
        assertChildAccepts(colQual, TIMESTAMP_IN_FUTURE);
        assertChildAccepts(colQual, 80 * DAYS_AGO);
        assertChildAccepts(colQual, 350 * DAYS_AGO);
        assertChildRejects(colQual, 400 * DAYS_AGO);
        assertChildRejects(colQual, VERY_OLD_TIMESTAMP);
    }
    
    @Test
    public void inspectCoffeeGrounds() {
        // parent: set to 90d
        assertParentRejects("coffeeGround&chocolate", VERY_OLD_TIMESTAMP);
        assertParentAccepts("coffeeGround&chocolate", TIMESTAMP_IN_FUTURE);
        
        // child: inherits parent's 90d ttl, no override
        assertChildRejects("2/coffeeGround/chocolate", VERY_OLD_TIMESTAMP);
        assertChildAccepts("2/coffeeGround/chocolate", TIMESTAMP_IN_FUTURE);
    }
    
    @Test
    public void inspectCoffeeWholeBean() {
        // parent: set to 183d
        String colVis = "coffeeWholeBean&chocolate";
        assertParentAccepts(colVis, TIMESTAMP_IN_FUTURE);
        assertParentAccepts(colVis, 90 * DAYS_AGO);
        assertParentAccepts(colVis, 182 * DAYS_AGO);
        assertParentRejects(colVis, 183 * DAYS_AGO);
        assertParentRejects(colVis, 365 * DAYS_AGO);
        assertParentRejects(colVis, VERY_OLD_TIMESTAMP);
        
        // child: inherits from parent, no override: 183d
        String colQual = "1/coffeeWholeBean/chocolate";
        assertChildAccepts(colQual, TIMESTAMP_IN_FUTURE);
        assertChildAccepts(colQual, 90 * DAYS_AGO);
        assertChildAccepts(colQual, 182 * DAYS_AGO);
        assertChildRejects(colQual, 183 * DAYS_AGO);
        assertChildRejects(colQual, 365 * DAYS_AGO);
        assertChildRejects(colQual, VERY_OLD_TIMESTAMP);
    }
    
    @Test
    public void inspectCoffeeInstant() {
        // parent: set to 730d
        String colVis = "coffeeInstant&chocolate";
        assertParentAccepts(colVis, TIMESTAMP_IN_FUTURE);
        assertParentAccepts(colVis, 90 * DAYS_AGO);
        assertParentAccepts(colVis, 365 * DAYS_AGO);
        assertParentAccepts(colVis, 729 * DAYS_AGO);
        assertParentRejects(colVis, 730 * DAYS_AGO);
        assertParentRejects(colVis, 1365 * DAYS_AGO);
        assertParentRejects(colVis, VERY_OLD_TIMESTAMP);
        
        // child override: 365d
        String colQual = "1/coffeeInstant/chocolate";
        assertChildAccepts(colQual, TIMESTAMP_IN_FUTURE);
        assertChildAccepts(colQual, 90 * DAYS_AGO);
        assertChildAccepts(colQual, 364 * DAYS_AGO);
        assertChildRejects(colQual, 365 * DAYS_AGO);
        assertChildRejects(colQual, 1830 * DAYS_AGO);
        assertChildRejects(colQual, VERY_OLD_TIMESTAMP);
    }
    
    @Test
    public void inspectMysteryMeat() {
        // parent: doesn't have this property
        String colVis = "mysteryMeat&chocolate";
        assertChildAccepts(colVis, TIMESTAMP_IN_FUTURE);
        assertChildAccepts(colVis, 90 * DAYS_AGO);
        assertChildAccepts(colVis, 365 * DAYS_AGO);
        assertChildAccepts(colVis, 729 * DAYS_AGO);
        assertChildAccepts(colVis, 730 * DAYS_AGO);
        assertChildAccepts(colVis, 1365 * DAYS_AGO);
        assertChildAccepts(colVis, VERY_OLD_TIMESTAMP);
        
        // child override: 365d
        String colQual = "1/mysteryMeat/chocolate";
        assertChildAccepts(colQual, TIMESTAMP_IN_FUTURE);
        assertChildAccepts(colQual, 36 * DAYS_AGO);
        assertChildAccepts(colQual, 364 * DAYS_AGO);
        assertChildAccepts(colQual, 3649 * DAYS_AGO);
        assertChildRejects(colQual, 3650 * DAYS_AGO);
        assertChildRejects(colQual, 36500 * DAYS_AGO);
        assertChildRejects(colQual, VERY_OLD_TIMESTAMP);
    }
    
    private void assertChildAccepts(String colQual, long timestamp) {
        Key key = new Key("row", "cf", colQual, "cv", timestamp);
        assertTrue(parentFilter.accept(key, new Value()));
    }
    
    private void assertChildRejects(String colQual, long timestamp) {
        Key key = new Key("row", "cf", colQual, "cv", timestamp);
        assertFalse(childFilter.accept(key, new Value()));
    }
    
    private void assertParentAccepts(String colVis, long timestamp) {
        Key key = new Key("row", "cf", "cq", colVis, timestamp);
        assertTrue(parentFilter.accept(key, new Value()));
    }
    
    private void assertParentRejects(String colVis, long timestamp) {
        Key key = new Key("row", "cf", "cq", colVis, timestamp);
        assertFalse(parentFilter.accept(key, new Value()));
    }
    
    private static AppliedRule loadRulesFromFile(FileRuleWatcher watcher, FileSystem fs, Path filePath) throws IOException {
        Collection<FilterRule> rules = watcher.loadContents(fs.open(filePath));
        // should only have the single rule
        assertEquals(1, rules.size());
        return (AppliedRule) rules.iterator().next();
    }
}
