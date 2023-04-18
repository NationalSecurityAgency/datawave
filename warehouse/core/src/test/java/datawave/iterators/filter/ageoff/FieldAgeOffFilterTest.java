package datawave.iterators.filter.ageoff;

import com.google.common.base.Predicate;
import datawave.iterators.filter.AgeOffConfigParams;
import datawave.iterators.filter.AgeOffTtlUnits;
import datawave.iterators.test.StubbedIteratorEnvironment;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class FieldAgeOffFilterTest {
    private static final String VISIBILITY_PATTERN = "MY_VIS";
    private static final int ONE_SEC = 1000;
    private static final int ONE_MIN = 60 * ONE_SEC;
    
    private ConfigurableIteratorEnvironment iterEnv = new ConfigurableIteratorEnvironment();
    
    private class ConfigurableIteratorEnvironment extends StubbedIteratorEnvironment {
        
        private IteratorUtil.IteratorScope scope;
        private AccumuloConfiguration conf;
        
        public ConfigurableIteratorEnvironment() {
            scope = null;
            conf = null;
        }
        
        public void setConf(AccumuloConfiguration conf) {
            this.conf = conf;
        }
        
        @Override
        public AccumuloConfiguration getConfig() {
            return conf;
        }
        
        @Override
        public IteratorUtil.IteratorScope getIteratorScope() {
            return scope;
        }
    }
    
    public class EditableAccumuloConfiguration extends AccumuloConfiguration {
        
        private Map<String,String> map = new HashMap<>();
        
        public EditableAccumuloConfiguration(AccumuloConfiguration source) {
            for (Map.Entry<String,String> item : source) {
                map.put(item.getKey(), item.getValue());
            }
        }
        
        public void put(String k, String v) {
            map.put(k, v);
        }
        
        @Override
        public String get(Property property) {
            return map.get(property.getKey());
        }
        
        @Override
        public void getProperties(Map<String,String> props, Predicate<String> filter) {
            for (Map.Entry<String,String> item : map.entrySet()) {
                if (filter.apply(item.getKey())) {
                    props.put(item.getKey(), item.getValue());
                }
            }
        }
        
        @Override
        public Iterator<Map.Entry<String,String>> iterator() {
            return map.entrySet().iterator();
        }
        
    }
    
    @Test
    public void testIndexTrueUsesDefaultWhenFieldLacksTtl() {
        EditableAccumuloConfiguration conf = new EditableAccumuloConfiguration(AccumuloConfiguration.getDefaultConfiguration());
        conf.put("table.custom.isindextable", "true");
        iterEnv.setConf(conf);
        
        long tenSecondsAgo = System.currentTimeMillis() - (10L * ONE_SEC);
        
        FieldAgeOffFilter ageOffFilter = new FieldAgeOffFilter();
        FilterOptions filterOptions = createFilterOptionsWithPattern();
        // set the default to 5 seconds
        filterOptions.setTTL(5L);
        filterOptions.setTTLUnits(AgeOffTtlUnits.SECONDS);
        // set up ttls for field_y and field_z only, deliberately exclude the ttl for field_y
        filterOptions.setOption("fields", "field_y,field_z\\x00my-uuid");
        filterOptions.setOption("field_z\\x00my-uuid.ttl", "2"); // 2 seconds
        filterOptions.setOption("field_y.ttl", "2"); // 2 seconds
        ageOffFilter.init(filterOptions, iterEnv);
        // field_y is a match, but its ttl was not defined, so it wil use the default one
        Key key = new Key("1234", "field_z\\x00my-uuid", "field_z\u0000value", VISIBILITY_PATTERN, tenSecondsAgo);
        Assert.assertFalse(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key, new Value()));
        Assert.assertTrue(ageOffFilter.isFilterRuleApplied());
        key = new Key("1234", "field_y", "field_y\u0000value", VISIBILITY_PATTERN, tenSecondsAgo);
        Assert.assertFalse(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key, new Value()));
        Assert.assertTrue(ageOffFilter.isFilterRuleApplied());
    }
    
    @Test
    public void testIterEnvNotLostOnDeepCopy() {
        EditableAccumuloConfiguration conf = new EditableAccumuloConfiguration(AccumuloConfiguration.getDefaultConfiguration());
        conf.put("table.custom.isindextable", "true");
        iterEnv.setConf(conf);
        
        long tenSecondsAgo = System.currentTimeMillis() - (10L * ONE_SEC);
        
        FieldAgeOffFilter ageOffFilter = new FieldAgeOffFilter();
        
        FilterOptions filterOptions = createFilterOptionsWithPattern();
        // set the default to 5 seconds
        filterOptions.setTTL(5L);
        filterOptions.setTTLUnits(AgeOffTtlUnits.SECONDS);
        // set up ttls for field_y and field_z only, deliberately exclude the ttl for field_y
        filterOptions.setOption("fields", "field_y,field_z\\x00my-uuid");
        filterOptions.setOption("field_z\\x00my-uuid.ttl", "2"); // 2 seconds
        filterOptions.setOption("field_y.ttl", "2"); // 2 seconds
        
        ageOffFilter.init(filterOptions, iterEnv);
        Assert.assertNotNull("IteratorEnvironment should not be null after init!", ageOffFilter.iterEnv);
        // originally this would cause the iterEnv to be lost and test would fail
        ageOffFilter = (FieldAgeOffFilter) ageOffFilter.deepCopy(tenSecondsAgo);
        
        Assert.assertNotNull("IteratorEnvironment should not be null after deep copy!", ageOffFilter.iterEnv);
    }
    
    @Test
    public void testIndexFalseUsesDefaultWhenFieldLacksTtl() {
        EditableAccumuloConfiguration conf = new EditableAccumuloConfiguration(AccumuloConfiguration.getDefaultConfiguration());
        conf.put("isindextable", "false");
        iterEnv.setConf(conf);
        
        long tenSecondsAgo = System.currentTimeMillis() - (10L * ONE_SEC);
        
        FieldAgeOffFilter ageOffFilter = new FieldAgeOffFilter();
        FilterOptions filterOptions = createFilterOptionsWithPattern();
        // set the default to 5 seconds
        filterOptions.setTTL(5L);
        filterOptions.setTTLUnits(AgeOffTtlUnits.SECONDS);
        // set up ttls for field_y and field_z only, deliberately exclude the ttl for field_y
        filterOptions.setOption("fields", "field_y,field_z\\x00my-uuid");
        filterOptions.setOption("field_z\\x00my-uuid.ttl", "2"); // 2 seconds
        filterOptions.setOption("field_y.ttl", "2"); // 2 seconds
        ageOffFilter.init(filterOptions, iterEnv);
        // field_y is a match, but its ttl was not defined, so it wil use the default one
        Key key = new Key("1234", "field_z\\x00my-uuid", "field_z\u0000value", VISIBILITY_PATTERN, tenSecondsAgo);
        Assert.assertTrue(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key, new Value()));
        Assert.assertFalse(ageOffFilter.isFilterRuleApplied());
        key = new Key("1234", "field_y", "field_y\u0000value", VISIBILITY_PATTERN, tenSecondsAgo);
        Assert.assertFalse(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key, new Value()));
        Assert.assertTrue(ageOffFilter.isFilterRuleApplied());
    }
    
    @Test
    public void testLegacyIndexTrueUsesDefaultWhenFieldLacksTtl() {
        EditableAccumuloConfiguration conf = new EditableAccumuloConfiguration(AccumuloConfiguration.getDefaultConfiguration());
        iterEnv.setConf(conf);
        
        long tenSecondsAgo = System.currentTimeMillis() - (10L * ONE_SEC);
        
        FieldAgeOffFilter ageOffFilter = new FieldAgeOffFilter();
        FilterOptions filterOptions = createFilterOptionsWithPattern();
        // set the default to 5 seconds
        filterOptions.setTTL(5L);
        filterOptions.setTTLUnits(AgeOffTtlUnits.SECONDS);
        filterOptions.setOption("isindextable", "true");
        // set up ttls for field_y and field_z only, deliberately exclude the ttl for field_y
        filterOptions.setOption("fields", "field_y,field_z\\x00my-uuid");
        filterOptions.setOption("field_z\\x00my-uuid.ttl", "2"); // 2 seconds
        filterOptions.setOption("field_y.ttl", "2"); // 2 seconds
        ageOffFilter.init(filterOptions, iterEnv);
        // field_y is a match, but its ttl was not defined, so it wil use the default one
        Key key = new Key("1234", "field_z\\x00my-uuid", "field_z\u0000value", VISIBILITY_PATTERN, tenSecondsAgo);
        Assert.assertFalse(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key, new Value()));
        Assert.assertTrue(ageOffFilter.isFilterRuleApplied());
        key = new Key("1234", "field_y", "field_y\u0000value", VISIBILITY_PATTERN, tenSecondsAgo);
        Assert.assertFalse(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key, new Value()));
        Assert.assertTrue(ageOffFilter.isFilterRuleApplied());
    }
    
    @Test
    public void testLegacyIndexFalseUsesDefaultWhenFieldLacksTtl() {
        EditableAccumuloConfiguration conf = new EditableAccumuloConfiguration(AccumuloConfiguration.getDefaultConfiguration());
        iterEnv.setConf(conf);
        
        long tenSecondsAgo = System.currentTimeMillis() - (10L * ONE_SEC);
        
        FieldAgeOffFilter ageOffFilter = new FieldAgeOffFilter();
        FilterOptions filterOptions = createFilterOptionsWithPattern();
        // set the default to 5 seconds
        filterOptions.setTTL(5L);
        filterOptions.setTTLUnits(AgeOffTtlUnits.SECONDS);
        filterOptions.setOption("isindextable", "false");
        // set up ttls for field_y and field_z only, deliberately exclude the ttl for field_y
        filterOptions.setOption("fields", "field_y,field_z\\x00my-uuid");
        filterOptions.setOption("field_z\\x00my-uuid.ttl", "2"); // 2 seconds
        filterOptions.setOption("field_y.ttl", "2"); // 2 seconds
        ageOffFilter.init(filterOptions, iterEnv);
        // field_y is a match, but its ttl was not defined, so it wil use the default one
        Key key = new Key("1234", "field_z\\x00my-uuid", "field_z\u0000value", VISIBILITY_PATTERN, tenSecondsAgo);
        Assert.assertTrue(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key, new Value()));
        Assert.assertFalse(ageOffFilter.isFilterRuleApplied());
        key = new Key("1234", "field_y", "field_y\u0000value", VISIBILITY_PATTERN, tenSecondsAgo);
        Assert.assertFalse(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key, new Value()));
        Assert.assertTrue(ageOffFilter.isFilterRuleApplied());
    }
    
    @Test
    public void testIndexTrueDefaultFalseWhenFieldLacksTtl() {
        EditableAccumuloConfiguration conf = new EditableAccumuloConfiguration(AccumuloConfiguration.getDefaultConfiguration());
        iterEnv.setConf(conf);
        
        long tenSecondsAgo = System.currentTimeMillis() - (10L * ONE_SEC);
        
        FieldAgeOffFilter ageOffFilter = new FieldAgeOffFilter();
        FilterOptions filterOptions = createFilterOptionsWithPattern();
        // set the default to 5 seconds
        filterOptions.setTTL(5L);
        filterOptions.setTTLUnits(AgeOffTtlUnits.SECONDS);
        // set up ttls for field_y and field_z only, deliberately exclude the ttl for field_y
        filterOptions.setOption("fields", "field_y,field_z\\x00my-uuid");
        filterOptions.setOption("field_z\\x00my-uuid.ttl", "2"); // 2 seconds
        filterOptions.setOption("field_y.ttl", "2"); // 2 seconds
        ageOffFilter.init(filterOptions, iterEnv);
        // field_y is a match, but its ttl was not defined, so it wil use the default one
        Key key = new Key("1234", "field_z\\x00my-uuid", "field_z\u0000value", VISIBILITY_PATTERN, tenSecondsAgo);
        Assert.assertTrue(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key, new Value()));
        Assert.assertFalse(ageOffFilter.isFilterRuleApplied());
        key = new Key("1234", "field_y", "field_y\u0000value", VISIBILITY_PATTERN, tenSecondsAgo);
        Assert.assertFalse(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key, new Value()));
        Assert.assertTrue(ageOffFilter.isFilterRuleApplied());
    }
    
    @Test
    public void testIgnoresDocument() {
        Key key = new Key("1234", "d", "someother stuff", VISIBILITY_PATTERN);
        
        FieldAgeOffFilter ageOffFilter = new FieldAgeOffFilter();
        ageOffFilter.init(createFilterOptionsWithPattern(), iterEnv);
        
        // age off immediately
        AgeOffPeriod futureAgeOff = new AgeOffPeriod(System.currentTimeMillis());
        Assert.assertTrue(ageOffFilter.accept(futureAgeOff, key, new Value()));
        Assert.assertFalse(ageOffFilter.isFilterRuleApplied());
    }
    
    @Test
    public void testKeepsMatchBeforeTtl() {
        long oneSecondAgo = System.currentTimeMillis() - (1 * ONE_SEC);
        
        FieldAgeOffFilter ageOffFilter = new FieldAgeOffFilter();
        FilterOptions filterOptions = createFilterOptionsWithPattern();
        // set the default to 5 minutes
        filterOptions.setTTL(5);
        filterOptions.setTTLUnits(AgeOffTtlUnits.MINUTES);
        // set up ttls for field_y and field_z only, deliberately exclude the ttl for field_y
        filterOptions.setOption("fields", "field_y,field_z");
        filterOptions.setOption("field_z.ttl", "2"); // 2 seconds
        ageOffFilter.init(filterOptions, iterEnv);
        // field_y is a match, but its ttl was not defined, so it will use the default one
        Key key = new Key("1234", "myDataType\\x00my-uuid", "field_y\u0000value", VISIBILITY_PATTERN, oneSecondAgo);
        Assert.assertTrue(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key, new Value()));
        Assert.assertTrue(ageOffFilter.isFilterRuleApplied());
    }
    
    @Test
    public void testUsesDefaultWhenFieldLacksTtl() {
        long tenSecondsAgo = System.currentTimeMillis() - (10 * ONE_SEC);
        
        FieldAgeOffFilter ageOffFilter = new FieldAgeOffFilter();
        FilterOptions filterOptions = createFilterOptionsWithPattern();
        // set the default to 5 seconds
        filterOptions.setTTL(5);
        filterOptions.setTTLUnits(AgeOffTtlUnits.SECONDS);
        // set up ttls for field_y and field_z only, deliberately exclude the ttl for field_y
        filterOptions.setOption("fields", "field_y,field_z");
        filterOptions.setOption("field_z.ttl", "2"); // 2 seconds
        ageOffFilter.init(filterOptions, iterEnv);
        // field_y is a match, but its ttl was not defined, so it will use the default one
        Key key = new Key("1234", "myDataType\\x00my-uuid", "field_y\u0000value", VISIBILITY_PATTERN, tenSecondsAgo);
        Assert.assertFalse(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key, new Value()));
        Assert.assertTrue(ageOffFilter.isFilterRuleApplied());
    }
    
    @Test
    public void testPassesThroughWhenFieldDoesNotMatch() {
        long tenSecondsAgo = System.currentTimeMillis() - (10 * ONE_SEC);
        
        FieldAgeOffFilter ageOffFilter = new FieldAgeOffFilter();
        FilterOptions filterOptions = createFilterOptionsWithPattern();
        // set the default to 5 seconds
        filterOptions.setTTL(5);
        filterOptions.setTTLUnits(AgeOffTtlUnits.SECONDS);
        // set up ttls for field_y and field_z only
        filterOptions.setOption("fields", "field_y,field_z");
        filterOptions.setOption("field_y.ttl", "1"); // 1 second
        filterOptions.setOption("field_z.ttl", "2"); // 2 seconds
        ageOffFilter.init(filterOptions, iterEnv);
        
        // field_a is not a match, so it should pass through
        Key key = new Key("1234", "myDataType\\x00my-uuid", "field_a\u0000value", VISIBILITY_PATTERN, tenSecondsAgo);
        Assert.assertTrue(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key, new Value()));
        Assert.assertFalse(ageOffFilter.isFilterRuleApplied());
    }
    
    @Test
    public void testKeepsMatchBeforeTtlForRevisedOptionKey() {
        long currentTime = System.currentTimeMillis();
        long oneMinuteAgo = currentTime - (1 * ONE_MIN);
        long tenMinutesAgo = currentTime - (10 * ONE_MIN);
        
        FieldAgeOffFilter ageOffFilter = new FieldAgeOffFilter();
        FilterOptions filterOptions = createFilterOptionsWithPattern();
        // set the default to 5 minutes
        filterOptions.setTTL(5);
        filterOptions.setTTLUnits(AgeOffTtlUnits.MINUTES);
        // set up ttls for field_y and field_z only, deliberately exclude the ttl for field_y
        filterOptions.setOption("fields", "field_y");
        filterOptions.setOption("field.field_z.ttl", "2"); // 2 minutes
        ageOffFilter.init(filterOptions, iterEnv);
        // field_y is a match, but its ttl was not defined, so it will use the default one
        Key keyY = new Key("1234", "myDataType\\x00my-uuid", "field_y\u0000value", VISIBILITY_PATTERN, oneMinuteAgo);
        Assert.assertTrue(ageOffFilter.accept(filterOptions.getAgeOffPeriod(currentTime), keyY, new Value()));
        Assert.assertTrue(ageOffFilter.isFilterRuleApplied());
        // field_z is a match and is not defined in the "fields" tag
        Key keyZ = new Key("1234", "myDataType\\x00my-uuid", "field_z\u0000value", VISIBILITY_PATTERN, tenMinutesAgo);
        Assert.assertFalse(ageOffFilter.accept(filterOptions.getAgeOffPeriod(currentTime), keyZ, new Value()));
        Assert.assertTrue(ageOffFilter.isFilterRuleApplied());
    }
    
    @Test
    public void testUsesDefaultWhenFieldLacksTtlForRevisedOptionKey() {
        long oneSecondAgo = System.currentTimeMillis() - (1 * ONE_SEC);
        long tenSecondsAgo = System.currentTimeMillis() - (10 * ONE_SEC);
        
        FieldAgeOffFilter ageOffFilter = new FieldAgeOffFilter();
        FilterOptions filterOptions = createFilterOptionsWithPattern();
        // set the default to 5 seconds
        filterOptions.setTTL(5);
        filterOptions.setTTLUnits(AgeOffTtlUnits.SECONDS);
        // set up ttls for field_y and field_z only, deliberately exclude the ttl for field_y
        filterOptions.setOption("fields", "field_y");
        filterOptions.setOption("field.field_z.ttl", "2"); // 2 seconds
        ageOffFilter.init(filterOptions, iterEnv);
        // field_y is a match, but its ttl was not defined, so it will use the default one
        Key key = new Key("1234", "myDataType\\x00my-uuid", "field_y\u0000value", VISIBILITY_PATTERN, tenSecondsAgo);
        Assert.assertFalse(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key, new Value()));
        Assert.assertTrue(ageOffFilter.isFilterRuleApplied());
        // field_z is a match, but the key is more recent than its age off period
        Key keyZ = new Key("1234", "myDataType\\x00my-uuid", "field_z\u0000value", VISIBILITY_PATTERN, oneSecondAgo);
        Assert.assertTrue(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), keyZ, new Value()));
        Assert.assertTrue(ageOffFilter.isFilterRuleApplied());
    }
    
    @Test
    public void handlesFieldsStartingWithNumber() {
        long oneSecondAgo = System.currentTimeMillis() - (1 * ONE_SEC);
        long tenSecondsAgo = System.currentTimeMillis() - (10 * ONE_SEC);
        
        FieldAgeOffFilter ageOffFilter = new FieldAgeOffFilter();
        FilterOptions filterOptions = createFilterOptionsWithPattern();
        // set the default to 5 seconds
        filterOptions.setTTL(5);
        filterOptions.setTTLUnits(AgeOffTtlUnits.SECONDS);
        // set up ttls for field_y and field_z only, deliberately exclude the ttl for field_y
        filterOptions.setOption("fields", "field_y");
        filterOptions.setOption("field.12_3_4.ttl", "2"); // 2 seconds
        filterOptions.setOption("field.12_3_4.ttlUnits", "s"); // 2 seconds
        ageOffFilter.init(filterOptions, iterEnv);
        // field_y is a match, but its ttl was not defined, so it will use the default one
        Key key = new Key("1234", "myDataType\\x00my-uuid", "field_y\u0000value", VISIBILITY_PATTERN, tenSecondsAgo);
        Assert.assertFalse(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key, new Value()));
        Assert.assertTrue(ageOffFilter.isFilterRuleApplied());
        // field_z is a match, but the key is more recent than its age off period
        Key keyNumeric = new Key("1234", "myDataType\\x00my-uuid", "12_3_4\u0000value", VISIBILITY_PATTERN, oneSecondAgo);
        Assert.assertTrue(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), keyNumeric, new Value()));
        Assert.assertTrue(ageOffFilter.isFilterRuleApplied());
    }
    
    @Test
    public void testOverrideUnits() {
        long currentTime = System.currentTimeMillis();
        long oneMinuteAgo = currentTime - (1 * ONE_MIN);
        long tenMinutesAgo = currentTime - (10 * ONE_MIN);
        
        FieldAgeOffFilter ageOffFilter = new FieldAgeOffFilter();
        FilterOptions filterOptions = createFilterOptionsWithPattern();
        // set the default to 5 minutes
        filterOptions.setTTL(5);
        filterOptions.setTTLUnits(AgeOffTtlUnits.MINUTES);
        // set up ttls for field_y and field_z only, deliberately exclude the ttl for field_y
        filterOptions.setOption("fields", "field_y");
        filterOptions.setOption("field.field_z.ttl", "2");
        filterOptions.setOption("field.field_z.ttlUnits", "d"); // 2 days
        ageOffFilter.init(filterOptions, iterEnv);
        // field_y is a match, but its ttl was not defined, so it will use the default one
        Key keyY = new Key("1234", "myDataType\\x00my-uuid", "field_y\u0000value", VISIBILITY_PATTERN, oneMinuteAgo);
        Assert.assertTrue(ageOffFilter.accept(filterOptions.getAgeOffPeriod(currentTime), keyY, new Value()));
        Assert.assertTrue(ageOffFilter.isFilterRuleApplied());
        // field_z is a match and is not defined in the "fields" tag
        Key keyZ = new Key("1234", "myDataType\\x00my-uuid", "field_z\u0000value", VISIBILITY_PATTERN, tenMinutesAgo);
        Assert.assertTrue(ageOffFilter.accept(filterOptions.getAgeOffPeriod(currentTime), keyZ, new Value()));
        Assert.assertTrue(ageOffFilter.isFilterRuleApplied());
    }
    
    @Test
    public void testPassesThroughWhenFieldDoesNotMatchForRevisedOptionKey() {
        long tenSecondsAgo = System.currentTimeMillis() - (10 * ONE_SEC);
        
        FieldAgeOffFilter ageOffFilter = new FieldAgeOffFilter();
        FilterOptions filterOptions = createFilterOptionsWithPattern();
        // set the default to 5 seconds
        filterOptions.setTTL(5);
        filterOptions.setTTLUnits(AgeOffTtlUnits.SECONDS);
        // set up ttls for field_y and field_z only
        filterOptions.setOption("field.field_y.ttl", "1"); // 1 second
        filterOptions.setOption("field.field_z.ttl", "2"); // 2 seconds
        ageOffFilter.init(filterOptions, iterEnv);
        
        // field_a is not a match, so it should pass through
        Key key = new Key("1234", "myDataType\\x00my-uuid", "field_a\u0000value", VISIBILITY_PATTERN, tenSecondsAgo);
        Assert.assertTrue(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key, new Value()));
        Assert.assertFalse(ageOffFilter.isFilterRuleApplied());
    }
    
    @Test
    public void testExcludeEventData() {
        long tenSecondsAgo = System.currentTimeMillis() - (10 * ONE_SEC);
        
        FieldAgeOffFilter ageOffFilter = new FieldAgeOffFilter();
        FilterOptions filterOptions = createFilterOptionsWithPattern();
        // set the default to 5 seconds
        filterOptions.setTTL(5);
        filterOptions.setTTLUnits(AgeOffTtlUnits.SECONDS);
        // set up ttls for field_y and field_z only
        filterOptions.setOption("fields", "field_y,field_z");
        filterOptions.setOption("excludeData", "event");
        filterOptions.setOption("field.field_y.ttl", "9"); // 9 seconds
        filterOptions.setOption("field.field_z.ttl", "11"); // 11 seconds
        ageOffFilter.init(filterOptions, iterEnv);
        
        // field_y is event data, it should pass through; but rule is not applied
        Key key1 = new Key("1234", "myDataType\\x00my-uuid", "field_y\u0000value", VISIBILITY_PATTERN, tenSecondsAgo);
        Assert.assertTrue(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key1, new Value()));
        Assert.assertFalse(ageOffFilter.isFilterRuleApplied());
        
        // field_y is index data, it should not pass through and rule applied
        Key key2 = new Key("1234", "fi\u0000field_y", "my_value\\x00my-uuid", VISIBILITY_PATTERN, tenSecondsAgo);
        Assert.assertFalse(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key2, new Value()));
        Assert.assertTrue(ageOffFilter.isFilterRuleApplied());
        
        // field_z is index data and not aged-off, so it should pass through
        Key key3 = new Key("1234", "fi\u0000field_z", "my_value\\x00my-uuid", VISIBILITY_PATTERN, tenSecondsAgo);
        Assert.assertTrue(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key3, new Value()));
        Assert.assertTrue(ageOffFilter.isFilterRuleApplied());
    }
    
    private FilterOptions createFilterOptionsWithPattern() {
        FilterOptions filterOptions = new FilterOptions();
        filterOptions.setOption(AgeOffConfigParams.MATCHPATTERN, VISIBILITY_PATTERN);
        return filterOptions;
    }
}
