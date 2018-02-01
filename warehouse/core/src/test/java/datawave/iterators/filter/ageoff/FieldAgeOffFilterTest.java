package datawave.iterators.filter.ageoff;

import datawave.iterators.filter.AgeOffConfigParams;
import datawave.iterators.filter.AgeOffTtlUnits;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Assert;
import org.junit.Test;

public class FieldAgeOffFilterTest {
    private static final String VISIBILITY_PATTERN = "MY_VIS";
    private static final int ONE_SEC = 1000;
    private static final int ONE_MIN = 60 * ONE_SEC;
    
    @Test
    public void testIgnoresDocument() {
        Key key = new Key("1234", "d", "someother stuff", VISIBILITY_PATTERN);
        
        FieldAgeOffFilter ageOffFilter = new FieldAgeOffFilter();
        ageOffFilter.init(createFilterOptionsWithPattern());
        
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
        ageOffFilter.init(filterOptions);
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
        ageOffFilter.init(filterOptions);
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
        ageOffFilter.init(filterOptions);
        
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
        ageOffFilter.init(filterOptions);
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
        ageOffFilter.init(filterOptions);
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
        ageOffFilter.init(filterOptions);
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
        ageOffFilter.init(filterOptions);
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
        ageOffFilter.init(filterOptions);
        
        // field_a is not a match, so it should pass through
        Key key = new Key("1234", "myDataType\\x00my-uuid", "field_a\u0000value", VISIBILITY_PATTERN, tenSecondsAgo);
        Assert.assertTrue(ageOffFilter.accept(filterOptions.getAgeOffPeriod(System.currentTimeMillis()), key, new Value()));
        Assert.assertFalse(ageOffFilter.isFilterRuleApplied());
    }
    
    private FilterOptions createFilterOptionsWithPattern() {
        FilterOptions filterOptions = new FilterOptions();
        filterOptions.setOption(AgeOffConfigParams.MATCHPATTERN, VISIBILITY_PATTERN);
        return filterOptions;
    }
}
