package datawave.iterators.filter;

import datawave.iterators.filter.ageoff.AppliedRule;
import datawave.iterators.filter.ageoff.FilterOptions;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

@RunWith(EasyMockRunner.class)
public class ConfigurableAgeOffFilterTest extends EasyMockSupport {
    
    private static long MILLIS_IN_DAY = 1000L * 60 * 60 * 24L;
    // reused in tests but contents never accessed
    private static Value VALUE = new Value();
    
    @Mock
    private IteratorEnvironment env;
    @Mock
    private SortedKeyValueIterator<Key,Value> source;
    
    private DefaultConfiguration conf = new DefaultConfiguration();
    
    @Before
    public void setUp() throws Exception {
        expect(env.getConfig()).andReturn(conf).anyTimes();
        // These two are only for the disabled test
        expect(env.getIteratorScope()).andReturn(IteratorUtil.IteratorScope.majc).anyTimes();
        expect(env.isFullMajorCompaction()).andReturn(false).anyTimes();
        replay(env);
    }
    
    @Test
    public void testAcceptKeyValue_DisabledFullMajc() throws Exception {
        ConfigurableAgeOffFilter filter = new ConfigurableAgeOffFilter();
        Map<String,String> options = getOptionsMap(30, AgeOffTtlUnits.DAYS);
        options.put(AgeOffConfigParams.DISABLE_ON_NON_FULL_MAJC, "true");
        
        filter.init(source, options, env);
        
        assertThat(filter.accept(new Key(), VALUE), is(true));
        // 1970 is older than 30 days, but filter is disable so should be true
        assertThat(filter.accept(getKey(0), VALUE), is(true));
    }
    
    @Test
    public void testAcceptKeyValue_OnlyTtlNoInnerFilters() throws Exception {
        ConfigurableAgeOffFilter filter = new ConfigurableAgeOffFilter();
        Map<String,String> options = getOptionsMap(30, AgeOffTtlUnits.DAYS);
        
        // no file or other delegate filters configured, so only the ttl are used
        filter.init(source, options, env);
        
        assertThat(filter.accept(getKey(daysAgo(10)), VALUE), is(true));
        // 100 is older than 30 days
        assertThat(filter.accept(getKey(daysAgo(100)), VALUE), is(false));
    }
    
    @Test
    public void testAcceptKeyValue_WithFile() throws Exception {
        ConfigurableAgeOffFilter filter = new ConfigurableAgeOffFilter();
        Map<String,String> options = getOptionsMap(30, AgeOffTtlUnits.DAYS);
        options.put(AgeOffConfigParams.FILTER_CONFIG, pathFromClassloader("/test-root-rules.xml"));
        filter.init(source, options, env);
        
        // the file uses TestFilter which always returns false for accept and filter applied
        // so only ttl is uses for acceptance
        assertThat(filter.accept(getKey(daysAgo(15)), VALUE), is(true));
        assertThat(filter.accept(getKey(daysAgo(123)), VALUE), is(false));
    }
    
    @Test
    public void testAcceptKeyValue_TtlSet() throws Exception {
        ConfigurableAgeOffFilter filter = new ConfigurableAgeOffFilter();
        Map<String,String> options = getOptionsMap(30, AgeOffTtlUnits.DAYS);
        // don't start applying this filter unless the data is at least this old?
        options.put(AgeOffConfigParams.TTL_SHORT_CIRCUIT, "5");
        
        Collection<AppliedRule> rules = singleRowMatcher("foo", options);
        // for holding the filters
        FilterWrapper wrapper = getWrappedFilterWithRules(rules, source, options, env);
        
        // copy cofigs to actual filter we are testing
        filter.initialize(wrapper);
        
        // brand new key should be good
        assertThat(filter.accept(new Key(), VALUE), is(true));
        // first five will hit the ttl short circuit
        assertThat(filter.accept(getKey(daysAgo(1)), VALUE), is(true));
        assertThat(filter.accept(getKey(daysAgo(2)), VALUE), is(true));
        assertThat(filter.accept(getKey(daysAgo(3)), VALUE), is(true));
        assertThat(filter.accept(getKey(daysAgo(4)), VALUE), is(true));
        assertThat("If this fails it may be an edge case due to date rollover, try again in a minute", //
                        filter.accept(getKey(daysAgo(5)), VALUE), is(true));
        
        // these will not hit the ttl short circuit and the single applied rule
        assertThat(filter.accept(getKey("foo", daysAgo(6)), VALUE), is(true));
        // will not match so should be true
        assertThat(filter.accept(getKey("bar", daysAgo(7)), VALUE), is(true));
        assertThat(filter.accept(getKey("foo", daysAgo(8)), VALUE), is(true));
        // this is really old and matches so should not be accepted
        assertThat(filter.accept(getKey("foo", daysAgo(365)), VALUE), is(false));
        
    }
    
    @Test
    public void testAcceptKeyValue_MultipleFilters() throws Exception {
        ConfigurableAgeOffFilter filter = new ConfigurableAgeOffFilter();
        Map<String,String> options = getOptionsMap(30, AgeOffTtlUnits.DAYS);
        
        Collection<AppliedRule> rules = singleRowMatcher("foo", options);
        rules.addAll(singleColumnFamilyMatcher("bar", options));
        // for holding the filters
        FilterWrapper wrapper = getWrappedFilterWithRules(rules, source, options, env);
        // copy cofigs to actual filter we are testing
        filter.initialize(wrapper);
        
        // created two rules
        // one looks at the row for "foo"
        // two looks at the column family for "bar"
        // only if they match do they then test the age off
        
        // want to create a scenario where one filter accepts and a second rejects
        // the ttl is set to 30 days
        // test data is:
        // @formatter:off
        // | row | cf  | age  | actions|
        // |-----|-----|------|--------|
        // | foo | wee | 5d   | accept |
        // | bar | tab | 29d  | accept | <= no matches for either filter, under default ttl
        // | bar | tab | 100d | reject | <= no matches for either filter, but over default ttl
        // | low | bar | 32d  | reject | <= second filter rejects due to age
        // @formatter:on
        
        Key fooWee = getKey("foo", "wee", daysAgo(5));
        Key newBarTab = getKey("bar", "tab", daysAgo(29));
        Key oldBarTab = getKey("bar", "tab", daysAgo(100));
        Key lowBar = getKey("low", "bar", daysAgo(32));
        
        assertThat(filter.accept(fooWee, VALUE), is(true));
        assertThat(filter.accept(newBarTab, VALUE), is(true));
        assertThat(filter.accept(oldBarTab, VALUE), is(false));
        assertThat(filter.accept(lowBar, VALUE), is(false));
    }
    
    @Test(expected = NullPointerException.class)
    public void testInitWithNoTtl() throws Exception {
        ConfigurableAgeOffFilter filter = new ConfigurableAgeOffFilter();
        filter.init(source, new HashMap<>(), env);
    }
    
    @Test(expected = NullPointerException.class)
    public void testInitWithNoTtlUnits() throws Exception {
        ConfigurableAgeOffFilter filter = new ConfigurableAgeOffFilter();
        HashMap<String,String> options = new HashMap<>();
        options.put(AgeOffConfigParams.TTL, "31");
        filter.init(source, options, env);
    }
    
    @Test
    public void testValidateOptions() {
        ConfigurableAgeOffFilter filter = new ConfigurableAgeOffFilter();
        Map<String,String> options = new HashMap<>();
        options.put(AgeOffConfigParams.TTL, "1");
        // @formatter:off
        List<String> allUnits = Arrays.asList(
            AgeOffTtlUnits.DAYS, AgeOffTtlUnits.HOURS, AgeOffTtlUnits.MINUTES, AgeOffTtlUnits.SECONDS, AgeOffTtlUnits.MILLISECONDS);
        // @formatter:on
        for (String unit : allUnits) {
            options.put(AgeOffConfigParams.TTL_UNITS, unit);
            assertThat(filter.validateOptions(options), is(true));
        }
        options.put(AgeOffConfigParams.TTL_UNITS, "parsecs");
        assertThat(filter.validateOptions(options), is(false));
        
        options.put(AgeOffConfigParams.TTL, "0x143");
        options.put(AgeOffConfigParams.TTL_UNITS, AgeOffTtlUnits.DAYS);
        assertThat(filter.validateOptions(options), is(false));
    }
    
    // --------------------------------------------
    // Test helper methods and classes
    // --------------------------------------------
    
    private static Map<String,String> getOptionsMap(int ttl, String unit) {
        Map<String,String> options = new HashMap<>();
        options.put(AgeOffConfigParams.TTL, Integer.toString(ttl));
        options.put(AgeOffConfigParams.TTL_UNITS, unit);
        return options;
    }
    
    private Collection<AppliedRule> singleRowMatcher(String pattern, Map<String,String> options) {
        return singleAppliedRule(pattern, options, new TestRowFilter());
    }
    
    private Collection<AppliedRule> singleColumnFamilyMatcher(String pattern, Map<String,String> options) {
        return singleAppliedRule(pattern, options, new TestColFamFilter());
    }
    
    private Collection<AppliedRule> singleAppliedRule(String pattern, Map<String,String> options, AppliedRule inner) {
        FilterOptions filterOpts = new FilterOptions();
        filterOpts.setOption(AgeOffConfigParams.MATCHPATTERN, pattern);
        filterOpts.setTTL(Long.parseLong(options.getOrDefault(AgeOffConfigParams.TTL, "30")));
        filterOpts.setTTLUnits(options.getOrDefault(AgeOffConfigParams.TTL_UNITS, AgeOffTtlUnits.DAYS));
        inner.init(filterOpts);
        Collection<AppliedRule> list = new ArrayList<>();
        // need to do this because otherwise will use 0 as the anchor time
        AppliedRule copyWithCorrectTimestamp = (AppliedRule) inner.deepCopy(System.currentTimeMillis());
        list.add(copyWithCorrectTimestamp);
        return list;
    }
    
    private static long daysAgo(int n) {
        return System.currentTimeMillis() - (MILLIS_IN_DAY * n);
    }
    
    private static Key getKey(long ts) {
        return getKey("", ts);
    }
    
    private static Key getKey(String row, long ts) {
        return getKey(row, "", ts);
    }
    
    private static Key getKey(String row, String cf, long ts) {
        return new Key(row, cf, "", ts);
    }
    
    private String pathFromClassloader(String name) {
        URL resource = this.getClass().getResource(name);
        assertNotNull("Unable to get resource with name: " + name, resource);
        return resource.getPath();
    }
    
    /**
     * Only so we can inject the filterList without using a file to load them from
     */
    private static class FilterWrapper extends ConfigurableAgeOffFilter {
        public FilterWrapper() {}
        
        void setFilterList(Collection<AppliedRule> filterList) {
            this.filterList = filterList;
        }
    }
    
    /**
     * Initilizes the filter with the supplied options, then overwrites the filterList with the ones provided
     */
    static FilterWrapper getWrappedFilterWithRules(Collection<AppliedRule> filterList, SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
                    IteratorEnvironment env) throws IOException {
        // this will initialize the filter with the options, which will set the filter list to null, unless a file is specified in the options
        FilterWrapper wrapper = new FilterWrapper();
        wrapper.init(source, options, env);
        // this will then set the filter list to use
        wrapper.setFilterList(filterList);
        return wrapper;
    }
    
    public static class TestRowFilter extends RegexFilterBase {
        
        @Override
        protected String getKeyField(Key k, Value v) {
            return k.getRow().toString();
        }
    }
    
    public static class TestColFamFilter extends RegexFilterBase {
        
        @Override
        protected String getKeyField(Key k, Value v) {
            return k.getColumnFamily().toString();
        }
    }
    
}
