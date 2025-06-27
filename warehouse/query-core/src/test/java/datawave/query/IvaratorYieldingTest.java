package datawave.query;

import static datawave.query.iterator.QueryOptions.SORTED_UIDS;
import static datawave.query.testframework.CitiesDataType.CityField.CITY;
import static datawave.query.testframework.CitiesDataType.CityField.CONTINENT;
import static datawave.query.testframework.CitiesDataType.CityField.COUNTRY;
import static datawave.query.testframework.CitiesDataType.CityField.EVENT_ID;
import static datawave.query.testframework.CitiesDataType.CityField.NUM;
import static datawave.query.testframework.CitiesDataType.CityField.START_DATE;
import static datawave.query.testframework.CitiesDataType.CityField.STATE;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.WaitWindowQueryIterator;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.CityDataManager;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.FileType;
import datawave.query.testframework.GenericCityFields;

public class IvaratorYieldingTest extends AbstractFunctionalQuery {

    @ClassRule
    public static AccumuloSetup accumuloSetup = new AccumuloSetup();

    private static final Logger log = Logger.getLogger(IvaratorYieldingTest.class);

    public IvaratorYieldingTest() {
        super(CitiesDataType.getManager());
    }

    // ============================================
    // implemented abstract methods
    protected void testInit() {
        this.auths = CitiesDataType.getTestAuths();
        this.documentKey = EVENT_ID.name();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addIndexField(NUM.name());
        CityDataManager.newInstance();
        dataTypes.add(new CitiesDataType(CitiesDataType.CityEntry.generic, generic));

        accumuloSetup.setData(FileType.CSV, dataTypes);
        // The YieldingQueryIterator is simulating the Accumulo framework that handles yields and
        // re-seeks the iterator. Set RebuildingScannerTestHelper to NEVER/NEVER to avoid interference
        client = accumuloSetup.loadTables(log, RebuildingScannerTestHelper.TEARDOWN.NEVER, RebuildingScannerTestHelper.INTERRUPT.NEVER);
    }

    @Before
    public void setup() throws IOException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        logic.setFullTableScanEnabled(true);
        // this should force regex expansion into ivarators
        logic.setMaxValueExpansionThreshold(1);

        // setup the hadoop configuration
        URL hadoopConfig = this.getClass().getResource("/testhadoop.config");
        logic.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());

        // setup a directory for cache results
        IvaratorCacheDirConfig config = new IvaratorCacheDirConfig(temporaryFolder.newFolder().toURI().toString());
        logic.setIvaratorCacheDirConfigs(Collections.singletonList(config));

        logic.setYieldThresholdMs(1);
        logic.getQueryPlanner().setQueryIteratorClass(YieldingSortedKeyValueIterator.class);
    }

    @Test
    public void testSerial_SortedUIDs_TimingDetails() throws Exception {
        runTest1(1, true, true);
        runTest2(1, true, true);
    }

    @Test
    public void testSerial_SortedUIDs_NoTimingDetails() throws Exception {
        runTest1(1, true, false);
        runTest2(1, true, false);
    }

    @Test
    public void testSerial_UnSortedUIDs_TimingDetails() throws Exception {
        runTest1(1, false, true);
        runTest2(1, false, true);
    }

    @Test
    public void testSerial_UnSortedUIDs_NoTimingDetails() throws Exception {
        runTest1(1, false, false);
        runTest2(1, false, false);
    }

    @Test
    public void testPipeline_SortedUIDs_TimingDetails() throws Exception {
        runTest1(4, true, true);
        runTest2(4, true, true);
    }

    @Test
    public void testPipeline_SortedUIDs_NoTimingDetails() throws Exception {
        runTest1(4, true, false);
        runTest2(4, true, false);
    }

    @Test
    public void testPipeline_UnSortedUIDs_TimingDetails() throws Exception {
        runTest1(4, false, true);
        runTest2(4, false, true);
    }

    @Test
    public void testPipeline_UnSortedUIDs_NoTimingDetails() throws Exception {
        runTest1(4, false, false);
        runTest2(4, false, false);
    }

    public void runTest1(int pipelines, boolean sortedUIDs, boolean timingDetails) throws Exception {
        Map<String,String> params = new HashMap<>();
        if (sortedUIDs) {
            // both required in order to force ivarator to call fillSets
            params.put(SORTED_UIDS, "true");
        }
        logic.setUnsortedUIDsEnabled(!sortedUIDs);
        logic.setCollectTimingDetails(timingDetails);
        logic.setLogTimingDetails(timingDetails);
        logic.setMaxEvaluationPipelines(pipelines);

        String query = STATE.name() + "=~'.*[a-z].*' && filter:includeRegex(" + STATE.name() + ",'m.*')";
        String expected = STATE.name() + "=~'m.*'";
        logExpectedEvents(expected);
        runTest(query, expected, params);
    }

    public void runTest2(int pipelines, boolean sortedUIDs, boolean timingDetails) throws Exception {
        Map<String,String> params = new HashMap<>();
        if (sortedUIDs) {
            // both required in order to force ivarator to call fillSets
            params.put(SORTED_UIDS, "true");
        }
        logic.setUnsortedUIDsEnabled(!sortedUIDs);
        logic.setCollectTimingDetails(timingDetails);
        logic.setLogTimingDetails(timingDetails);
        logic.setMaxEvaluationPipelines(pipelines);

        String query = "((" + CITY.name() + " == 'rome' || " + CITY.name() + " == 'paris' || " + CITY.name() + " == 'london' || " + CITY.name()
                        + " =~ '.*edge.*'" + ") && (" + COUNTRY.name() + " == 'united states')) || (" + COUNTRY.name() + " == 'italy' && " + CONTINENT.name()
                        + " == 'europe')";
        String expected = "(CITY =~ '(rome|paris|london)' && COUNTRY == 'united states' && STATE =~ '(m|o).*') || (COUNTRY == 'italy' && CONTINENT == 'europe')";
        logExpectedEvents(expected);
        runTest(query, expected, params);
    }

    private void logExpectedEvents(String expectedQuery) {
        Collection<String> fields = Arrays.asList(START_DATE.name(), EVENT_ID.name(), CITY.name(), STATE.name(), COUNTRY.name());
        List<Map<String,String>> expectedEvents = getExpectedEvents(expectedQuery, fields);
        StringBuilder sb = new StringBuilder();
        for (Map<String,String> event : expectedEvents) {
            sb.append(event.toString()).append("\n");
        }
        log.info("Expected events for query: " + expectedQuery + "\n" + sb);
    }

    public static class YieldingSortedKeyValueIterator implements SortedKeyValueIterator<Key,Value> {

        private QueryIterator __delegate;
        private YieldCallback<Key> __yield = new YieldCallback<>();
        private SortedKeyValueIterator<Key,Value> __source;
        private Map<String,String> __options;
        private IteratorEnvironment __env;
        private Range __range;
        private Collection<ByteSequence> __columnFamilies;
        private boolean __inclusive;
        private Key lastResultKey = null;
        private Random random = new Random();
        // private long maxChecksBeforeYield = 8;
        // private long randomYieldFrequency = 3;

        public YieldingSortedKeyValueIterator() {
            __delegate = new WaitWindowQueryIterator(getMaxChecksBeforeYield(), getRandomYieldFrequency());
        }

        public YieldingSortedKeyValueIterator(QueryIterator other, IteratorEnvironment env) {
            __delegate = new WaitWindowQueryIterator((WaitWindowQueryIterator) other, env);
        }

        @Override
        public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
            __source = source;
            __options = options;
            __env = env;
            __delegate.init(source, options, env);
            // now enable yielding
            __delegate.enableYielding(__yield);
        }

        @Override
        public boolean hasTop() {
            boolean hasTop = __delegate.hasTop();
            while (__yield.hasYielded()) {
                try {
                    Key yieldKey = __yield.getPositionAndReset();
                    checkYieldKey(yieldKey);
                    createAndSeekNewQueryIterator(yieldKey);
                    hasTop = __delegate.hasTop();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return hasTop;
        }

        public long getMaxChecksBeforeYield() {
            return random.nextInt(10);
        }

        public long getRandomYieldFrequency() {
            return random.nextInt(4) + 1;
        }

        @Override
        public void enableYielding(YieldCallback<Key> yieldCallback) {
            throw new UnsupportedOperationException("Yielding being handled internally");
        }

        private void checkYieldKey(Key yieldKey) throws IOException {
            if (!__range.contains(yieldKey)) {
                throw new IllegalStateException("Yielded to key outside of range " + yieldKey + " not in " + __range);
            }
            if (lastResultKey != null && yieldKey.compareTo(lastResultKey) <= 0) {
                throw new IOException(
                                "Underlying iterator yielded to a position that does not follow the last key returned: " + yieldKey + " <= " + lastResultKey);
            }
        }

        private void createAndSeekNewQueryIterator(Key yieldKey) throws IOException {
            log.debug("Yielded at " + yieldKey + " after seeking range " + __range);
            __delegate = new WaitWindowQueryIterator(getMaxChecksBeforeYield(), getRandomYieldFrequency());
            __delegate.init(__source, __options, __env);
            __delegate.enableYielding(__yield);
            __range = new Range(yieldKey, false, __range.getEndKey(), __range.isEndKeyInclusive());
            __delegate.seek(__range, __columnFamilies, __inclusive);
        }

        @Override
        public void next() throws IOException {
            __delegate.next();
        }

        @Override
        public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
            __range = range;
            __columnFamilies = columnFamilies;
            __inclusive = inclusive;
            __delegate.seek(range, columnFamilies, inclusive);
        }

        @Override
        public Key getTopKey() {
            Key resultKey = __delegate.getTopKey();
            if (lastResultKey != null && resultKey != null && resultKey.compareTo(lastResultKey) < 0) {
                throw new IllegalStateException(
                                "Result key does not follow the last key returned -- results should be sorted: " + resultKey + " <= " + lastResultKey);
            }
            lastResultKey = resultKey;
            return resultKey;
        }

        @Override
        public Value getTopValue() {
            return __delegate.getTopValue();
        }

        @Override
        public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
            return __delegate.deepCopy(env);
        }

    }
}
