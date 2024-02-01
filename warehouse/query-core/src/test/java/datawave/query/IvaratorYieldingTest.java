package datawave.query;

import static datawave.query.iterator.QueryOptions.SORTED_UIDS;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetup;
import datawave.query.testframework.CitiesDataType;
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
        this.documentKey = CitiesDataType.CityField.EVENT_ID.name();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        Collection<DataTypeHadoopConfig> dataTypes = new ArrayList<>();
        FieldConfig generic = new GenericCityFields();
        generic.addIndexField(CitiesDataType.CityField.NUM.name());
        dataTypes.add(new CitiesDataType(CitiesDataType.CityEntry.generic, generic));

        accumuloSetup.setData(FileType.CSV, dataTypes);
        client = accumuloSetup.loadTables(log, RebuildingScannerTestHelper.TEARDOWN.ALWAYS_SANS_CONSISTENCY,
                        RebuildingScannerTestHelper.INTERRUPT.FI_EVERY_OTHER);
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
        logic.getQueryPlanner().setQueryIteratorClass(YieldingQueryIterator.class);
    }

    @Test
    public void testSerial_SortedUIDs_TimingDetails() throws Exception {
        runTest(1, true, true);
    }

    @Test
    public void testSerial_SortedUIDs_NoTimingDetails() throws Exception {
        runTest(1, true, false);
    }

    @Test
    public void testSerial_UnSortedUIDs_TimingDetails() throws Exception {
        runTest(1, false, true);
    }

    @Test
    public void testSerial_UnSortedUIDs_NoTimingDetails() throws Exception {
        runTest(1, false, false);
    }

    @Test
    public void testPipeline_SortedUIDs_TimingDetails() throws Exception {
        runTest(4, true, true);
    }

    @Test
    public void testPipeline_SortedUIDs_NoTimingDetails() throws Exception {
        runTest(4, true, false);
    }

    @Test
    public void testPipeline_UnSortedUIDs_TimingDetails() throws Exception {
        runTest(4, false, true);
    }

    @Test
    public void testPipeline_UnSortedUIDs_NoTimingDetails() throws Exception {
        runTest(4, false, false);
    }

    public void runTest(int pipelines, boolean sortedUIDs, boolean timingDetails) throws Exception {
        Map<String,String> params = new HashMap<>();
        if (sortedUIDs) {
            // both required in order to force ivarator to call fillSets
            params.put(SORTED_UIDS, "true");
        }
        logic.setUnsortedUIDsEnabled(!sortedUIDs);
        logic.setCollectTimingDetails(timingDetails);
        logic.setLogTimingDetails(timingDetails);
        logic.setMaxEvaluationPipelines(pipelines);

        String query = CitiesDataType.CityField.STATE.name() + "=~'.*[a-z].*' && filter:includeRegex(" + CitiesDataType.CityField.STATE.name() + ",'m.*')";
        String expected = CitiesDataType.CityField.STATE.name() + "=~'m.*'";
        runTest(query, expected, params);
    }

    public static class YieldingQueryIterator implements SortedKeyValueIterator<Key,Value> {

        private QueryIterator __delegate;
        private YieldCallback<Key> __yield = new YieldCallback<>();
        private SortedKeyValueIterator<Key,Value> __source;
        private Map<String,String> __options;
        private IteratorEnvironment __env;
        private Range __range;
        private Collection<ByteSequence> __columnFamilies;
        private boolean __inclusive;
        private Key lastResultKey = null;

        public YieldingQueryIterator() {
            __delegate = new QueryIterator();
        }

        public YieldingQueryIterator(QueryIterator other, IteratorEnvironment env) {
            __delegate = new QueryIterator(other, env);
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
            __delegate = new QueryIterator();
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
