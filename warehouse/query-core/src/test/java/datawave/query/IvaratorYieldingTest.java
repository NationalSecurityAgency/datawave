package datawave.query;

import datawave.configuration.spring.SpringBean;
import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.attributes.TypeAttribute;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.iterator.QueryIterator;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.tables.edge.DefaultEdgeEventQueryLogic;
import datawave.query.testframework.AbstractFunctionalQuery;
import datawave.query.testframework.AccumuloSetupHelper;
import datawave.query.testframework.CitiesDataType;
import datawave.query.testframework.DataTypeHadoopConfig;
import datawave.query.testframework.FieldConfig;
import datawave.query.testframework.GenericCityFields;
import datawave.query.util.TypeMetadata;
import datawave.query.util.TypeMetadataHelper;
import datawave.query.util.TypeMetadataWriter;
import datawave.query.util.WiseGuysIngest;
import datawave.webservice.edgedictionary.RemoteEdgeDictionary;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.query.configuration.GenericQueryConfiguration;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import javax.inject.Inject;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.accumulo.core.iterators.YieldingKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import static datawave.query.QueryTestTableHelper.METADATA_TABLE_NAME;
import static datawave.query.QueryTestTableHelper.MODEL_TABLE_NAME;
import static datawave.query.QueryTestTableHelper.SHARD_INDEX_TABLE_NAME;
import static datawave.query.QueryTestTableHelper.SHARD_TABLE_NAME;
import static datawave.query.iterator.QueryOptions.SORTED_UIDS;
import static datawave.query.testframework.RawDataManager.AND_OP;
import static datawave.query.testframework.RawDataManager.JEXL_AND_OP;
import static datawave.query.testframework.RawDataManager.RE_OP;

public class IvaratorYieldingTest extends AbstractFunctionalQuery {
    
    private static final Logger log = Logger.getLogger(IvaratorYieldingTest.class);
    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();
    
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
        
        final AccumuloSetupHelper helper = new AccumuloSetupHelper(dataTypes);
        client = helper.loadTables(log, RebuildingScannerTestHelper.TEARDOWN.ALWAYS_SANS_CONSISTENCY, RebuildingScannerTestHelper.INTERRUPT.FI_EVERY_OTHER);
    }
    
    @Before
    public void setup() throws IOException {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        
        logic.setCollectTimingDetails(true);
        
        logic.setFullTableScanEnabled(true);
        // this should force regex expansion into ivarators
        logic.setMaxValueExpansionThreshold(1);
        
        // setup the hadoop configuration
        URL hadoopConfig = this.getClass().getResource("/testhadoop.config");
        logic.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());
        
        // setup a directory for cache results
        File tmpDir = this.tmpDir.newFolder("Ivarator.cache");
        IvaratorCacheDirConfig config = new IvaratorCacheDirConfig(tmpDir.toURI().toString());
        logic.setIvaratorCacheDirConfigs(Collections.singletonList(config));
        
        logic.setYieldThresholdMs(1);
        logic.getQueryPlanner().setQueryIteratorClass(YieldingQueryIterator.class);
        
        logic.setMaxEvaluationPipelines(1);
    }
    
    @Test
    public void testIvaratorInterruptedAndYieldSorted() throws Exception {
        Map<String,String> params = new HashMap<>();
        // both required in order to force ivarator to call fillSets
        params.put(SORTED_UIDS, "true");
        logic.getConfig().setUnsortedUIDsEnabled(false);
        
        String query = CitiesDataType.CityField.STATE.name() + "=~'.*[a-z].*' && filter:includeRegex(" + CitiesDataType.CityField.STATE.name() + ",'ma.*')";
        String expected = CitiesDataType.CityField.STATE.name() + "=~'ma.*'";
        runTest(query, expected, params);
    }
    
    @Test
    public void testIvaratorInterruptedAndYieldUnsorted() throws Exception {
        String query = CitiesDataType.CityField.STATE.name() + RE_OP + "'.*[a-z].*'" + JEXL_AND_OP + "filter:includeRegex("
                        + CitiesDataType.CityField.STATE.name() + ",'ma.*')";
        String expected = CitiesDataType.CityField.STATE.name() + "=~'ma.*'";
        runTest(query, expected);
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
            return __delegate.hasTop();
        }
        
        @Override
        public void enableYielding(YieldCallback<Key> yieldCallback) {
            throw new UnsupportedOperationException("Yielding being handled internally");
        }
        
        @Override
        public void next() throws IOException {
            __delegate.next();
            while (__yield.hasYielded()) {
                Key key = __yield.getPositionAndReset();
                if (!__range.contains(key)) {
                    throw new IllegalStateException("Yielded to key outside of range");
                }
                __delegate = new QueryIterator();
                __delegate.init(__source, __options, __env);
                __delegate.enableYielding(__yield);
                __range = new Range(key, false, __range.getEndKey(), __range.isEndKeyInclusive());
                log.info("Yielded at " + __range.getStartKey());
                __delegate.seek(__range, __columnFamilies, __inclusive);
            }
        }
        
        @Override
        public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
            __range = range;
            __columnFamilies = columnFamilies;
            __inclusive = inclusive;
            __delegate.seek(range, columnFamilies, inclusive);
            while (__yield.hasYielded()) {
                Key key = __yield.getPositionAndReset();
                if (!__range.contains(key)) {
                    throw new IllegalStateException("Yielded to key outside of range");
                }
                __delegate = new QueryIterator();
                __delegate.init(__source, __options, __env);
                __delegate.enableYielding(__yield);
                __range = new Range(key, false, __range.getEndKey(), __range.isEndKeyInclusive());
                log.info("Yielded at " + __range.getStartKey());
                __delegate.seek(__range, __columnFamilies, __inclusive);
            }
        }
        
        @Override
        public Key getTopKey() {
            return __delegate.getTopKey();
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
