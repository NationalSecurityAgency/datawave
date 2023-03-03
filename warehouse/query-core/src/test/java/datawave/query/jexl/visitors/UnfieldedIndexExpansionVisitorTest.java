package datawave.query.jexl.visitors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DoNotPerformOptimizedQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.model.QueryModel;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MockMetadataHelper;
import datawave.util.TableName;
import datawave.util.time.DateHelper;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class UnfieldedIndexExpansionVisitorTest {
    
    private static InMemoryInstance instance = new InMemoryInstance(UnfieldedIndexExpansionVisitorTest.class.toString());
    private static Connector conn;
    
    private final static Long timestamp = DateHelper.parse("20210101").getTime();
    private final static Value emptyValue = new Value(new byte[0]);
    
    private static QueryModel model;
    private static Set<String> indexedFields = new HashSet<>();
    private static Set<String> reverseFields = new HashSet<>();
    
    private static ShardQueryConfiguration config;
    private static MyMockMetadataHelper metadataHelper;
    
    @BeforeClass
    public static void setup() throws Exception {
        conn = instance.getConnector("", new PasswordToken(new byte[0]));
        
        createTables();
        writeShardIndexData();
        writeShardReverseIndexData();
        createQueryModel();
    }
    
    private static void createTables() throws TableExistsException, AccumuloSecurityException, AccumuloException {
        conn.tableOperations().create(TableName.SHARD_INDEX);
        conn.tableOperations().create(TableName.SHARD_RINDEX);
        conn.tableOperations().create(TableName.METADATA);
    }
    
    private static void writeShardIndexData() throws TableNotFoundException, MutationsRejectedException {
        try (BatchWriter bw = conn.createBatchWriter(TableName.SHARD_INDEX, new BatchWriterConfig().setMaxLatency(10, TimeUnit.SECONDS).setMaxMemory(100000L)
                        .setMaxWriteThreads(1))) {
            
            Mutation m = new Mutation("taco");
            m.put(new Text("FIELD1"), new Text("20210101\0datatype"), timestamp, emptyValue);
            m.put(new Text("FIELD2"), new Text("20210102\0datatype"), timestamp, emptyValue);
            m.put(new Text("FIELD3"), new Text("20210103\0datatype"), timestamp, emptyValue);
            
            bw.addMutation(m);
            
            m = new Mutation("burrito");
            m.put(new Text("FIELD1"), new Text("20210101\0datatype"), timestamp, emptyValue);
            m.put(new Text("FIELD1"), new Text("20210102\0datatype"), timestamp, emptyValue);
            m.put(new Text("FIELD1"), new Text("20210103\0datatype"), timestamp, emptyValue);
            m.put(new Text("FIELD1"), new Text("20210104\0datatype"), timestamp, emptyValue);
            m.put(new Text("FIELD1"), new Text("20210105\0datatype"), timestamp, emptyValue);
            
            m.put(new Text("FIELD2"), new Text("20210102\0datatype"), timestamp, emptyValue);
            m.put(new Text("FIELD2"), new Text("20210103\0datatype"), timestamp, emptyValue);
            
            m.put(new Text("FIELD3"), new Text("20210103\0datatype"), timestamp, emptyValue);
            
            m.put(new Text("FIELD4"), new Text("20210104\0datatype"), timestamp, emptyValue);
            
            bw.addMutation(m);
            indexedFields.addAll(Sets.newHashSet("FIELD1", "FIELD2", "FIELD3", "FIELD4"));
            
            // some mutations for regex expansion
            
            m = new Mutation("dog");
            m.put(new Text("FIELD7"), new Text("20210101\0datatype"), timestamp, emptyValue);
            m.put(new Text("FIELD8"), new Text("20210102\0datatype"), timestamp, emptyValue);
            m.put(new Text("FIELD9"), new Text("20210103\0datatype"), timestamp, emptyValue);
            bw.addMutation(m);
            
            m = new Mutation("dogfish");
            m.put(new Text("FIELD7"), new Text("20210101\0datatype"), timestamp, emptyValue);
            m.put(new Text("FIELD8"), new Text("20210102\0datatype"), timestamp, emptyValue);
            m.put(new Text("FIELD9"), new Text("20210103\0datatype"), timestamp, emptyValue);
            bw.addMutation(m);
            
            m = new Mutation("doghouse");
            m.put(new Text("FIELD7"), new Text("20210101\0datatype"), timestamp, emptyValue);
            m.put(new Text("FIELD8"), new Text("20210102\0datatype"), timestamp, emptyValue);
            m.put(new Text("FIELD9"), new Text("20210103\0datatype"), timestamp, emptyValue);
            bw.addMutation(m);
            
            indexedFields.addAll(Sets.newHashSet("FIELD7", "FIELD8", "FIELD9"));
        }
    }
    
    private static void writeShardReverseIndexData() throws TableNotFoundException, MutationsRejectedException {
        try (BatchWriter bw = conn.createBatchWriter(TableName.SHARD_RINDEX, new BatchWriterConfig().setMaxLatency(10, TimeUnit.SECONDS).setMaxMemory(100000L)
                        .setMaxWriteThreads(1))) {
            
            Mutation m = new Mutation(StringUtils.reverse("beautifully"));
            m.put(new Text("FIELD7"), new Text("20210101\0datatype"), timestamp, emptyValue);
            
            bw.addMutation(m);
            
            m = new Mutation(StringUtils.reverse("fearfully"));
            m.put(new Text("FIELD8"), new Text("20210101\0datatype"), timestamp, emptyValue);
            
            bw.addMutation(m);
            
            m = new Mutation(StringUtils.reverse("wonderfully"));
            m.put(new Text("FIELD9"), new Text("20210101\0datatype"), timestamp, emptyValue);
            
            bw.addMutation(m);
            
            reverseFields.addAll(Sets.newHashSet("FIELD7", "FIELD8", "FIELD9"));
        }
    }
    
    private static void createQueryModel() {
        model = new QueryModel();
        addModelMappings(model, "ALIAS1", Arrays.asList("FIELD1", "FIELD3"));
    }
    
    private static void addModelMappings(QueryModel model, String alias, Collection<String> fields) {
        for (String field : fields) {
            model.addTermToModel(alias, field);
        }
    }
    
    @Before
    public void setupTest() {
        config = createConfig();
        metadataHelper = createMetadataHelper();
    }
    
    private ShardQueryConfiguration createConfig() {
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setConnector(conn);
        config.setDatatypeFilter(Sets.newHashSet("datatype"));
        
        // default date range
        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));
        
        return config;
    }
    
    private MyMockMetadataHelper createMetadataHelper() {
        MyMockMetadataHelper metadataHelper = new MyMockMetadataHelper();
        metadataHelper.setIndexedFields(indexedFields);
        metadataHelper.setReverseIndexFields(reverseFields);
        return metadataHelper;
    }
    
    // anyfield single term expands into four fields
    @Test
    public void testSingleTermMatches() throws Exception {
        String query = "_ANYFIELD_ == 'burrito'";
        String expected = "FIELD1 == 'burrito' || FIELD2 == 'burrito' || FIELD3 == 'burrito' || FIELD4 == 'burrito'";
        test(query, expected);
    }
    
    // single term expansion restricted by query model
    @Test
    public void testSingleTermMatchesRestrictedByQueryModel() throws Exception {
        ShardQueryConfiguration config = createConfig();
        // limit expansion to model
        config.setModelName("MODEL");
        config.setModelTableName(TableName.METADATA);
        config.setLimitTermExpansionToModel(true);
        
        MyMockMetadataHelper helper = createMetadataHelper();
        helper.setQueryModel("MODEL", model);
        
        String query = "_ANYFIELD_ == 'burrito'";
        String expected = "FIELD1 == 'burrito' || FIELD3 == 'burrito'";
        
        test(query, expected, config, helper);
    }
    
    // single term expansion where we explicitly set the expansion fields
    @Test
    public void testSingleTermMatchesWithExpansionFields() {
        try {
            String query = "_ANYFIELD_ == 'burrito'";
            String expected = "FIELD1 == 'burrito' || FIELD2 == 'burrito'";
            
            ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
            ScannerFactory scannerFactory = new ScannerFactory(config.getConnector());
            metadataHelper.addExpansionFields(ImmutableSet.of("FIELD1", "FIELD2"));
            ASTJexlScript fixed = UnfieldedIndexExpansionVisitor.expandUnfielded(config, scannerFactory, metadataHelper, script);
            
            // assert and validate
            assertTrue(JexlASTHelper.validateLineage(fixed, false));
            
            // the order of query terms is non-deterministic so use TreeEqualityVisitor over comparing raw query strings
            ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
            assertTrue(TreeEqualityVisitor.isEqual(expectedScript, fixed));
            
        } catch (Exception e) {
            fail("Test failed with exception " + e.getMessage());
        }
    }
    
    // Should replace failed expansion with _NOFIELD_
    @Test
    public void testSingleTermMatchesWithMisMatchedExpansionFields() {
        try {
            String query = "_ANYFIELD_ == 'burrito'";
            String expected = "_NOFIELD_ == 'burrito'";
            
            ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
            ScannerFactory scannerFactory = new ScannerFactory(config.getConnector());
            metadataHelper.addExpansionFields(ImmutableSet.of("FOOBAR"));
            ASTJexlScript fixed = UnfieldedIndexExpansionVisitor.expandUnfielded(config, scannerFactory, metadataHelper, script);
            
            // assert and validate
            assertTrue(JexlASTHelper.validateLineage(fixed, false));
            
            // the order of query terms is non-deterministic so use TreeEqualityVisitor over comparing raw query strings
            ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
            assertTrue(TreeEqualityVisitor.isEqual(expectedScript, fixed));
            
        } catch (Exception e) {
            fail("Test failed with exception " + e.getMessage());
        }
    }
    
    // term 'icecream' is not found in the data, produces a _NOFIELD_ marker
    @Test
    public void testNoMatch() throws Exception {
        String query = "_ANYFIELD_ == 'icecream'";
        String expected = "_NOFIELD_ == 'icecream'";
        test(query, expected);
    }
    
    // term 'taco' gets expanded out to FIELD[1,3], 'missingterm' gets a _NOFIELD_ marker
    @Test
    public void testMultipleTermsMatch() throws Exception {
        String query = "_ANYFIELD_ == 'taco' || _ANYFIELD_ == 'missingterm'";
        String expected = "FIELD1 == 'taco' || FIELD2 == 'taco' || FIELD3 == 'taco' || _NOFIELD_ == 'missingterm'";
        test(query, expected);
    }
    
    // multiple terms, one matches and is expanded. Missing term is marked with a _NOFIELD_ marker
    @Test
    public void testMultipleTermsMatchRestrictedByModel() throws Exception {
        ShardQueryConfiguration config = createConfig();
        // limit expansion to model
        config.setModelName("MODEL");
        config.setModelTableName(TableName.METADATA);
        config.setLimitTermExpansionToModel(true);
        
        MyMockMetadataHelper helper = createMetadataHelper();
        helper.setQueryModel("MODEL", model);
        
        String query = "_ANYFIELD_ == 'taco' || _ANYFIELD_ == 'missingterm'";
        String expected = "FIELD1 == 'taco' || FIELD3 == 'taco' || _NOFIELD_ == 'missingterm'";
        test(query, expected, config, helper);
    }
    
    // regex term fails to expand fields, marked with an exceeded term threshold marker
    @Test
    public void testExceededTermThreshold() throws Exception {
        ShardQueryConfiguration config = createConfig();
        config.setMaxUnfieldedExpansionThreshold(2);
        
        String query = "_ANYFIELD_ =~ 'dog.*'";
        String expected = "((_Term_ = true) && (_ANYFIELD_ =~ 'dog.*'))";
        test(query, expected, config);
    }
    
    // regex term expands into fields, but values expansion fails by exceeding a threshold.
    // a conjunction of fielded exceeded value markers is created.
    @Test
    public void testExceededValueThreshold() throws Exception {
        ShardQueryConfiguration config = createConfig();
        config.setMaxUnfieldedExpansionThreshold(3); // 3 fields <= max threshold
        config.setMaxValueExpansionThreshold(1); // values per field exceed threshold, preserving regex
        
        String query = "_ANYFIELD_ =~ 'dog.*'";
        String expected = "((_Value_ = true) && (FIELD7 =~ 'dog.*')) || ((_Value_ = true) && (FIELD8 =~ 'dog.*')) || ((_Value_ = true) && (FIELD9 =~ 'dog.*'))";
        test(query, expected, config);
    }
    
    // negated regex for term that does not exist, persist the _ANYFIELD_ marker
    @Test
    public void testNegatedRegexIsMarkedNoField() throws Exception {
        String query = "FOO == 'bar' && _ANYFIELD_ !~ 'dolos.*'";
        test(query, query);
    }
    
    // negated term that does not exist, persist the _ANYFIELD_ marker
    @Test
    public void testNegatedTermIsMarkedNoField() throws Exception {
        String query = "FOO == 'bar' && _ANYFIELD_ != 'tacocat'";
        test(query, query);
    }
    
    // alternate form of a negated term that does not exist, persist the _ANYFIELD_ marker
    @Test
    public void testNegatedTermIsMarkedNoFieldAlternateForm() throws Exception {
        String query = "FOO == 'bar' && !(_ANYFIELD_ == 'tacocat')";
        test(query, query);
    }
    
    // leading regex '.*?ly' term expands into *-ly terms via the shard reverse index
    @Test
    public void testLeadingRegexExpansionViaReverseIndex() throws Exception {
        String query = "_ANYFIELD_ =~ '.*?ly'";
        String expected = "FIELD7 == 'beautifully' || FIELD8 == 'fearfully' || FIELD9 == 'wonderfully'";
        test(query, expected);
    }
    
    // negated term that is a leading regex, original node is preserved
    @Test
    public void testNegatedLeadingRegexExpansionViaReverseIndex() throws Exception {
        String query = "_ANYFIELD_ !~ '.*?ly'";
        String expected = "(_ANYFIELD_ !~ '.*?ly' && FIELD7 != 'beautifully' && FIELD8 != 'fearfully' && FIELD9 != 'wonderfully')";
        test(query, expected);
    }
    
    // alternate form of a negated term that is a leading regex, original node is preserved
    @Test
    public void testNegatedLeadingRegexExpansionViaReverseIndexAlternateForm() throws Exception {
        String query = "!(_ANYFIELD_ =~ '.*?ly')";
        String expected = "!(_ANYFIELD_ =~ '.*?ly' || FIELD7 == 'beautifully' || FIELD8 == 'fearfully' || FIELD9 == 'wonderfully')";
        test(query, expected);
    }
    
    // negated reverse index expansion, expansion restricted by a set of fields
    @Test
    public void testNegatedLeadingRegexExpansionRestrictedByExpansionFields() {
        try {
            String query = "_ANYFIELD_ !~ '.*?ly'";
            String expected = "(_ANYFIELD_ !~ '.*?ly' && FIELD8 != 'fearfully' && FIELD9 != 'wonderfully')";
            
            ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
            ScannerFactory scannerFactory = new ScannerFactory(config.getConnector());
            metadataHelper.addExpansionFields(ImmutableSet.of("FIELD8", "FIELD9"));
            ASTJexlScript fixed = UnfieldedIndexExpansionVisitor.expandUnfielded(config, scannerFactory, metadataHelper, script);
            
            // assert and validate
            assertTrue(JexlASTHelper.validateLineage(fixed, false));
            
            // the order of query terms is non-deterministic so use TreeEqualityVisitor over comparing raw query strings
            ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
            assertTrue(TreeEqualityVisitor.isEqual(expectedScript, fixed));
            
        } catch (Exception e) {
            fail("Test failed with exception " + e.getMessage());
        }
    }
    
    // +------------------+
    // | additional tests |
    // +------------------+
    
    // one term is expanded, one term is marked _NOFIELD_ and the isNotNull filter is persisted
    @Test
    public void testNotNullWithNonIndexedAnd() throws Exception {
        String query = "_ANYFIELD_ == 'taco' && _ANYFIELD_ == 'icecream' && not(filter:isNull(FIELD1))";
        String expected = "(FIELD1 == 'taco' || FIELD2 == 'taco' || FIELD3 == 'taco') && _NOFIELD_ == 'icecream' && not(filter:isNull(FIELD1))";
        test(query, expected);
    }
    
    // double-ended wildcards cannot be expanded
    @Test(expected = DoNotPerformOptimizedQueryException.class)
    public void testUnresolvableRegexWithoutFullTableScan() throws Exception {
        ShardQueryConfiguration config = createConfig();
        config.setFullTableScanEnabled(false);
        
        String query = "_ANYFIELD_ =~ '.*?teehee.*'";
        test(query, query, config);
    }
    
    // double-ended wildcards under a negation cannot be expanded
    @Test(expected = DoNotPerformOptimizedQueryException.class)
    public void testUnresolvableRegexWithoutFullTableScanAlternateForm() throws Exception {
        ShardQueryConfiguration config = createConfig();
        config.setFullTableScanEnabled(false);
        
        String query = "!(_ANYFIELD_ =~ '.*?teehee.*')";
        test(query, query, config);
    }
    
    public void test(String query, String expected) throws Exception {
        test(query, expected, config);
    }
    
    public void test(String query, String expected, ShardQueryConfiguration config) throws Exception {
        test(query, expected, config, metadataHelper);
    }
    
    // throw exception to assert failure cases, such as CannotExpandUnfieldedTermFatalException
    public void test(String query, String expected, ShardQueryConfiguration config, MockMetadataHelper metadataHelper) throws Exception {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        ScannerFactory scannerFactory = new ScannerFactory(config.getConnector());
        ASTJexlScript fixed = UnfieldedIndexExpansionVisitor.expandUnfielded(config, scannerFactory, metadataHelper, script);
        
        // assert and validate
        assertTrue(JexlASTHelper.validateLineage(fixed, false));
        
        // the order of query terms is non-deterministic so use TreeEqualityVisitor over comparing raw query strings
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        String errMsg = "Expected trees to be equal:\n" + JexlStringBuildingVisitor.buildQueryWithoutParse(expectedScript) + "\n"
                        + JexlStringBuildingVisitor.buildQueryWithoutParse(fixed);
        assertTrue(errMsg, TreeEqualityVisitor.isEqual(expectedScript, fixed));
    }
    
    // utility class created to get around some exception finding composite terms
    class MyMockMetadataHelper extends MockMetadataHelper {
        
        public MyMockMetadataHelper() {
            super();
        }
        
        @Override
        public Multimap<String,String> getCompositeToFieldMap() throws TableNotFoundException {
            return null;
        }
        
        // assume that all reverse fields are indexed, for the purposes of this test
        @Override
        public Set<String> getReverseIndexedFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
            return reverseFields;
        }
    }
}
