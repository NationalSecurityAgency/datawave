package datawave.query.jexl.visitors;

import com.google.common.collect.Sets;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.DateIndexHelperFactory;
import datawave.query.util.DateIndexTestIngest;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.query.util.MockDateIndexHelper;
import datawave.query.util.MockMetadataHelper;
import datawave.test.JexlNodeAssert;
import datawave.util.TableName;
import datawave.util.time.DateHelper;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.TimeZone;

public class FunctionIndexQueryExpansionVisitorTest {
    
    private ShardQueryConfiguration config;
    
    private MetadataHelper metadataHelper;
    
    private DateIndexHelper dateIndexHelper;
    
    @Before
    public void setUp() throws Exception {
        config = new ShardQueryConfiguration();
        metadataHelper = new MockMetadataHelper();
        dateIndexHelper = new MockDateIndexHelper();
    }
    
    @Test
    public void expandContentFields() throws ParseException {
        Set<String> fields = Sets.newHashSet("FOO", "BAR");
        
        // Configure the mock metadata helper.
        MockMetadataHelper mockMetadataHelper = new MockMetadataHelper();
        mockMetadataHelper.setIndexedFields(fields);
        mockMetadataHelper.addTermFrequencyFields(fields);
        this.metadataHelper = mockMetadataHelper;
        
        // Execute the test.
        String original = "content:phrase(termOffsetMap, 'abc', 'def')";
        String expected = "(content:phrase(termOffsetMap, 'abc', 'def') && ((BAR == 'def' && BAR == 'abc') || (FOO == 'def' && FOO == 'abc')))";
        
        runTest(original, expected);
    }
    
    @Test
    public void expandContentPhraseFunctionIntoSingleField() throws ParseException {
        Set<String> fields = Sets.newHashSet("FOO", "BAR");
        Set<String> tfFields = Sets.newHashSet("FOO");
        
        // Configure the mock metadata helper.
        MockMetadataHelper mockMetadataHelper = new MockMetadataHelper();
        mockMetadataHelper.setIndexedFields(fields);
        mockMetadataHelper.addTermFrequencyFields(tfFields);
        this.metadataHelper = mockMetadataHelper;
        
        // Execute the test.
        String original = "content:phrase(termOffsetMap, 'abc', 'def')";
        String expected = "(content:phrase(termOffsetMap, 'abc', 'def') && (FOO == 'def' && FOO == 'abc'))";
        
        runTest(original, expected);
    }
    
    @Test
    public void expandContentPhraseFunctionIntoMultipleFields() throws ParseException {
        Set<String> fields = Sets.newHashSet("FOO", "BAR");
        Set<String> tfFields = Sets.newHashSet("FOO", "BAR");
        
        // Configure the mock metadata helper.
        MockMetadataHelper mockMetadataHelper = new MockMetadataHelper();
        mockMetadataHelper.setIndexedFields(fields);
        mockMetadataHelper.addTermFrequencyFields(tfFields);
        
        // Execute the test.
        String original = "content:phrase(termOffsetMap, 'abc', 'def')";
        String expected = "(content:phrase(termOffsetMap, 'abc', 'def') && ((BAR == 'def' && BAR == 'abc') || (FOO == 'def' && FOO == 'abc')))";
        
        runTest(original, expected, mockMetadataHelper);
    }
    
    // no expansion function also applies to index query expansion
    @Test
    public void expandContentPhraseFunctionIntoMultipleFieldsWithNoExpansion() throws ParseException {
        Set<String> fields = Sets.newHashSet("FOO", "BAR");
        Set<String> tfFields = Sets.newHashSet("FOO", "BAR");
        
        // Configure the mock metadata helper.
        MockMetadataHelper mockMetadataHelper = new MockMetadataHelper();
        mockMetadataHelper.setIndexedFields(fields);
        mockMetadataHelper.addTermFrequencyFields(tfFields);
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setNoExpansionFields(Sets.newHashSet("BAR"));
        
        // Execute the test.
        String original = "content:phrase(termOffsetMap, 'abc', 'def')";
        String expected = "(content:phrase(termOffsetMap, 'abc', 'def') && (FOO == 'def' && FOO == 'abc'))";
        
        runTest(original, expected, config, mockMetadataHelper);
    }
    
    @Test
    public void expandContentPhraseFunctionIntoSingleFieldWithNoExpansion() throws ParseException {
        Set<String> fields = Sets.newHashSet("FOO", "BAR");
        Set<String> tfFields = Sets.newHashSet("FOO");
        
        // Configure the mock metadata helper.
        MockMetadataHelper mockMetadataHelper = new MockMetadataHelper();
        mockMetadataHelper.setIndexedFields(fields);
        mockMetadataHelper.addTermFrequencyFields(tfFields);
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setNoExpansionFields(Sets.newHashSet("FOO"));
        
        // Execute the test.
        String original = "content:phrase(termOffsetMap, 'abc', 'def')";
        
        runTest(original, original, config, mockMetadataHelper);
    }
    
    @Test
    public void expandDateIndex() throws Exception {
        // Set the default TimeZone.
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        
        // Create an in-memory accumulo instance and write the data index table.
        InMemoryInstance instance = new InMemoryInstance(FunctionIndexQueryExpansionVisitorTest.class.getName());
        Connector connector = instance.getConnector("root", new PasswordToken(""));
        connector.tableOperations().create(TableName.DATE_INDEX);
        DateIndexTestIngest.writeItAll(connector);
        
        // Configure the helpers.
        Authorizations auths = new Authorizations("HUSH");
        metadataHelper = new MetadataHelperFactory().createMetadataHelper(connector, TableName.DATE_INDEX, Collections.singleton(auths));
        dateIndexHelper = new DateIndexHelperFactory().createDateIndexHelper().initialize(connector, TableName.DATE_INDEX, Collections.singleton(auths), 2,
                        0.9f);
        
        // Configure the shard query configuration.
        config.setBeginDate(DateHelper.parse("20100701"));
        config.setEndDate(DateHelper.parse("20100710"));
        
        // Execute the test.
        String original = "filter:betweenDates(UPTIME, '20100704_200000', '20100704_210000')";
        String expected = "(filter:betweenDates(UPTIME, '20100704_200000', '20100704_210000') && (SHARDS_AND_DAYS = '20100703_0,20100704_0,20100704_2,20100705_1'))";
        
        runTest(original, expected);
    }
    
    private void runTest(String originalQuery, String expected) throws ParseException {
        runTest(originalQuery, expected, config, metadataHelper, dateIndexHelper);
    }
    
    private void runTest(String originalQuery, String expected, ShardQueryConfiguration config) throws ParseException {
        runTest(originalQuery, expected, config, metadataHelper, dateIndexHelper);
    }
    
    private void runTest(String originalQuery, String expected, MetadataHelper metadataHelper) throws ParseException {
        runTest(originalQuery, expected, config, metadataHelper, dateIndexHelper);
    }
    
    private void runTest(String originalQuery, String expected, ShardQueryConfiguration config, MetadataHelper metadataHelper) throws ParseException {
        runTest(originalQuery, expected, config, metadataHelper, dateIndexHelper);
    }
    
    private void runTest(String originalQuery, String expected, ShardQueryConfiguration config, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper)
                    throws ParseException {
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(originalQuery);
        
        ASTJexlScript actualScript = FunctionIndexQueryExpansionVisitor.expandFunctions(config, metadataHelper, dateIndexHelper, originalScript);
        
        // Verify the script is as expected, and has a valid lineage.
        JexlNodeAssert.assertThat(actualScript).isEqualTo(expected).hasValidLineage();
        
        // Verify that the original script was not modified.
        JexlNodeAssert.assertThat(originalScript).isEqualTo(originalQuery).hasValidLineage();
    }
}
