package datawave.query.jexl.visitors;

import com.google.common.collect.Sets;
import datawave.accumulo.inmemory.InMemoryAccumuloClient;
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
    public void expandContentPhraseFunctionIntoSingleField() throws ParseException {
        Set<String> fields = Sets.newHashSet("FOO");
        
        // Configure the mock metadata helper.
        MockMetadataHelper mockMetadataHelper = new MockMetadataHelper();
        mockMetadataHelper.setIndexedFields(fields);
        mockMetadataHelper.addTermFrequencyFields(fields);
        this.metadataHelper = mockMetadataHelper;
        
        // Execute the test.
        String original = "content:phrase(termOffsetMap, 'abc', 'def')";
        String expected = "(content:phrase(FOO, termOffsetMap, 'abc', 'def') && FOO == 'def' && FOO == 'abc')";
        runTest(original, expected);
        
        original = "content:phrase(FOO, termOffsetMap, 'abc', 'def')";
        expected = "(content:phrase(FOO, termOffsetMap, 'abc', 'def') && FOO == 'def' && FOO == 'abc')";
        runTest(original, expected);
    }
    
    @Test
    public void expandContentPhraseFunctionIntoMultipleFields() throws ParseException {
        Set<String> fields = Sets.newHashSet("FOO", "BAR");
        
        // Configure the mock metadata helper.
        MockMetadataHelper mockMetadataHelper = new MockMetadataHelper();
        mockMetadataHelper.setIndexedFields(fields);
        mockMetadataHelper.addTermFrequencyFields(fields);
        this.metadataHelper = mockMetadataHelper;
        
        // Execute the test.
        String original = "content:phrase(termOffsetMap, 'abc', 'def')";
        String expected = "((content:phrase(FOO, termOffsetMap, 'abc', 'def') && FOO == 'def' && FOO == 'abc') || (content:phrase(BAR, termOffsetMap, 'abc', 'def') && BAR == 'def' && BAR == 'abc'))";
        runTest(original, expected);
        
        original = "content:phrase((FOO || BAR), termOffsetMap, 'abc', 'def')";
        expected = "((content:phrase(FOO, termOffsetMap, 'abc', 'def') && FOO == 'def' && FOO == 'abc') || (content:phrase(BAR, termOffsetMap, 'abc', 'def') && BAR == 'def' && BAR == 'abc'))";
        runTest(original, expected, mockMetadataHelper);
    }
    
    @Test
    public void expandContentPhraseFunctionWithRepeatedTermIntoSingleField() throws ParseException {
        Set<String> fields = Sets.newHashSet("FOO");
        
        // Configure the mock metadata helper.
        MockMetadataHelper mockMetadataHelper = new MockMetadataHelper();
        mockMetadataHelper.setIndexedFields(fields);
        mockMetadataHelper.addTermFrequencyFields(fields);
        this.metadataHelper = mockMetadataHelper;
        
        // Execute the test.
        String original = "content:phrase(termOffsetMap, 'abc', 'abc')";
        String expected = "(content:phrase(FOO, termOffsetMap, 'abc', 'abc') && FOO == 'abc')";
        runTest(original, expected);
        
        original = "content:phrase(FOO, termOffsetMap, 'abc', 'abc')";
        expected = "(content:phrase(FOO, termOffsetMap, 'abc', 'abc') && FOO == 'abc')";
        runTest(original, expected);
    }
    
    @Test
    public void expandContentPhraseFunctionWithRepeatedTermIntoMultipleFields() throws ParseException {
        Set<String> fields = Sets.newHashSet("FOO", "BAR");
        
        // Configure the mock metadata helper.
        MockMetadataHelper mockMetadataHelper = new MockMetadataHelper();
        mockMetadataHelper.setIndexedFields(fields);
        mockMetadataHelper.addTermFrequencyFields(fields);
        this.metadataHelper = mockMetadataHelper;
        
        // Execute the test.
        String original = "content:phrase(termOffsetMap, 'abc', 'abc')";
        String expected = "((content:phrase(BAR, termOffsetMap, 'abc', 'abc') && BAR == 'abc') || (content:phrase(FOO, termOffsetMap, 'abc', 'abc') && FOO == 'abc'))";
        runTest(original, expected);
        
        original = "content:phrase((FOO || BAR), termOffsetMap, 'abc', 'abc')";
        expected = "((content:phrase(BAR, termOffsetMap, 'abc', 'abc') && BAR == 'abc') || (content:phrase(FOO, termOffsetMap, 'abc', 'abc') && FOO == 'abc'))";
        runTest(original, expected);
    }
    
    @Test
    public void expandContentFunctionWithRepeatedValues() throws ParseException {
        Set<String> fields = Sets.newHashSet("FOO");
        
        // Configure the mock metadata helper.
        MockMetadataHelper mockMetadataHelper = new MockMetadataHelper();
        mockMetadataHelper.setIndexedFields(fields);
        mockMetadataHelper.addTermFrequencyFields(fields);
        this.metadataHelper = mockMetadataHelper;
        
        // Execute the test.
        String original = "content:phrase(termOffsetMap, 'run', 'spot', 'run')";
        String expected = "(content:phrase(FOO, termOffsetMap, 'run', 'spot', 'run') && (FOO == 'run' && FOO == 'spot'))";
        
        runTest(original, expected);
    }
    
    @Test
    public void expandContentAdjacentFunctionIntoSingleField() throws ParseException {
        Set<String> fields = Sets.newHashSet("FOO", "BAR");
        Set<String> tfFields = Sets.newHashSet("FOO");
        
        // Configure the mock metadata helper.
        MockMetadataHelper mockMetadataHelper = new MockMetadataHelper();
        mockMetadataHelper.setIndexedFields(fields);
        mockMetadataHelper.addTermFrequencyFields(tfFields);
        this.metadataHelper = mockMetadataHelper;
        
        // Execute the test.
        String original = "content:adjacent(termOffsetMap, 'abc', 'def')";
        String expected = "(content:adjacent(FOO, termOffsetMap, 'abc', 'def') && FOO == 'def' && FOO == 'abc')";
        runTest(original, expected);
        
        original = "content:adjacent(FOO, termOffsetMap, 'abc', 'def')";
        expected = "(content:adjacent(FOO, termOffsetMap, 'abc', 'def') && FOO == 'def' && FOO == 'abc')";
        runTest(original, expected);
    }
    
    @Test
    public void expandContentAdjacentFunctionIntoMultipleFields() throws ParseException {
        Set<String> fields = Sets.newHashSet("FOO", "BAR");
        Set<String> tfFields = Sets.newHashSet("FOO", "BAR");
        
        // Configure the mock metadata helper.
        MockMetadataHelper mockMetadataHelper = new MockMetadataHelper();
        mockMetadataHelper.setIndexedFields(fields);
        mockMetadataHelper.addTermFrequencyFields(tfFields);
        this.metadataHelper = mockMetadataHelper;
        
        // Execute the test.
        String original = "content:adjacent(termOffsetMap, 'abc', 'def')";
        String expected = "((content:adjacent(FOO, termOffsetMap, 'abc', 'def') && FOO == 'def' && FOO == 'abc') || (content:adjacent(BAR, termOffsetMap, 'abc', 'def') && BAR == 'def' && BAR == 'abc'))";
        runTest(original, expected);
        
        original = "content:adjacent((FOO || BAR), termOffsetMap, 'abc', 'def')";
        expected = "((content:adjacent(FOO, termOffsetMap, 'abc', 'def') && FOO == 'def' && FOO == 'abc') || (content:adjacent(BAR, termOffsetMap, 'abc', 'def') && BAR == 'def' && BAR == 'abc'))";
        runTest(original, expected);
    }
    
    @Test
    public void expandContentWithinFunctionIntoSingleField() throws ParseException {
        Set<String> fields = Sets.newHashSet("FOO", "BAR");
        Set<String> tfFields = Sets.newHashSet("FOO");
        
        // Configure the mock metadata helper.
        MockMetadataHelper mockMetadataHelper = new MockMetadataHelper();
        mockMetadataHelper.setIndexedFields(fields);
        mockMetadataHelper.addTermFrequencyFields(tfFields);
        this.metadataHelper = mockMetadataHelper;
        
        // Execute the test.
        String original = "content:within(2, termOffsetMap, 'abc', 'def')";
        String expected = "(content:within(FOO, 2, termOffsetMap, 'abc', 'def') && (FOO == 'def' && FOO == 'abc'))";
        runTest(original, expected);
        
        original = "content:within(FOO, 2, termOffsetMap, 'abc', 'def')";
        expected = "(content:within(FOO, 2, termOffsetMap, 'abc', 'def') && (FOO == 'def' && FOO == 'abc'))";
        runTest(original, expected);
    }
    
    @Test
    public void expandContentWithinFunctionIntoMultipleFields() throws ParseException {
        Set<String> fields = Sets.newHashSet("FOO", "BAR");
        Set<String> tfFields = Sets.newHashSet("FOO", "BAR");
        
        // Configure the mock metadata helper.
        MockMetadataHelper mockMetadataHelper = new MockMetadataHelper();
        mockMetadataHelper.setIndexedFields(fields);
        mockMetadataHelper.addTermFrequencyFields(tfFields);
        this.metadataHelper = mockMetadataHelper;
        
        // Execute the test.
        String original = "content:within(2, termOffsetMap, 'abc', 'def')";
        String expected = "(content:within(FOO, 2, termOffsetMap, 'abc', 'def') && (FOO == 'def' && FOO == 'abc')) || (content:within(BAR, 2, termOffsetMap, 'abc', 'def') && (BAR == 'def' && BAR == 'abc'))";
        runTest(original, expected);
        
        original = "content:within((FOO || BAR), 2, termOffsetMap, 'abc', 'def')";
        expected = "(content:within(FOO, 2, termOffsetMap, 'abc', 'def') && (FOO == 'def' && FOO == 'abc')) || (content:within(BAR, 2, termOffsetMap, 'abc', 'def') && (BAR == 'def' && BAR == 'abc'))";
        runTest(original, expected);
    }
    
    @Test
    public void expandContentScoredPhraseFunctionIntoSingleField() throws ParseException {
        Set<String> fields = Sets.newHashSet("FOO", "BAR");
        Set<String> tfFields = Sets.newHashSet("FOO");
        
        // Configure the mock metadata helper.
        MockMetadataHelper mockMetadataHelper = new MockMetadataHelper();
        mockMetadataHelper.setIndexedFields(fields);
        mockMetadataHelper.addTermFrequencyFields(tfFields);
        this.metadataHelper = mockMetadataHelper;
        
        // Execute the test.
        String original = "content:scoredPhrase(-1.5, termOffsetMap, 'abc', 'def')";
        String expected = "(content:scoredPhrase(FOO, -1.5, termOffsetMap, 'abc', 'def') && FOO == 'def' && FOO == 'abc')";
        runTest(original, expected);
        
        original = "content:scoredPhrase(FOO, -1.5, termOffsetMap, 'abc', 'def')";
        expected = "(content:scoredPhrase(FOO, -1.5, termOffsetMap, 'abc', 'def') && FOO == 'def' && FOO == 'abc')";
        runTest(original, expected);
    }
    
    @Test
    public void expandContentScoredPhraseFunctionIntoMultipleFields() throws ParseException {
        Set<String> fields = Sets.newHashSet("FOO", "BAR");
        Set<String> tfFields = Sets.newHashSet("FOO", "BAR");
        
        // Configure the mock metadata helper.
        MockMetadataHelper mockMetadataHelper = new MockMetadataHelper();
        mockMetadataHelper.setIndexedFields(fields);
        mockMetadataHelper.addTermFrequencyFields(tfFields);
        this.metadataHelper = mockMetadataHelper;
        
        // Execute the test.
        String original = "content:scoredPhrase(-1.5, termOffsetMap, 'abc', 'def')";
        String expected = "(content:scoredPhrase(FOO, -1.5, termOffsetMap, 'abc', 'def') && FOO == 'def' && FOO == 'abc') || (content:scoredPhrase(BAR, -1.5, termOffsetMap, 'abc', 'def') && BAR == 'def' && BAR == 'abc')";
        runTest(original, expected);
    }
    
    // scored phrase does not support 'scoredPhrase(Iterable, ...)'
    @Test(expected = IllegalArgumentException.class)
    public void expandContentScoredPhraseFunctionIntoMultipleFields_exception() throws ParseException {
        Set<String> fields = Sets.newHashSet("FOO", "BAR");
        Set<String> tfFields = Sets.newHashSet("FOO", "BAR");
        
        // Configure the mock metadata helper.
        MockMetadataHelper mockMetadataHelper = new MockMetadataHelper();
        mockMetadataHelper.setIndexedFields(fields);
        mockMetadataHelper.addTermFrequencyFields(tfFields);
        this.metadataHelper = mockMetadataHelper;
        
        String original = "content:scoredPhrase((FOO || BAR), -1.5, termOffsetMap, 'abc', 'def')";
        String expected = "(content:scoredPhrase(FOO, -1.5, termOffsetMap, 'abc', 'def') && FOO == 'def' && FOO == 'abc') || (content:scoredPhrase(BAR, -1.5, termOffsetMap, 'abc', 'def') && BAR == 'def' && BAR == 'abc')";
        runTest(original, expected);
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
        String expected = "(content:phrase(FOO, termOffsetMap, 'abc', 'def') && FOO == 'def' && FOO == 'abc')";
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
        InMemoryAccumuloClient client = new InMemoryAccumuloClient("root", new InMemoryInstance(FunctionIndexQueryExpansionVisitorTest.class.getName()));
        client.tableOperations().create(TableName.DATE_INDEX);
        DateIndexTestIngest.writeItAll(client);
        
        // Configure the helpers.
        Authorizations auths = new Authorizations("HUSH");
        metadataHelper = new MetadataHelperFactory().createMetadataHelper(client, TableName.DATE_INDEX, Collections.singleton(auths));
        dateIndexHelper = new DateIndexHelperFactory().createDateIndexHelper().initialize(client, TableName.DATE_INDEX, Collections.singleton(auths), 2, 0.9f);
        
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
        ASTJexlScript originalScript = JexlASTHelper.parseAndFlattenJexlQuery(originalQuery);
        
        ASTJexlScript actualScript = FunctionIndexQueryExpansionVisitor.expandFunctions(config, metadataHelper, dateIndexHelper, originalScript);
        
        // Verify the script is as expected, and has a valid lineage.
        JexlNodeAssert.assertThat(actualScript).isEqualTo(expected).hasValidLineage();
        
        // Verify that the original script was not modified.
        JexlNodeAssert.assertThat(originalScript).isEqualTo(originalQuery).hasValidLineage();
    }
}
