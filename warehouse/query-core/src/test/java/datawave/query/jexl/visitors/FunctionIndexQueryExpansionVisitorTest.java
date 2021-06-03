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
import datawave.util.TableName;
import datawave.util.time.DateHelper;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.TimeZone;

import static org.junit.Assert.assertTrue;

public class FunctionIndexQueryExpansionVisitorTest {
    
    private static final Logger log = Logger.getLogger(FunctionIndexQueryExpansionVisitorTest.class);
    
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
        ASTJexlScript originalScript = JexlASTHelper.parseJexlQuery(originalQuery);
        
        ASTJexlScript actualScript = FunctionIndexQueryExpansionVisitor.expandFunctions(config, metadataHelper, dateIndexHelper, originalScript);
        
        // Verify the script is as expected, and has a valid lineage.
        assertScriptEquality(actualScript, expected);
        assertLineage(actualScript);
        
        // Verify that the original script was not modified.
        assertScriptEquality(originalScript, originalQuery);
        assertLineage(originalScript);
    }
    
    private void assertScriptEquality(ASTJexlScript actualScript, String expected) throws ParseException {
        ASTJexlScript expectedScript = JexlASTHelper.parseJexlQuery(expected);
        TreeEqualityVisitor.Comparison comparison = TreeEqualityVisitor.checkEquality(expectedScript, actualScript);
        if (!comparison.isEqual()) {
            log.error("Expected " + PrintingVisitor.formattedQueryString(expectedScript));
            log.error("Actual " + PrintingVisitor.formattedQueryString(actualScript));
        }
        assertTrue(comparison.getReason(), comparison.isEqual());
    }
    
    private void assertLineage(JexlNode node) {
        assertTrue(JexlASTHelper.validateLineage(node, true));
    }
}
