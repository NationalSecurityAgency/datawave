package datawave.query.jexl.visitors;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.util.MockMetadataHelper;
import datawave.test.JexlNodeAssert;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static datawave.query.jexl.JexlASTHelper.parseJexlQuery;

public class CaseSensitivityVisitorTest {
    
    private final MockMetadataHelper helper = new MockMetadataHelper();
    private final ShardQueryConfiguration config = new ShardQueryConfiguration();
    
    @Before
    public void beforeTest() {
        helper.addTermFrequencyFields(Collections.singletonList("FOO"));
        helper.setIndexedFields(Collections.singleton("FOO"));
    }
    
    @Test
    public void testUpperCaseTerms() throws ParseException {
        String original = "foo == 'bar' && too == 'baz'";
        String expected = "FOO == 'bar' && TOO == 'baz'";
        runUpperCaseTest(original, expected);
    }
    
    @Test
    public void testPhraseFunction() throws ParseException {
        // Construct query with content functions found in query-core Constants.java
        String original = "foo == 'bar' && content:phrase(termOffsetMap, 'foo', 'too')";
        String expected = "FOO == 'bar' && content:phrase(termOffsetMap, 'foo', 'too')";
        runUpperCaseTest(original, expected);
    }
    
    @Test
    public void testPhraseFunctionForTfField() throws ParseException {
        // Construct query with content functions found in query-core Constants.java
        String original = "foo == 'bar' && content:phrase(foo, termOffsetMap, 'foo', 'too')";
        String expected = "FOO == 'bar' && content:phrase(FOO, termOffsetMap, 'foo', 'too')";
        runUpperCaseTest(original, expected);
    }
    
    @Test
    public void testRegexFunction() throws ParseException {
        // Construct query with content functions found in query-core Constants.java
        String original = "foo == 'bar' && filter:includeRegex(too, '.*')";
        String expected = "FOO == 'bar' && filter:includeRegex(TOO, '.*')";
        runUpperCaseTest(original, expected);
    }
    
    private void runUpperCaseTest(String original, String expected) throws ParseException {
        ASTJexlScript script = parseJexlQuery(original);
        
        CaseSensitivityVisitor.upperCaseIdentifiers(config, helper, script);
        
        JexlNodeAssert.assertThat(script).hasExactQueryString(expected);
    }
}
