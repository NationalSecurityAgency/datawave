package datawave.query.language.parser.jexl;

import datawave.query.language.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JexlControlledQueryParserTest {
    
    private JexlControlledQueryParser parser;
    
    @Test(expected = datawave.query.language.parser.ParseException.class)
    public void testExceptionWhenQueryingInvalidFields() throws ParseException {
        parser = new JexlControlledQueryParser();
        Set<String> allowedFields = new HashSet<>();
        allowedFields.add("123_4");
        allowedFields.add("123_5");
        parser.setAllowedFields(allowedFields);
        parser.parse("$999_9=='johndoe'");
    }
    
    /**
     * The query parser previously compared fields in the query (possibly with dollar signs) with fields from the metadata table (without dollar signs), so they
     * never matched.
     * 
     * @throws ParseException
     *             if we fail to parse.
     */
    @Test
    public void testAtomWithDollaBillSign() throws ParseException {
        parser = new JexlControlledQueryParser();
        Set<String> allowedFields = new HashSet<>();
        allowedFields.add("123_4");
        allowedFields.add("123_5");
        parser.setAllowedFields(allowedFields);
        Assert.assertEquals("$123_4=='johndoe'", parser.parse("$123_4=='johndoe'").getOriginalQuery());
        Assert.assertEquals(" $123_4=='johndoe'", parser.parse(" $123_4=='johndoe'").getOriginalQuery());
    }
    
    @Test
    public void testSomeoneWantsAFunction() throws ParseException {
        parser = new JexlControlledQueryParser();
        Set<String> allowedFields = new HashSet<>();
        allowedFields.add("123_4");
        allowedFields.add("123_5");
        parser.setAllowedFields(allowedFields);
        Assert.assertEquals("$123_4=='johndoe' AND filter:includeRegex($123_4, 'jo.*')",
                        parser.parse("$123_4=='johndoe' AND filter:includeRegex($123_4, 'jo.*')").getOriginalQuery());
        Assert.assertEquals(" $123_4=='johndoe' AND filter:includeRegex($123_4, 'jo.*')",
                        parser.parse(" $123_4=='johndoe' AND filter:includeRegex($123_4, 'jo.*')").getOriginalQuery());
        try {
            parser.parse("$123_4=='johndoe' AND filter:includeRegex($123_6, 'jo.*')");
            Assert.fail("failed to catch that $123_6 in the function is not an allowed field");
        } catch (ParseException ex) {
            // good!
        }
    }
    
    /**
     * MetadataQuery logic doesn't accept lowercase field names. It previously threw: datawave.query.language.parser.ParseException: Unallowed field(s)
     * '[bbb_1]' for this type of query
     *
     * It should accept these fields when checking against the allowed list.
     */
    @Test
    public void testTransformsLowerCaseBeforeComparingAgainstAllowed() throws ParseException {
        parser = new JexlControlledQueryParser();
        Set<String> allowedFields = new HashSet<>();
        allowedFields.add("BBB_1");
        allowedFields.add("B99_9");
        parser.setAllowedFields(allowedFields);
        Assert.assertEquals("bbb_1=='johndoe'", parser.parse("bbb_1=='johndoe'").getOriginalQuery());
        Assert.assertEquals(" b99_9 =='johndoe'", parser.parse(" b99_9 =='johndoe'").getOriginalQuery());
        Assert.assertEquals(" B99_9 =='johndoe'", parser.parse(" B99_9 =='johndoe'").getOriginalQuery());
    }
    
    @Test
    public void testIncludeFilterExpansion() throws ParseException {
        parser = new JexlControlledQueryParser();
        Set<String> allowedFields = new HashSet<>();
        allowedFields.add("1337_1");
        allowedFields.add("9001_1");
        parser.setAllowedFields(allowedFields);
        
        Map<String,Set<String>> includedValMap = new HashMap<>();
        Set<String> includedValSet1 = new HashSet<>();
        includedValSet1.add("John");
        includedValSet1.add("Doe");
        includedValMap.put("$1337_1", includedValSet1);
        parser.setIncludedValues(includedValMap);
        
        String expandedQuery = parser.parse("$9001_1='dudududuu'").getOriginalQuery();
        // Note: Or clause is not order dependent and may flip between jvm impls / versions.
        Assert.assertTrue("($9001_1='dudududuu') && ((filter:includeRegex($1337_1, 'Doe') || filter:includeRegex($1337_1, 'John')))".equals(expandedQuery)
                        || "($9001_1='dudududuu') && ((filter:includeRegex($1337_1, 'John') || filter:includeRegex($1337_1, 'Doe')))".equals(expandedQuery));
    }
    
    @Test
    public void testExcludeFilterExpansion() throws ParseException {
        parser = new JexlControlledQueryParser();
        Set<String> allowedFields = new HashSet<>();
        allowedFields.add("1337_1");
        allowedFields.add("9001_1");
        parser.setAllowedFields(allowedFields);
        
        Map<String,Set<String>> excludedValMap = new HashMap<>();
        Set<String> excludedValSet = new HashSet<>();
        excludedValSet.add("John");
        excludedValSet.add("Doe");
        excludedValMap.put("$1337_1", excludedValSet);
        parser.setExcludedValues(excludedValMap);
        
        String expandedQuery = parser.parse("$9001_1='dudududuu'").getOriginalQuery();
        
        // Note: Or clause is not order dependent and may flip between jvm impls / versions.
        Assert.assertTrue("($9001_1='dudududuu') && ((not(filter:includeRegex($1337_1, 'Doe')) && not(filter:includeRegex($1337_1, 'John'))))"
                        .equals(expandedQuery)
                        || "($9001_1='dudududuu') && ((not(filter:includeRegex($1337_1, 'John')) && not(filter:includeRegex($1337_1, 'Doe'))))"
                                        .equals(expandedQuery));
    }
    
    @Test
    public void testBothIncludeAndExcludeFilterExpansion() throws ParseException {
        parser = new JexlControlledQueryParser();
        Set<String> allowedFields = new HashSet<>();
        allowedFields.add("1337_1");
        allowedFields.add("9001_1");
        parser.setAllowedFields(allowedFields);
        
        Map<String,Set<String>> includedValMap = new HashMap<>();
        Set<String> includedValSet = new HashSet<>();
        includedValSet.add("John");
        includedValMap.put("$1337_1", includedValSet);
        parser.setIncludedValues(includedValMap);
        
        Map<String,Set<String>> excludedValMap = new HashMap<>();
        Set<String> excludedValSet = new HashSet<>();
        excludedValSet.add("Doe");
        excludedValMap.put("$1337_1", excludedValSet);
        parser.setExcludedValues(excludedValMap);
        
        String expandedQuery = parser.parse("$9001_1='dudududuu'").getOriginalQuery();
        Assert.assertEquals("($9001_1='dudududuu') && ((filter:includeRegex($1337_1, 'John')) && (not(filter:includeRegex($1337_1, 'Doe'))))", expandedQuery);
    }
}
