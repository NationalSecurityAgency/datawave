package datawave.query.language.parser.jexl;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import datawave.query.language.parser.ParseException;
import datawave.query.language.tree.QueryNode;
import datawave.query.language.tree.ServerHeadNode;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class TestLuceneToJexlControlledQueryParser {
    
    private LuceneToJexlControlledQueryParser parser;
    
    @Test(expected = ParseException.class)
    public void testDisallowedAnyfield() throws ParseException {
        parser = new LuceneToJexlControlledQueryParser();
        parser.setAllowAnyField(false);
        parseQuery("anyfield should not work");
    }
    
    /**
     * Noticed bug when no allowed fields were set, users could not perform an ANYFIELD query.
     * 
     * @throws ParseException
     */
    @Test
    public void testAllowedAnyfieldWithEmptyAllowedFields() throws ParseException {
        parser = new LuceneToJexlControlledQueryParser();
        parser.setAllowAnyField(true);
        parser.setIncludedValues(ImmutableMap.<String,Set<String>> of());
        parser.setAllowedFields(ImmutableSet.<String> of());
        parseQuery("unfieldedValue");
    }
    
    @Test(expected = ParseException.class)
    public void testDisallowedFieldsInAnd() throws ParseException {
        parser = new LuceneToJexlControlledQueryParser();
        Set<String> allowedFields = new HashSet<>();
        allowedFields.add("FIELD1");
        allowedFields.add("FIELD2");
        parser.setAllowedFields(allowedFields);
        parseQuery("FIELD1:value FIELD2:value FIELD3:value");
    }
    
    /**
     * MetadataQuery logic doesn't accept lowercase field names. It previously threw: datawave.query.language.parser.ParseException: Unallowed field(s)
     * '[field2]' for this type of query
     *
     * It should accept these fields when checking against the allowed list.
     */
    @Test
    public void testTransformsLowerCaseBeforeComparingAgainstAllowed() throws ParseException {
        parser = new LuceneToJexlControlledQueryParser();
        Set<String> allowedFields = new HashSet<>();
        allowedFields.add("FIELD1");
        allowedFields.add("FIELD2");
        allowedFields.add("FIELD3");
        parser.setAllowedFields(allowedFields);
        parser.parse("FIELD1:value field2:value FIELD3:value");
        parser.parse("field1:value FIELD2:value field3:value");
    }
    
    @Test(expected = ParseException.class)
    public void testDisallowedFieldsInFunction1() throws ParseException {
        parser = new LuceneToJexlControlledQueryParser();
        Set<String> allowedFields = new HashSet<>();
        allowedFields.add("FIELD1");
        parser.setAllowedFields(allowedFields);
        parseQuery("FIELD1:value #INCLUDE(FIELD3, regex3)");
    }
    
    @Test(expected = ParseException.class)
    public void testDisallowedFieldsInFunction2() throws ParseException {
        parser = new LuceneToJexlControlledQueryParser();
        Set<String> allowedFields = new HashSet<>();
        allowedFields.add("FIELD1");
        parser.setAllowedFields(allowedFields);
        parseQuery("FIELD1:value #ISNULL(FIELD3)");
    }
    
    @Test
    public void testAllowedFields() throws ParseException {
        parser = new LuceneToJexlControlledQueryParser();
        Set<String> allowedFields = new HashSet<>();
        allowedFields.add("FIELD1");
        allowedFields.add("FIELD2");
        parser.setAllowedFields(allowedFields);
        Assert.assertEquals("FIELD1 == 'value' && FIELD2 == 'value'", parseQuery("FIELD1:value FIELD2:value"));
    }
    
    @Test
    public void testIncludedFields() throws ParseException {
        parser = new LuceneToJexlControlledQueryParser();
        Set<String> allowedFields = new HashSet<>();
        allowedFields.add("FIELD1");
        allowedFields.add("FIELD2");
        allowedFields.add("FIELD3");
        parser.setAllowedFields(allowedFields);
        
        Map<String,Set<String>> includedValues = new LinkedHashMap<>();
        includedValues.put("FIELD2", Collections.singleton("specialvalue2"));
        includedValues.put("FIELD3", Collections.singleton("specialvalue3"));
        parser.setIncludedValues(includedValues);
        
        Assert.assertEquals("(FIELD1 == 'value') && (filter:includeRegex(FIELD2, 'specialvalue2') || filter:includeRegex(FIELD3, 'specialvalue3'))",
                        parseQuery("FIELD1:value"));
    }
    
    @Test
    public void testExcludedValues() throws ParseException {
        parser = new LuceneToJexlControlledQueryParser();
        Set<String> allowedFields = new HashSet<>();
        allowedFields.add("FIELD1");
        allowedFields.add("FIELD2");
        allowedFields.add("FIELD3");
        parser.setAllowedFields(allowedFields);
        
        Map<String,Set<String>> excludedValues = new LinkedHashMap<>();
        excludedValues.put("FIELD2", Collections.singleton("specialvalue2"));
        excludedValues.put("FIELD3", Collections.singleton("specialvalue3"));
        parser.setExcludedValues(excludedValues);
        
        Assert.assertEquals("(FIELD1 == 'value') && (not(filter:includeRegex(FIELD2, 'specialvalue2')) && not(filter:includeRegex(FIELD3, 'specialvalue3')))",
                        parseQuery("FIELD1:value"));
    }
    
    @Test
    public void testBothIncludeAndExcludeValues() throws ParseException {
        parser = new LuceneToJexlControlledQueryParser();
        Set<String> allowedFields = new HashSet<>();
        allowedFields.add("$9001_1");
        allowedFields.add("$1337_1");
        parser.setAllowedFields(allowedFields);
        
        Map<String,Set<String>> includedValMap = new HashMap<>();
        Set<String> includedValSet = new HashSet<>();
        includedValSet.add("John");
        includedValMap.put("$1337_1", includedValSet);
        parser.setIncludedValues(includedValMap);
        
        Map<String,Set<String>> excludedValMap = new HashMap<>();
        Set<String> excludedValSet = new HashSet<>();
        excludedValSet.add("Cena");
        excludedValMap.put("$1337_1", excludedValSet);
        parser.setExcludedValues(excludedValMap);
        
        String expandedQuery = parseQuery("$9001_1:dudududuu");
        
        Assert.assertEquals("($9001_1 == 'dudududuu') && (filter:includeRegex($1337_1, 'John') && not(filter:includeRegex($1337_1, 'Cena')))", expandedQuery);
    }
    
    private String parseQuery(String query) throws ParseException {
        String parsedQuery = null;
        
        try {
            QueryNode node = parser.parse(query);
            if (node instanceof ServerHeadNode) {
                parsedQuery = node.getOriginalQuery();
            }
        } catch (RuntimeException e) {
            throw new ParseException(e);
        }
        return parsedQuery;
    }
}
