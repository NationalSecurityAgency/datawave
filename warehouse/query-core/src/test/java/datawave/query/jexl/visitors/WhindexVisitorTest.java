package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MockMetadataHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WhindexVisitorTest {
    
    private static final Set<String> mappingFields = new HashSet<>();
    private static final Map<String,Map<String,List<String>>> simpleFieldMappings = new HashMap<>();
    private static final Map<String,Map<String,List<String>>> complexFieldMappings = new HashMap<>();
    
    @BeforeClass
    public static void beforeClass() {
        mappingFields.add("TOPPINGS");
        mappingFields.add("FIXINGS");
        
        List<String> remappedFields = Collections.singletonList("HOT_FUDGE_SUNDAE");
        
        Map<String,List<String>> fieldMappings = new HashMap<>();
        fieldMappings.put("ICE_CREAM", remappedFields);
        
        simpleFieldMappings.put("HOT_FUDGE", fieldMappings);
        complexFieldMappings.put("HOT_FUDGE", fieldMappings);
        
        remappedFields = Collections.singletonList("BANANA_SPLIT");
        fieldMappings = new HashMap<>();
        fieldMappings.put("ICE_CREAM", remappedFields);
        
        complexFieldMappings.put("BANANA", fieldMappings);
    }
    
    @Test
    public void simpleIntersectsTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && TOPPINGS == 'HOT_FUDGE'";
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, mappingFields, simpleFieldMappings, new MockMetadataHelper());
        
        Assert.assertEquals("geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void alternateIntersectsTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && FIXINGS == 'HOT_FUDGE'";
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, mappingFields, simpleFieldMappings, new MockMetadataHelper());
        
        Assert.assertEquals("geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void oneToManyIntersectsTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && TOPPINGS == 'HOT_FUDGE' && TOPPINGS == 'BANANA'";
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, mappingFields, complexFieldMappings, new MockMetadataHelper());
        
        Assert.assertEquals("geowave:intersects((HOT_FUDGE_SUNDAE || BANANA_SPLIT), 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void oneToManyExtraIntersectsTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && TOPPINGS == 'HOT_FUDGE' && TOPPINGS == 'BANANA' && TOPPINGS == 'CHERRY'";
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, mappingFields, complexFieldMappings, new MockMetadataHelper());
        
        Assert.assertEquals(
                        "geowave:intersects((HOT_FUDGE_SUNDAE || BANANA_SPLIT), 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && TOPPINGS == 'CHERRY'",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void simpleIntersectsIncludeRegexTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && filter:includeRegex(TOPPINGS, 'HOT_FUDGE')";
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, mappingFields, simpleFieldMappings, new MockMetadataHelper());
        
        Assert.assertEquals(
                        "geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && filter:includeRegex(TOPPINGS, 'HOT_FUDGE')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void alternateIntersectsIncludeRegexTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && filter:includeRegex(FIXINGS, 'HOT_FUDGE')";
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, mappingFields, simpleFieldMappings, new MockMetadataHelper());
        
        Assert.assertEquals(
                        "geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && filter:includeRegex(FIXINGS, 'HOT_FUDGE')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void oneToManyIntersectsIncludeRegexTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && filter:includeRegex(TOPPINGS, 'HOT_FUDGE|BANANA')";
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, mappingFields, complexFieldMappings, new MockMetadataHelper());
        
        Assert.assertEquals(
                        "geowave:intersects((HOT_FUDGE_SUNDAE || BANANA_SPLIT), 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && filter:includeRegex(TOPPINGS, 'HOT_FUDGE|BANANA')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void oneToManyAlternateIntersectsIncludeRegexTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && filter:includeRegex(TOPPINGS, 'HOT_FUDGE') && filter:includeRegex(TOPPINGS, 'BANANA')";
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, mappingFields, complexFieldMappings, new MockMetadataHelper());
        
        Assert.assertEquals(
                        "geowave:intersects((HOT_FUDGE_SUNDAE || BANANA_SPLIT), 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && filter:includeRegex(TOPPINGS, 'HOT_FUDGE') && filter:includeRegex(TOPPINGS, 'BANANA')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void oneToManyExtraIntersectsIncludeRegexTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && filter:includeRegex(TOPPINGS, 'HOT_FUDGE|BANANA|CHERRY')";
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, mappingFields, complexFieldMappings, new MockMetadataHelper());
        
        Assert.assertEquals(
                        "geowave:intersects((HOT_FUDGE_SUNDAE || BANANA_SPLIT), 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && filter:includeRegex(TOPPINGS, 'HOT_FUDGE|BANANA|CHERRY')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void oneToManyAlternateExtraIntersectsIncludeRegexTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && filter:includeRegex(TOPPINGS, 'HOT_FUDGE') && filter:includeRegex(TOPPINGS, 'BANANA') && filter:includeRegex(TOPPINGS, 'CHERRY')";
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, mappingFields, complexFieldMappings, new MockMetadataHelper());
        
        Assert.assertEquals(
                        "geowave:intersects((HOT_FUDGE_SUNDAE || BANANA_SPLIT), 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && filter:includeRegex(TOPPINGS, 'HOT_FUDGE') && filter:includeRegex(TOPPINGS, 'BANANA') && filter:includeRegex(TOPPINGS, 'CHERRY')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
}
