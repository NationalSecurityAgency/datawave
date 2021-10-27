package datawave.query.jexl.visitors;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.whindex.WhindexVisitor;
import datawave.query.util.MockMetadataHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class WhindexVisitorTest {
    
    private static final Set<String> mappingFields = new HashSet<>();
    private static final Map<String,Map<String,String>> singleFieldMapping = new HashMap<>();
    private static final Map<String,Map<String,String>> multipleFieldMapping = new HashMap<>();
    private static final Map<String,Map<String,String>> manyToOneFieldMapping = new HashMap<>();
    
    private static final MockMetadataHelper metadataHelper = new MockMetadataHelper() {
        @Override
        public Date getEarliestOccurrenceOfField(String fieldName) {
            return new Date(new Date(0).getTime() + TimeUnit.DAYS.toMillis(1));
        }
    };
    
    @BeforeClass
    public static void beforeClass() {
        
        mappingFields.add("TOPPINGS");
        mappingFields.add("FIXINGS");
        
        singleFieldMapping.computeIfAbsent("HOT_FUDGE", k1 -> {
            Map<String,String> map = new HashMap<>();
            map.put("ICE_CREAM", "HOT_FUDGE_SUNDAE");
            return map;
        });
        
        multipleFieldMapping.computeIfAbsent("HOT_FUDGE", k1 -> {
            Map<String,String> map = new HashMap<>();
            map.put("ICE_CREAM", "HOT_FUDGE_SUNDAE");
            return map;
        });
        multipleFieldMapping.computeIfAbsent("BANANA", k1 -> {
            Map<String,String> map = new HashMap<>();
            map.put("ICE_CREAM", "BANANA_SPLIT");
            return map;
        });
        
        manyToOneFieldMapping.computeIfAbsent("PEANUT", k1 -> {
            Map<String,String> map = new HashMap<>();
            map.put("ICE_CREAM", "NUT_SUNDAE");
            return map;
        });
        manyToOneFieldMapping.computeIfAbsent("PISTACHIO", k1 -> {
            Map<String,String> map = new HashMap<>();
            map.put("ICE_CREAM", "NUT_SUNDAE");
            return map;
        });
        manyToOneFieldMapping.computeIfAbsent("CASHEW", k1 -> {
            Map<String,String> map = new HashMap<>();
            map.put("ICE_CREAM", "NUT_SUNDAE");
            return map;
        });
    }
    
    @Test
    public void mappedFieldMissingIntersectsTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && TOPPINGS == 'HOT_FUDGE'";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(singleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(0), metadataHelper);
        
        Assert.assertEquals("geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && TOPPINGS == 'HOT_FUDGE'",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void oneToOneIntersectsTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && TOPPINGS == 'HOT_FUDGE'";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(singleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals("geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void oneToOneNestedIntersectsTest() throws ParseException {
        String query = "(geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || FIELD == 'blah') && TOPPINGS == 'HOT_FUDGE'";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(singleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "((TOPPINGS == 'HOT_FUDGE' && FIELD == 'blah') || geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))'))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void oneToOneNestedReverseIntersectsTest() throws ParseException {
        String query = "(TOPPINGS == 'HOT_FUDGE' || FIELD == 'blah') && geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(singleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "(geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && FIELD == 'blah') || geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void reverseOneToOneIntersectsTest() throws ParseException {
        String query = "TOPPINGS == 'HOT_FUDGE' && geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(singleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals("geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void oneToOneIntersectsWithTermsAndRangesTest() throws ParseException {
        String query = "(geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (ICE_CREAM == '0100' || ((BoundedRange = true) && (ICE_CREAM >= '0102' && ICE_CREAM <= '0103')))) && TOPPINGS == 'HOT_FUDGE'";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(singleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (HOT_FUDGE_SUNDAE == '0100' || ((BoundedRange = true) && (HOT_FUDGE_SUNDAE >= '0102' && HOT_FUDGE_SUNDAE <= '0103')))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void reverseOneToOneIntersectsWithTermsAndRangesTest() throws ParseException {
        String query = "TOPPINGS == 'HOT_FUDGE' && ((ICE_CREAM == '0100' || ((BoundedRange = true) && (ICE_CREAM >= '0102' && ICE_CREAM <= '0103'))) && geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))'))";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(singleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (HOT_FUDGE_SUNDAE == '0100' || ((BoundedRange = true) && (HOT_FUDGE_SUNDAE >= '0102' && HOT_FUDGE_SUNDAE <= '0103')))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void oneToOneAltFieldIntersectsTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && FIXINGS == 'HOT_FUDGE'";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(singleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals("geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void reverseOneToOneAltFieldIntersectsTest() throws ParseException {
        String query = "FIXINGS == 'HOT_FUDGE' && geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(singleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals("geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void oneToManyIntersectsTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void reverseOneToManyIntersectsTest() throws ParseException {
        String query = "(TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA') && geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void oneToManyIntersectsDistributedUnchangedTest() throws ParseException {
        String query = "FIELD == 'VALUE' && (TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals("FIELD == 'VALUE' && (TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA')", JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void reverseOneToManyIntersectsDistributedUnchangedTest() throws ParseException {
        String query = "(TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA') && FIELD == 'VALUE'";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals("(TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA') && FIELD == 'VALUE'", JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void oneToManyIntersectsDistributedGroupedTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA' || CONTAINER == 'CONE' || CONTAINER == 'BOWL')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "(geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (CONTAINER == 'CONE' || CONTAINER == 'BOWL')) || geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void reverseOneToManyIntersectsDistributedGroupedTest() throws ParseException {
        String query = "(TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA' || CONTAINER == 'CONE' || CONTAINER == 'BOWL') && geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "(geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (CONTAINER == 'CONE' || CONTAINER == 'BOWL')) || geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void manyToManyIntersectsTest() throws ParseException {
        String query = "geowave:intersects((ICE_CREAM || SHERBERT), 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "((TOPPINGS == 'HOT_FUDGE' && geowave:intersects(SHERBERT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || ((TOPPINGS == 'BANANA' && geowave:intersects(SHERBERT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))'))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void reverseManyToManyIntersectsTest() throws ParseException {
        String query = "(TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA') && geowave:intersects((ICE_CREAM || SHERBERT), 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "((TOPPINGS == 'HOT_FUDGE' && geowave:intersects(SHERBERT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || ((TOPPINGS == 'BANANA' && geowave:intersects(SHERBERT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))'))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void manyToManyUniqueIntersectsTest() throws ParseException {
        String query = "(geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || geowave:intersects(ICE_CREAM, 'POLYGON((-20 -20, 20 -20, 20 20, -20 20, -20 -20))')) && (TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "(geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-20 -20, 20 -20, 20 20, -20 20, -20 -20))')) || (geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || geowave:intersects(BANANA_SPLIT, 'POLYGON((-20 -20, 20 -20, 20 20, -20 20, -20 -20))'))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void reverseManyToManyUniqueIntersectsTest() throws ParseException {
        String query = "(TOPPINGS == 'BANANA' || TOPPINGS == 'HOT_FUDGE') && (geowave:intersects(ICE_CREAM, 'POLYGON((-20 -20, 20 -20, 20 20, -20 20, -20 -20))') || geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))'))";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "(geowave:intersects(BANANA_SPLIT, 'POLYGON((-20 -20, 20 -20, 20 20, -20 20, -20 -20))') || geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || (geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-20 -20, 20 -20, 20 20, -20 20, -20 -20))') || geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))'))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void manyToManyMultipleIntersectsTest() throws ParseException {
        String query = "geowave:intersects((ICE_CREAM || SHERBERT), 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && TOPPINGS == 'PEANUT' && (TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "(TOPPINGS == 'PEANUT' && ((TOPPINGS == 'HOT_FUDGE' && geowave:intersects(SHERBERT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))'))) || (TOPPINGS == 'PEANUT' && ((TOPPINGS == 'BANANA' && geowave:intersects(SHERBERT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void reverseManyToManyMultipleIntersectsTest() throws ParseException {
        String query = "(TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA') && TOPPINGS == 'PEANUT' && geowave:intersects((ICE_CREAM || SHERBERT), 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "(TOPPINGS == 'PEANUT' && ((TOPPINGS == 'HOT_FUDGE' && geowave:intersects(SHERBERT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))'))) || (TOPPINGS == 'PEANUT' && ((TOPPINGS == 'BANANA' && geowave:intersects(SHERBERT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void oneToManyMultipleIntersectsTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && TOPPINGS == 'PEANUT' && (TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "(TOPPINGS == 'PEANUT' && geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || (TOPPINGS == 'PEANUT' && geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))'))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void reverseOneToManyMultipleIntersectsTest() throws ParseException {
        String query = "(TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA') && TOPPINGS == 'PEANUT' && geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "(TOPPINGS == 'PEANUT' && geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || (TOPPINGS == 'PEANUT' && geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))'))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void incompleteMappingIntersectsTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA' || TOPPINGS == 'CHERRY')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "(geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && TOPPINGS == 'CHERRY') || geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void reverseIncompleteMappingIntersectsTest() throws ParseException {
        String query = "(TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA' || TOPPINGS == 'CHERRY') && geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "(geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && TOPPINGS == 'CHERRY') || geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void incompleteMappingIntersectsWithTermsAndRangesTest() throws ParseException {
        String query = "(geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (ICE_CREAM == '0100' || ((BoundedRange = true) && (ICE_CREAM >= '0102' && ICE_CREAM <= '0103')))) && (TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA' || TOPPINGS == 'CHERRY')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "(geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (ICE_CREAM == '0100' || ((BoundedRange = true) && (ICE_CREAM >= '0102' && ICE_CREAM <= '0103'))) && TOPPINGS == 'CHERRY') || (geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (HOT_FUDGE_SUNDAE == '0100' || ((BoundedRange = true) && (HOT_FUDGE_SUNDAE >= '0102' && HOT_FUDGE_SUNDAE <= '0103')))) || (geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (BANANA_SPLIT == '0100' || ((BoundedRange = true) && (BANANA_SPLIT >= '0102' && BANANA_SPLIT <= '0103'))))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void reverseIncompleteMappingIntersectsWithTermsAndRangesTest() throws ParseException {
        String query = "(TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA' || TOPPINGS == 'CHERRY') && ((ICE_CREAM == '0100' || ((BoundedRange = true) && (ICE_CREAM >= '0102' && ICE_CREAM <= '0103'))) && geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))'))";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals(
                        "((ICE_CREAM == '0100' || ((BoundedRange = true) && (ICE_CREAM >= '0102' && ICE_CREAM <= '0103'))) && geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && TOPPINGS == 'CHERRY') || (geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (HOT_FUDGE_SUNDAE == '0100' || ((BoundedRange = true) && (HOT_FUDGE_SUNDAE >= '0102' && HOT_FUDGE_SUNDAE <= '0103')))) || (geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (BANANA_SPLIT == '0100' || ((BoundedRange = true) && (BANANA_SPLIT >= '0102' && BANANA_SPLIT <= '0103'))))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void manyToOneIntersectsTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (TOPPINGS == 'PEANUT' || TOPPINGS == 'PISTACHIO' || TOPPINGS == 'CASHEW')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(manyToOneFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals("geowave:intersects(NUT_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
    
    @Test
    public void reverseManyToOneIntersectsTest() throws ParseException {
        String query = "(TOPPINGS == 'PEANUT' || TOPPINGS == 'PISTACHIO' || TOPPINGS == 'CASHEW') && geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')";
        
        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(manyToOneFieldMapping);
        
        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);
        
        Assert.assertEquals("geowave:intersects(NUT_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
}
