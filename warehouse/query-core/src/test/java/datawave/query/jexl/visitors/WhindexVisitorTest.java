package datawave.query.jexl.visitors;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.whindex.WhindexVisitor;
import datawave.query.util.MockMetadataHelper;

public class WhindexVisitorTest {

    private static final Set<String> mappingFields = new HashSet<>();
    private static final Map<String,Map<String,String>> singleFieldMapping = new HashMap<>();
    private static final Map<String,Map<String,String>> multipleFieldMapping = new HashMap<>();
    private static final Map<String,Map<String,String>> manyToOneFieldMapping = new HashMap<>();

    private static final Map<String,Map<String,String>> allMappings = new HashMap<>();

    private static final Map<String,Date> creationDateMap = new HashMap<>();

    private static final MockMetadataHelper metadataHelper = new MockMetadataHelper();

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

        allMappings.putAll(multipleFieldMapping);
        allMappings.putAll(manyToOneFieldMapping);

        creationDateMap.put("HOT_FUDGE_SUNDAE", new Date(0));
        creationDateMap.put("BANANA_SPLIT", new Date(0));
        creationDateMap.put("NUT_SUNDAE", new Date(0));
    }

    @Test
    public void mappedFieldMissingIntersectsTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && TOPPINGS == 'HOT_FUDGE'";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(singleFieldMapping);

        Map<String,Date> creationDateMap = new HashMap<>();
        creationDateMap.put("HOT_FUDGE_SUNDAE", new Date(new Date(0).getTime() + TimeUnit.DAYS.toMillis(1)));
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals("geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void oneToOneWithinBoundingBoxTest() throws ParseException {
        String query = "geo:within_bounding_box(ICE_CREAM, '-10_-10', '10_10') && TOPPINGS == 'HOT_FUDGE'";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(singleFieldMapping);
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals("geo:within_bounding_box(HOT_FUDGE_SUNDAE, '-10_-10', '10_10')", JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void oneToOneWithinBoundingBoxParensTest() throws ParseException {
        String query = "geo:within_bounding_box(ICE_CREAM, '-10_-10', '10_10') && (TOPPINGS == 'HOT_FUDGE')";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(singleFieldMapping);
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals("geo:within_bounding_box(HOT_FUDGE_SUNDAE, '-10_-10', '10_10')", JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void oneToOneWithinCircleTest() throws ParseException {
        String query = "geo:within_circle(ICE_CREAM, '-10_-10', '10') && TOPPINGS == 'HOT_FUDGE'";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(singleFieldMapping);
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals("geo:within_circle(HOT_FUDGE_SUNDAE, '-10_-10', '10')", JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void oneToOneNestedIntersectsTest() throws ParseException {
        String query = "(geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || FIELD == 'blah') && TOPPINGS == 'HOT_FUDGE'";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(singleFieldMapping);
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals(
                        "(TOPPINGS == 'HOT_FUDGE' && geowave:intersects(SHERBERT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || (TOPPINGS == 'BANANA' && geowave:intersects(SHERBERT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void manyToManyWithinBoundingBoxTest() throws ParseException {
        String query = "geo:within_bounding_box((ICE_CREAM || SHERBERT), '-10_-10', '10_10') && (TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA')";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals(
                        "(TOPPINGS == 'HOT_FUDGE' && geo:within_bounding_box(SHERBERT, '-10_-10', '10_10')) || geo:within_bounding_box(HOT_FUDGE_SUNDAE, '-10_-10', '10_10') || (TOPPINGS == 'BANANA' && geo:within_bounding_box(SHERBERT, '-10_-10', '10_10')) || geo:within_bounding_box(BANANA_SPLIT, '-10_-10', '10_10')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void reverseManyToManyIntersectsTest() throws ParseException {
        String query = "(TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA') && geowave:intersects((ICE_CREAM || SHERBERT), 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals(
                        "(TOPPINGS == 'HOT_FUDGE' && geowave:intersects(SHERBERT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || (TOPPINGS == 'BANANA' && geowave:intersects(SHERBERT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void manyToManyUniqueIntersectsTest() throws ParseException {
        String query = "(geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || geowave:intersects(ICE_CREAM, 'POLYGON((-20 -20, 20 -20, 20 20, -20 20, -20 -20))')) && (TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA')";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals(
                        "TOPPINGS == 'PEANUT' && ((TOPPINGS == 'HOT_FUDGE' && geowave:intersects(SHERBERT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || (TOPPINGS == 'BANANA' && geowave:intersects(SHERBERT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))'))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void reverseManyToManyMultipleIntersectsTest() throws ParseException {
        String query = "(TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA') && TOPPINGS == 'PEANUT' && geowave:intersects((ICE_CREAM || SHERBERT), 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals(
                        "TOPPINGS == 'PEANUT' && ((TOPPINGS == 'HOT_FUDGE' && geowave:intersects(SHERBERT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || (TOPPINGS == 'BANANA' && geowave:intersects(SHERBERT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))'))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void oneToManyMultipleIntersectsTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && TOPPINGS == 'PEANUT' && (TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA')";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals(
                        "TOPPINGS == 'PEANUT' && (geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))'))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void reverseOneToManyMultipleIntersectsTest() throws ParseException {
        String query = "(TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA') && TOPPINGS == 'PEANUT' && geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals(
                        "TOPPINGS == 'PEANUT' && (geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || geowave:intersects(BANANA_SPLIT, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))'))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void incompleteMappingIntersectsTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (TOPPINGS == 'HOT_FUDGE' || TOPPINGS == 'BANANA' || TOPPINGS == 'CHERRY')";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(multipleFieldMapping);
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

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
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals(
                        "geowave:intersects(NUT_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (((_Eval_ = true) && (TOPPINGS == 'PEANUT')) || ((_Eval_ = true) && (TOPPINGS == 'PISTACHIO')) || ((_Eval_ = true) && (TOPPINGS == 'CASHEW')))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void manyToOnePartialIntersectsTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (TOPPINGS == 'PEANUT' || TOPPINGS == 'PISTACHIO')";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(manyToOneFieldMapping);
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals(
                        "geowave:intersects(NUT_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (((_Eval_ = true) && (TOPPINGS == 'PEANUT')) || ((_Eval_ = true) && (TOPPINGS == 'PISTACHIO')))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void manyToOneParensFieldValueIntersectsTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (TOPPINGS == 'PEANUT')";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(manyToOneFieldMapping);
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals(
                        "((_Eval_ = true) && (TOPPINGS == 'PEANUT')) && geowave:intersects(NUT_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void manyToOneParensGeoIntersectsTest() throws ParseException {
        String query = "(geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) && TOPPINGS == 'PEANUT'";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(manyToOneFieldMapping);
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals(
                        "((_Eval_ = true) && (TOPPINGS == 'PEANUT')) && (geowave:intersects(NUT_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))'))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void reverseManyToOneIntersectsTest() throws ParseException {
        String query = "(TOPPINGS == 'PEANUT' || TOPPINGS == 'PISTACHIO' || TOPPINGS == 'CASHEW') && geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(manyToOneFieldMapping);
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals(
                        "geowave:intersects(NUT_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && (((_Eval_ = true) && (TOPPINGS == 'PEANUT')) || ((_Eval_ = true) && (TOPPINGS == 'PISTACHIO')) || ((_Eval_ = true) && (TOPPINGS == 'CASHEW')))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void manyToOneEvalOnlyIntersectsTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && TOPPINGS == 'PEANUT' && FOO == 'BAR'";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(manyToOneFieldMapping);
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals(
                        "((_Eval_ = true) && (TOPPINGS == 'PEANUT')) && FOO == 'BAR' && geowave:intersects(NUT_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void oneToOneNestedFieldValueTest() throws ParseException {
        String query = "geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && ((TOPPINGS == 'HOT_FUDGE' && BAR == 'FOO') || FOO == 'BAR')";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(singleFieldMapping);
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals(
                        "((geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && FOO == 'BAR') || (BAR == 'FOO' && geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void oneToOneDistributedFieldValueTest() throws ParseException {
        String query = "TOPPINGS == 'HOT_FUDGE' && ((geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && BAR == 'FOO') || FOO == 'BAR')";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(singleFieldMapping);
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals(
                        "((TOPPINGS == 'HOT_FUDGE' && FOO == 'BAR') || (BAR == 'FOO' && geowave:intersects(HOT_FUDGE_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void oneToOneUnhandledCase() throws ParseException {
        String query = "(geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || geowave:intersects(ICE_CREAM, 'POLYGON((-20 -20, 20 -20, 20 20, -20 20, -20 -20))')) && ((TOPPINGS == 'HOT_FUDGE' && BAR == 'FOO') || FOO == 'BAR')";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(singleFieldMapping);
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        Assert.assertEquals(
                        "(geowave:intersects(ICE_CREAM, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') || geowave:intersects(ICE_CREAM, 'POLYGON((-20 -20, 20 -20, 20 20, -20 20, -20 -20))')) && ((TOPPINGS == 'HOT_FUDGE' && BAR == 'FOO') || FOO == 'BAR')",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }

    @Test
    public void geoWaveMultiFieldTest() throws ParseException {
        // Test with a combination of fields that can and can't be mapped to a whindex
        String query = "geowave:intersects(ICE_CREAM || NOTHING, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))') && TOPPINGS == 'PEANUT'";

        ShardQueryConfiguration config = new ShardQueryConfiguration();
        config.setWhindexMappingFields(mappingFields);
        config.setWhindexFieldMappings(allMappings);
        config.setWhindexCreationDates(creationDateMap);

        ASTJexlScript jexlScript = JexlASTHelper.parseJexlQuery(query);
        jexlScript = WhindexVisitor.apply(jexlScript, config, new Date(), metadataHelper);

        System.out.println(JexlStringBuildingVisitor.buildQuery(jexlScript));

        Assert.assertEquals(
                        "(TOPPINGS == 'PEANUT' && geowave:intersects(NOTHING, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))')) || (((_Eval_ = true) && (TOPPINGS == 'PEANUT')) && geowave:intersects(NUT_SUNDAE, 'POLYGON((-10 -10, 10 -10, 10 10, -10 10, -10 -10))'))",
                        JexlStringBuildingVisitor.buildQuery(jexlScript));
    }
}
