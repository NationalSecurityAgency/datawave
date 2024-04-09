package datawave.microservice.map.visitor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Sets;

import datawave.core.geo.utils.GeoQueryConfig;
import datawave.core.query.jexl.JexlASTHelper;
import datawave.data.type.GeoType;
import datawave.data.type.GeometryType;
import datawave.data.type.PointType;
import datawave.data.type.Type;
import datawave.microservice.map.data.GeoQueryFeatures;

public class GeoFeatureVisitorTest {
    
    private static Map<String,Set<Type<?>>> typesByField;
    private static ObjectMapper objectMapper;
    
    @BeforeAll
    public static void beforeAll() {
        typesByField = new HashMap<>();
        typesByField.put("GEOWAVE_FIELD", Sets.newHashSet(new GeometryType()));
        typesByField.put("POINT_FIELD", Sets.newHashSet(new PointType()));
        typesByField.put("GEO_FIELD", Sets.newHashSet(new GeoType()));
        
        objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }
    
    @Test
    public void geowaveIntersectsGeometryFunctionTest() throws ParseException, IOException, URISyntaxException {
        String query = "geowave:intersects(GEOWAVE_FIELD, 'POLYGON((10 10, -10 10, -10 -10, 10 -10, 10 10))')";
        
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        GeoQueryFeatures geoQueryFeatures = GeoFeatureVisitor.getGeoFeatures(script, typesByField);
        
        ClassLoader classLoader = GeoFeatureVisitorTest.class.getClassLoader();
        String expected = Files.readString(Paths.get(classLoader.getResource("data/GEOWAVE_FIELD.json").toURI()));
        
        assertEquals(expected, objectMapper.writeValueAsString(geoQueryFeatures));
    }
    
    @Test
    public void geowaveIntersectsPointFunctionTest() throws ParseException, IOException, URISyntaxException {
        String query = "geowave:intersects(POINT_FIELD, 'POLYGON((10 10, -10 10, -10 -10, 10 -10, 10 10))')";
        
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        GeoQueryFeatures geoQueryFeatures = GeoFeatureVisitor.getGeoFeatures(script, typesByField);
        
        ClassLoader classLoader = GeoFeatureVisitorTest.class.getClassLoader();
        String expected = Files.readString(Paths.get(classLoader.getResource("data/POINT_FIELD.json").toURI()));
        
        assertEquals(expected, objectMapper.writeValueAsString(geoQueryFeatures));
    }
    
    @Test
    public void geoWithinBoundingBoxFunctionTest() throws ParseException, IOException, URISyntaxException {
        String query = "geo:within_bounding_box(GEO_FIELD, '-10_-10', '10_10')";
        
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        GeoQueryFeatures geoQueryFeatures = GeoFeatureVisitor.getGeoFeatures(script, typesByField);
        
        ClassLoader classLoader = GeoFeatureVisitorTest.class.getClassLoader();
        String expected = Files.readString(Paths.get(classLoader.getResource("data/GEO_FIELD.json").toURI()));
        
        assertEquals(expected, objectMapper.writeValueAsString(geoQueryFeatures));
    }
    
    @Test
    public void expandedGeowaveIntersectsGeometryFunctionTest() throws ParseException, IOException, URISyntaxException {
        String query = "geowave:intersects(GEOWAVE_FIELD, 'POLYGON((10 10, -10 10, -10 -10, 10 -10, 10 10))')";
        
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        GeoQueryFeatures geoQueryFeatures = GeoFeatureVisitor.getGeoFeatures(script, typesByField, GeoQueryConfig.builder().build());
        
        ClassLoader classLoader = GeoFeatureVisitorTest.class.getClassLoader();
        String expected = Files.readString(Paths.get(classLoader.getResource("data/EXPANDED_GEOWAVE_FIELD.json").toURI()));
        
        assertEquals(expected, objectMapper.writeValueAsString(geoQueryFeatures));
    }
    
    @Test
    public void expandedGeowaveIntersectsPointFunctionTest() throws ParseException, IOException, URISyntaxException {
        String query = "geowave:intersects(POINT_FIELD, 'POLYGON((10 10, -10 10, -10 -10, 10 -10, 10 10))')";
        
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        GeoQueryFeatures geoQueryFeatures = GeoFeatureVisitor.getGeoFeatures(script, typesByField, GeoQueryConfig.builder().build());
        
        ClassLoader classLoader = GeoFeatureVisitorTest.class.getClassLoader();
        String expected = Files.readString(Paths.get(classLoader.getResource("data/EXPANDED_POINT_FIELD.json").toURI()));
        
        assertEquals(expected, objectMapper.writeValueAsString(geoQueryFeatures));
    }
    
    @Test
    public void expandedGeoWithinBoundingBoxFunctionTest() throws ParseException, IOException, URISyntaxException {
        String query = "geo:within_bounding_box(GEO_FIELD, '-10_-10', '10_10')";
        
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        GeoQueryFeatures geoQueryFeatures = GeoFeatureVisitor.getGeoFeatures(script, typesByField, GeoQueryConfig.builder().build());
        
        ClassLoader classLoader = GeoFeatureVisitorTest.class.getClassLoader();
        String expected = Files.readString(Paths.get(classLoader.getResource("data/EXPANDED_GEO_FIELD.json").toURI()));
        
        assertEquals(expected, objectMapper.writeValueAsString(geoQueryFeatures));
    }
}
