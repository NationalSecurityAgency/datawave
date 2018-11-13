package datawave.query.tables.edge;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import datawave.query.QueryParameters;
import datawave.query.language.parser.QueryParser;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.query.model.edge.EdgeQueryModel;

import org.junit.Before;
import org.junit.Test;

import datawave.webservice.query.Query;
import datawave.webservice.query.QueryImpl;
import datawave.webservice.results.edgedictionary.DefaultEdgeDictionary;
import datawave.webservice.results.edgedictionary.EventField;
import datawave.webservice.results.edgedictionary.DefaultMetadata;

public class DefaultEdgeEventQueryLogicTest {
    
    DefaultEdgeDictionary dict = null;
    DefaultEdgeEventQueryLogic logic = new DefaultEdgeEventQueryLogic();
    Collection<DefaultMetadata> metadata;
    
    @Before
    public void setUp() throws Exception {
        logic.setEdgeQueryModel(EdgeQueryModel.loadModel("/DATAWAVE_EDGE.xml"));
        
        // Create the results of the DatawaveMetadata table scan for edge data
        metadata = new LinkedList<>();
        
        // Each test can have its own entry in the metadata table, just make sure type is unique
        // Basic Test
        DefaultMetadata meta = new DefaultMetadata();
        // Edge Metadata Entry
        meta.setEdgeType("TEST1");
        meta.setEdgeRelationship("REL1-REL2");
        meta.setEdgeAttribute1Source("SOURCE1-SOURCE2");
        // Event field names stored in metadata value
        EventField field = new EventField();
        field.setSourceField("SOURCEFIELD");
        field.setSinkField("TARGETFIELD");
        field.setEnrichmentField("ENRICHFIELD");
        field.setEnrichmentIndex("enrichValue");
        List<EventField> eventFields = new LinkedList<>();
        eventFields.add(field);
        meta.setEventFields(eventFields);
        metadata.add(meta);
        
        // Multiple Field Source Test
        meta = new DefaultMetadata();
        meta.setEdgeType("TEST2");
        meta.setEdgeRelationship("REL1-REL2");
        meta.setEdgeAttribute1Source("SOURCE1-SOURCE2");
        // Event field names stored in metadata value
        EventField field1 = new EventField();
        field1.setSourceField("SOURCEFIELD1");
        field1.setSinkField("TARGETFIELD1");
        field1.setEnrichmentField("ENRICHFIELD1");
        field1.setEnrichmentIndex("enrichValue1");
        EventField field2 = new EventField();
        field2.setSourceField("SOURCEFIELD2");
        field2.setSinkField("TARGETFIELD2");
        field2.setEnrichmentField("ENRICHFIELD2");
        field2.setEnrichmentIndex("enrichValue2");
        eventFields = new LinkedList<>();
        eventFields.add(field1);
        eventFields.add(field2);
        meta.setEventFields(eventFields);
        metadata.add(meta);
        
        // No enrichment
        meta = new DefaultMetadata();
        meta.setEdgeType("TEST3");
        meta.setEdgeRelationship("REL1-REL2");
        meta.setEdgeAttribute1Source("SOURCE1-SOURCE2");
        // Event field names stored in metadata value
        EventField fieldNoEnrich = new EventField();
        fieldNoEnrich.setSourceField("SOURCEFIELD1");
        fieldNoEnrich.setSinkField("TARGETFIELD1");
        eventFields = new LinkedList<>();
        eventFields.add(fieldNoEnrich);
        meta.setEventFields(eventFields);
        metadata.add(meta);
        
        // 1 enrichment, 1 no enrichment
        
        meta = new DefaultMetadata();
        meta.setEdgeType("TEST4");
        meta.setEdgeRelationship("REL1-REL2");
        meta.setEdgeAttribute1Source("SOURCE1-SOURCE2");
        // Event field names stored in metadata value
        field1 = new EventField();
        field1.setSourceField("SOURCEFIELD1");
        field1.setSinkField("TARGETFIELD1");
        field1.setEnrichmentField("ENRICHFIELD1");
        field1.setEnrichmentIndex("enrichValue1");
        field2 = new EventField();
        field2.setSourceField("SOURCEFIELD2");
        field2.setSinkField("TARGETFIELD2");
        eventFields = new LinkedList<>();
        eventFields.add(field1);
        eventFields.add(field2);
        meta.setEventFields(eventFields);
        metadata.add(meta);
        
        // multiple no enrichment
        meta = new DefaultMetadata();
        meta.setEdgeType("TEST5");
        meta.setEdgeRelationship("REL1-REL2");
        meta.setEdgeAttribute1Source("SOURCE1-SOURCE2");
        // Event field names stored in metadata value
        field1 = new EventField();
        field1.setSourceField("SOURCEFIELD1");
        field1.setSinkField("TARGETFIELD1");
        field2 = new EventField();
        field2.setSourceField("SOURCEFIELD2");
        field2.setSinkField("TARGETFIELD2");
        eventFields = new LinkedList<>();
        eventFields.add(field1);
        eventFields.add(field2);
        meta.setEventFields(eventFields);
        metadata.add(meta);
        
        // No Collection Source
        meta = new DefaultMetadata();
        meta.setEdgeType("TEST6");
        meta.setEdgeRelationship("REL1-REL2");
        // Event field names stored in metadata value
        field1 = new EventField();
        field1.setSourceField("SOURCEFIELD1");
        field1.setSinkField("TARGETFIELD1");
        eventFields = new LinkedList<>();
        eventFields.add(field1);
        meta.setEventFields(eventFields);
        metadata.add(meta);
        
        // Mis-matched Collection source
        meta = new DefaultMetadata();
        meta.setEdgeType("TEST7");
        meta.setEdgeRelationship("REL1-REL2");
        meta.setEdgeAttribute1Source("SOURCE1-SOURCE2");
        // Event field names stored in metadata value
        field1 = new EventField();
        field1.setSourceField("SOURCEFIELD1");
        field1.setSinkField("TARGETFIELD1");
        eventFields = new LinkedList<>();
        eventFields.add(field1);
        meta.setEventFields(eventFields);
        metadata.add(meta);
        
        // Grouped enrichment field
        meta = new DefaultMetadata();
        meta.setEdgeType("TEST8");
        meta.setEdgeRelationship("REL1-REL2");
        meta.setEdgeAttribute1Source("SOURCE1-SOURCE2");
        // Event field names stored in metadata value
        field = new EventField();
        field.setSourceField("SOURCEFIELD");
        field.setSinkField("TARGETFIELD");
        field.setEnrichmentField("1_5.100");
        field.setEnrichmentIndex("enrichValue");
        eventFields = new LinkedList<>();
        eventFields.add(field);
        meta.setEventFields(eventFields);
        metadata.add(meta);
        
        // Numbered and grouped source and target fields
        meta = new DefaultMetadata();
        meta.setEdgeType("TEST9");
        meta.setEdgeRelationship("REL1-REL2");
        meta.setEdgeAttribute1Source("SOURCE1-SOURCE2");
        // Event field names stored in metadata value
        field = new EventField();
        field.setSourceField("1_5_R");
        field.setSinkField("5_6.1");
        field.setEnrichmentField("1_5.100");
        field.setEnrichmentIndex("enrichValue");
        eventFields = new LinkedList<>();
        eventFields.add(field);
        meta.setEventFields(eventFields);
        metadata.add(meta);
        
        // Edges with jexl preconditions
        meta = new DefaultMetadata();
        meta.setEdgeType("TEST10");
        meta.setEdgeRelationship("REL1-REL2");
        meta.setEdgeAttribute1Source("SOURCE1-SOURCE2");
        // Event field names stored in metadata value
        field = new EventField();
        field.setSourceField("200_1_R");
        field.setSinkField("200_3_R");
        field.setEnrichmentField("F05_3_R");
        field.setEnrichmentIndex("enrichValue");
        field.setJexlPrecondition("$200_1_R == $123");
        eventFields = new LinkedList<>();
        eventFields.add(field);
        meta.setEventFields(eventFields);
        metadata.add(meta);
        
        // Edges with complex jexl preconditions
        meta = new DefaultMetadata();
        meta.setEdgeType("TEST11");
        meta.setEdgeRelationship("REL42-REL42");
        meta.setEdgeAttribute1Source("SOURCE16-SOURCE16");
        // Event field names stored in metadata value
        field = new EventField();
        field.setSourceField("609_1_R");
        field.setSinkField("604_1_R");
        field.setEnrichmentField("F05_3_R");
        field.setEnrichmentIndex("enrichValue");
        field.setJexlPrecondition("$609_3 == '2384' && $600_7 == '2'");
        eventFields = new LinkedList<>();
        eventFields.add(field);
        meta.setEventFields(eventFields);
        metadata.add(meta);
        
        // Setup Edge Dictionary, initialize logic
        dict = new DefaultEdgeDictionary(metadata);
        logic.setEdgeDictionary(dict);
    }
    
    @Test
    public void testBasicQueryParse() throws Exception {
        Query query = new QueryImpl();
        query.setQuery("SOURCE == 'sourceValue' AND SINK == 'targetValue' AND TYPE == 'TEST1' AND RELATION == 'REL1-REL2' AND ATTRIBUTE1 == 'SOURCE1-SOURCE2'");
        
        String transformed = logic.getEventQuery(query);
        
        assertEquals("(SOURCEFIELD == 'sourceValue' AND TARGETFIELD == 'targetValue' AND ENRICHFIELD == 'enrichValue')", transformed);
    }
    
    @Test
    public void testMultipleSource() throws Exception {
        Query query = new QueryImpl();
        query.setQuery("SOURCE == 'sourceValue' AND SINK == 'targetValue' AND TYPE == 'TEST2' AND RELATION == 'REL1-REL2' AND ATTRIBUTE1 == 'SOURCE1-SOURCE2'");
        
        String transformed = logic.getEventQuery(query);
        
        assertEquals("(SOURCEFIELD1 == 'sourceValue' AND TARGETFIELD1 == 'targetValue' AND ENRICHFIELD1 == 'enrichValue1') OR (SOURCEFIELD2 == 'sourceValue' AND TARGETFIELD2 == 'targetValue' AND ENRICHFIELD2 == 'enrichValue2')",
                        transformed);
    }
    
    @Test
    public void testNoEnrichment() throws Exception {
        Query query = new QueryImpl();
        query.setQuery("SOURCE == 'sourceValue' AND SINK == 'targetValue' AND TYPE == 'TEST3' AND RELATION == 'REL1-REL2' AND ATTRIBUTE1 == 'SOURCE1-SOURCE2'");
        
        String transformed = logic.getEventQuery(query);
        
        assertEquals("(SOURCEFIELD1 == 'sourceValue' AND TARGETFIELD1 == 'targetValue')", transformed);
    }
    
    @Test
    public void testEnrichNoEnrich() throws Exception {
        Query query = new QueryImpl();
        query.setQuery("SOURCE == 'sourceValue' AND SINK == 'targetValue' AND TYPE == 'TEST4' AND RELATION == 'REL1-REL2' AND ATTRIBUTE1 == 'SOURCE1-SOURCE2'");
        
        String transformed = logic.getEventQuery(query);
        
        // System.out.println("transformed = " + transformed);
        
        assertEquals("(SOURCEFIELD1 == 'sourceValue' AND TARGETFIELD1 == 'targetValue' AND ENRICHFIELD1 == 'enrichValue1') OR (SOURCEFIELD2 == 'sourceValue' AND TARGETFIELD2 == 'targetValue')",
                        transformed);
    }
    
    @Test
    public void testMultiNoEnrich() throws Exception {
        Query query = new QueryImpl();
        query.setQuery("SOURCE == 'sourceValue' AND SINK == 'targetValue' AND TYPE == 'TEST5' AND RELATION == 'REL1-REL2' AND ATTRIBUTE1 == 'SOURCE1-SOURCE2'");
        
        String transformed = logic.getEventQuery(query);
        
        assertEquals("(SOURCEFIELD1 == 'sourceValue' AND TARGETFIELD1 == 'targetValue') OR (SOURCEFIELD2 == 'sourceValue' AND TARGETFIELD2 == 'targetValue')",
                        transformed);
    }
    
    @Test
    public void testNoAttribute1Source() throws Exception {
        Query query = new QueryImpl();
        query.setQuery("SOURCE == 'sourceValue' AND SINK == 'targetValue' AND TYPE == 'TEST6' AND RELATION == 'REL1-REL2'");
        String transformed = logic.getEventQuery(query);
        
        assertEquals("(SOURCEFIELD1 == 'sourceValue' AND TARGETFIELD1 == 'targetValue')", transformed);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testMismatchAttribute1Source() throws Exception {
        Query query = new QueryImpl();
        query.setQuery("SOURCE == 'sourceValue' AND SINK == 'targetValue' AND TYPE == 'TEST7' AND RELATION == 'REL1-REL2' AND ATTRIBUTE1 == 'BLAH1-BLAH2'");
        
        logic.getEventQuery(query); // throws because BLAH1 and BLAH2 aren't in the edge dictionary
        
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testMismatchRelationshipSource() throws Exception {
        Query query = new QueryImpl();
        query.setQuery("SOURCE == 'sourceValue' AND SINK == 'targetValue' AND TYPE == 'TEST7' AND RELATION == 'NOREL1-NOREL2' AND ATTRIBUTE1 == 'SOURCE1-SOURCE2'");
        
        logic.getEventQuery(query); // throws because BLAH1 and BLAH2 aren't in the edge dictionary
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testBogusType() throws Exception {
        Query query = new QueryImpl();
        query.setQuery("SOURCE == 'sourceValue' AND SINK == 'targetValue' AND TYPE == 'DOESNTEXIST' AND RELATION == 'REL1-REL2' AND ATTRIBUTE1 == 'SOURCE1-SOURCE2'");
        
        logic.getEventQuery(query); // throws because BLAH1 and BLAH2 aren't in the edge dictionary
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidEdgeEventQuery() throws Exception {
        Query query = new QueryImpl();
        query.setQuery("SOURCE == 'sourceValue' AND TYPE == 'TEST1' AND RELATION == 'REL1-REL2' AND ATTRIBUTE1 == 'SOURCE1-SOURCE2'");
        
        logic.getEventQuery(query); // throws
    }
    
    @Test
    public void testGroupedEnrichmentField() throws Exception {
        Query query = new QueryImpl();
        query.setQuery("SOURCE == 'sourceValue' AND SINK == 'targetValue' AND TYPE == 'TEST8' AND RELATION == 'REL1-REL2' AND ATTRIBUTE1 == 'SOURCE1-SOURCE2'");
        
        String transformed = logic.getEventQuery(query);
        
        assertEquals("(SOURCEFIELD == 'sourceValue' AND TARGETFIELD == 'targetValue' AND $1_5 == 'enrichValue')", transformed);
    }
    
    @Test
    public void testGroupedNumberedField() throws Exception {
        Query query = new QueryImpl();
        query.setQuery("SOURCE == 'sourceValue' AND SINK == 'targetValue' AND TYPE == 'TEST9' AND RELATION == 'REL1-REL2' AND ATTRIBUTE1 == 'SOURCE1-SOURCE2'");
        
        String transformed = logic.getEventQuery(query);
        
        assertEquals("($1_5_R == 'sourceValue' AND $5_6 == 'targetValue' AND $1_5 == 'enrichValue')", transformed);
    }
    
    @Test
    public void testEdgesWithPreconditions() throws Exception {
        Query query = new QueryImpl();
        query.setQuery("SOURCE == '$200_1_R' AND SINK == '$200_3_R' AND TYPE == 'TEST10' AND RELATION == 'REL1-REL2'");
        
        String transformed = logic.getEventQuery(query);
        
        assertEquals("($200_1_R == '$200_1_R' AND $200_3_R == '$200_3_R' AND F05_3_R == 'enrichValue' AND ($200_1_R == $123))", transformed);
    }
    
    @Test
    public void testEdgesWithComplexPreconditions() throws Exception {
        Query query = new QueryImpl();
        query.setQuery("SOURCE == '$609_1_R' AND SINK == '$604_1_R' AND TYPE == 'TEST11' AND RELATION == 'REL42-REL42'");
        
        String transformed = logic.getEventQuery(query);
        
        assertEquals("($609_1_R == '$609_1_R' AND $604_1_R == '$604_1_R' AND F05_3_R == 'enrichValue' AND ($609_3 == '2384' && $600_7 == '2'))", transformed);
    }
    
    @Test
    public void testParseWithLucene() throws Exception {
        Query query = new QueryImpl();
        Map<String,QueryParser> parsers = new HashMap<>();
        
        parsers.put("LUCENE", new LuceneToJexlQueryParser());
        logic.setQuerySyntaxParsers(parsers);
        
        query.setQuery("SOURCE:sourceValue SINK:targetValue TYPE:TEST1 RELATION:REL1-REL2 ATTRIBUTE1:SOURCE1-SOURCE2");
        query.addParameter(QueryParameters.QUERY_SYNTAX, "LUCENE");
        
        String transformed = logic.getEventQuery(query);
        
        assertEquals("(SOURCEFIELD == 'sourceValue' AND TARGETFIELD == 'targetValue' AND ENRICHFIELD == 'enrichValue')", transformed);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testManditoryFieldEdgeType() throws Exception {
        Query query = new QueryImpl();
        query.setQuery("SOURCE == 'sourceValue' AND SINK == 'targetValue' AND RELATION == 'REL1-REL2' AND ATTRIBUTE1 == 'SOURCE1-SOURCE2'");
        logic.getEventQuery(query);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testManditoryFieldEdgeRelationship() throws Exception {
        Query query = new QueryImpl();
        query.setQuery("SOURCE == 'sourceValue' AND SINK == 'targetValue' AND TYPE == 'TEST1' AND ATTRIBUTE1 == 'SOURCE1-SOURCE2'");
        logic.getEventQuery(query);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void orAbleFieldtest() throws Exception {
        Query query = new QueryImpl();
        query.setQuery("SOURCE == 'sourceValue' AND SINK == 'targetValue' AND TYPE == 'TEST1' AND RELATION == 'REL1-REL2' AND ATTRIBUTE1 == 'SOURCE1-SOURCE2' AND (ATTRIBUTE2 == 'red' OR ATTRIBUTE2 == 'blue')");
        
        String transformed = logic.getEventQuery(query);
        
        assertEquals("(SOURCEFIELD == 'sourceValue' AND TARGETFIELD == 'targetValue' AND ENRICHFIELD == 'enrichValue')", transformed);
        
        query = new QueryImpl();
        query.setQuery("SOURCE == 'sourceValue' AND SINK == 'targetValue' AND TYPE == 'TEST1' AND RELATION == 'REL1-REL2' AND (ATTRIBUTE1 == 'SOURCE1-SOURCE2' OR ATTRIBUTE1 == 'SOURCE2-SOURCE2')");
        
        transformed = logic.getEventQuery(query); // throws
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void moreOrTesting() throws Exception {
        Query query = new QueryImpl();
        query.setQuery("(SOURCE == 'sourceValue' AND SINK == 'targetValue' AND TYPE == 'TEST1' AND RELATION == 'REL1-REL2' "
                        + "AND ATTRIBUTE1 == 'SOURCE1-SOURCE2' AND (ATTRIBUTE2 == 'red' OR ATTRIBUTE2 == 'blue')) OR "
                        + "(SOURCE == 'sourceValue2' AND SINK == 'targetValue2' AND TYPE == 'TEST1' AND RELATION == 'REL1-REL2' AND ATTRIBUTE1 == 'SOURCE1-SOURCE2')");
        
        String transformed = logic.getEventQuery(query);
        
        assertEquals("(SOURCEFIELD == 'sourceValue' AND TARGETFIELD == 'targetValue' AND ENRICHFIELD == 'enrichValue') "
                        + "OR (SOURCEFIELD == 'sourceValue2' AND TARGETFIELD == 'targetValue2' AND ENRICHFIELD == 'enrichValue')", transformed);
        
        query = new QueryImpl();
        query.setQuery("SOURCE == 'sourceValue' AND SINK == 'targetValue' AND TYPE == 'TEST1' AND RELATION == 'REL1-REL2' AND ATTRIBUTE1 == 'SOURCE1-SOURCE2' AND (ATTRIBUTE2 == 'red' OR ATTRIBUTE3 == 'blue')");
        
        transformed = logic.getEventQuery(query); // throws
        
    }
}
