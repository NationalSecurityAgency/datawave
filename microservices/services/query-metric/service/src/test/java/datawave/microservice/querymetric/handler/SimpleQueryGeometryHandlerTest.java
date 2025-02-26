package datawave.microservice.querymetric.handler;

import static datawave.query.QueryParameters.QUERY_SYNTAX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.QueryGeometry;
import datawave.microservice.querymetric.QueryGeometryResponse;
import datawave.microservice.querymetric.QueryMetric;
import datawave.microservice.querymetric.config.QueryMetricHandlerProperties;
import datawave.microservice.querymetric.factory.QueryMetricResponseFactory;
import datawave.webservice.query.exception.QueryExceptionType;

public class SimpleQueryGeometryHandlerTest {
    
    private SimpleQueryGeometryHandler handler;
    
    private String commonId;
    
    private Set<QueryImpl.Parameter> luceneParams;
    private Set<QueryImpl.Parameter> jexlParams;
    private Set<QueryImpl.Parameter> emptyParams;
    
    @BeforeEach
    public void setup() {
        handler = new SimpleQueryGeometryHandler(new QueryMetricHandlerProperties());
        handler.setQueryMetricResponseFactory(new QueryMetricResponseFactory(null, null));
        commonId = "super-special-query-id";
        
        luceneParams = new HashSet<>();
        luceneParams.add(new QueryImpl.Parameter(QUERY_SYNTAX, "LUCENE"));
        
        jexlParams = new HashSet<>();
        jexlParams.add(new QueryImpl.Parameter(QUERY_SYNTAX, "JEXL"));
        
        emptyParams = new HashSet<>();
    }
    
    public QueryGeometryResponse generateResponse(String id, String query, Set<QueryImpl.Parameter> params) {
        List<QueryMetric> queryMetrics = new ArrayList<>();
        
        QueryMetric qm = new QueryMetric();
        qm.setQueryId(id);
        qm.setQuery(query);
        qm.setParameters(params);
        queryMetrics.add(qm);
        
        return handler.getQueryGeometryResponse(id, queryMetrics);
    }
    
    @Test
    public void validQueryJexlTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "geowave:contains(field1, 'POINT(0 0)')", jexlParams);
        
        assertEquals(1, resp.getResult().size());
        assertNull(resp.getExceptions());
        
        QueryGeometry queryGeometry = resp.getResult().get(0);
        assertEquals("{\"type\":\"Point\",\"coordinates\":[0.0,0.0]}", queryGeometry.getGeometry());
        assertEquals("geowave:contains(field1, 'POINT(0 0)')", queryGeometry.getFunction());
    }
    
    @Test
    public void validGeoQueryJexlTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "geo:within_bounding_box(field1, '0_0', '10_10')", jexlParams);
        
        assertEquals(1, resp.getResult().size());
        assertNull(resp.getExceptions());
        
        QueryGeometry queryGeometry = resp.getResult().get(0);
        assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[0.0,0.0],[10,0.0],[10,10],[0.0,10],[0.0,0.0]]]}", queryGeometry.getGeometry());
        assertEquals("geo:within_bounding_box(field1, '0_0', '10_10')", queryGeometry.getFunction());
    }
    
    @Test
    public void validQueryLuceneTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "#COVERS(field2, 'POINT(1 1)')", luceneParams);
        
        assertEquals(1, resp.getResult().size());
        assertNull(resp.getExceptions());
        
        QueryGeometry queryGeometry = resp.getResult().get(0);
        assertEquals("{\"type\":\"Point\",\"coordinates\":[1,1]}", queryGeometry.getGeometry());
        assertEquals("#COVERS(field2, 'POINT(1 1)')", queryGeometry.getFunction());
    }
    
    @Test
    public void validGeoBoxQueryLuceneTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "#GEO(bounding_box, field1, '0_0', '10_10')", luceneParams);
        
        assertEquals(1, resp.getResult().size());
        assertNull(resp.getExceptions());
        
        QueryGeometry queryGeometry = resp.getResult().get(0);
        assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[0.0,0.0],[10,0.0],[10,10],[0.0,10],[0.0,0.0]]]}", queryGeometry.getGeometry());
        assertEquals("#GEO(bounding_box, field1, '0_0', '10_10')", queryGeometry.getFunction());
    }
    
    @Test
    public void validGeoCircleQueryLuceneTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "#GEO(circle, field1, '0_0', 10)", luceneParams);
        
        assertEquals(1, resp.getResult().size());
        assertNull(resp.getExceptions());
        
        QueryGeometry queryGeometry = resp.getResult().get(0);
        assertEquals("{\"type\":\"Polygon\",\"coordinates\":[[[10,0.0],[9.9452,1.0453],[9.7815,2.0791],[9.5106,3.0902],[9.1355,4.0674],[8.6603,5],[8.0902,5.8779],[7.4314,6.6913],[6.6913,7.4314],[5.8779,8.0902],[5,8.6603],[4.0674,9.1355],[3.0902,9.5106],[2.0791,9.7815],[1.0453,9.9452],[6.0E-16,10],[-1.0453,9.9452],[-2.0791,9.7815],[-3.0902,9.5106],[-4.0674,9.1355],[-5,8.6603],[-5.8779,8.0902],[-6.6913,7.4314],[-7.4314,6.6913],[-8.0902,5.8779],[-8.6603,5],[-9.1355,4.0674],[-9.5106,3.0902],[-9.7815,2.0791],[-9.9452,1.0453],[-10,1.2E-15],[-9.9452,-1.0453],[-9.7815,-2.0791],[-9.5106,-3.0902],[-9.1355,-4.0674],[-8.6603,-5],[-8.0902,-5.8779],[-7.4314,-6.6913],[-6.6913,-7.4314],[-5.8779,-8.0902],[-5,-8.6603],[-4.0674,-9.1355],[-3.0902,-9.5106],[-2.0791,-9.7815],[-1.0453,-9.9452],[-1.8E-15,-10],[1.0453,-9.9452],[2.0791,-9.7815],[3.0902,-9.5106],[4.0674,-9.1355],[5,-8.6603],[5.8779,-8.0902],[6.6913,-7.4314],[7.4314,-6.6913],[8.0902,-5.8779],[8.6603,-5],[9.1355,-4.0674],[9.5106,-3.0902],[9.7815,-2.0791],[9.9452,-1.0453],[10,0.0]]]}",
                        queryGeometry.getGeometry());
        assertEquals("#GEO(circle, field1, '0_0', 10)", queryGeometry.getFunction());
    }
    
    @Test
    public void validJexlQueryUndefinedSyntaxTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "geowave:covered_by(field3, 'POINT(2 2)')", emptyParams);
        
        assertEquals(1, resp.getResult().size());
        assertNull(resp.getExceptions());
        
        QueryGeometry queryGeometry = resp.getResult().get(0);
        assertEquals("{\"type\":\"Point\",\"coordinates\":[2,2]}", queryGeometry.getGeometry());
        assertEquals("geowave:covered_by(field3, 'POINT(2 2)')", queryGeometry.getFunction());
    }
    
    @Test
    public void validLuceneQueryUndefinedSyntaxTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "#CROSSES(field4, 'POINT(3 3)')", emptyParams);
        
        assertEquals(0, resp.getResult().size());
        assertEquals(1, resp.getExceptions().size());
        
        QueryExceptionType queryExceptionType = resp.getExceptions().get(0);
        assertEquals("Unable to parse the geo features", queryExceptionType.getMessage());
    }
    
    @Test
    public void validMultiFunctionQueryJexlTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "geowave:intersects(field5, 'POINT(4 4)') || geowave:overlaps(field6, 'POINT(5 5)')",
                        jexlParams);
        
        assertEquals(2, resp.getResult().size());
        assertNull(resp.getExceptions());
        
        QueryGeometry queryGeometry = resp.getResult().get(0);
        assertEquals("{\"type\":\"Point\",\"coordinates\":[4,4]}", queryGeometry.getGeometry());
        assertEquals("geowave:intersects(field5, 'POINT(4 4)')", queryGeometry.getFunction());
        
        queryGeometry = resp.getResult().get(1);
        assertEquals("{\"type\":\"Point\",\"coordinates\":[5,5]}", queryGeometry.getGeometry());
        assertEquals("geowave:overlaps(field6, 'POINT(5 5)')", queryGeometry.getFunction());
    }
    
    @Test
    public void validMultiFunctionQueryLuceneTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "#INTERSECTS(field7, 'POINT(6 6)') || #WITHIN(field8, 'POINT(7 7)')", luceneParams);
        
        assertEquals(2, resp.getResult().size());
        assertNull(resp.getExceptions());
        
        QueryGeometry queryGeometry = resp.getResult().get(0);
        assertEquals("{\"type\":\"Point\",\"coordinates\":[6,6]}", queryGeometry.getGeometry());
        assertEquals("#INTERSECTS(field7, 'POINT(6 6)')", queryGeometry.getFunction());
        
        queryGeometry = resp.getResult().get(1);
        assertEquals("{\"type\":\"Point\",\"coordinates\":[7,7]}", queryGeometry.getGeometry());
        assertEquals("#WITHIN(field8, 'POINT(7 7)')", queryGeometry.getFunction());
    }
    
    @Test
    public void validNonGeoQueryLuceneTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "field9: 'term'", luceneParams);
        
        assertEquals(0, resp.getResult().size());
        assertNull(resp.getExceptions());
    }
    
    @Test
    public void invalidQueryJexlTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "geowave:intersects(field11, 3000)", jexlParams);
        
        assertEquals(0, resp.getResult().size());
        assertNull(resp.getExceptions());
    }
    
    @Test
    public void invalidQueryLuceneTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "#INTERSECTS(field12, 5000)", luceneParams);
        
        assertEquals(0, resp.getResult().size());
        assertNull(resp.getExceptions());
    }
    
    @Test
    public void multipleQueryMetricsTest() {
        List<QueryMetric> queryMetrics = new ArrayList<>();
        
        // Valid query, Lucene syntax QueryMetric
        QueryMetric qm = new QueryMetric();
        qm.setQueryId(commonId);
        qm.setQuery("#COVERS(field1, 'POINT(1 1)')");
        qm.setParameters(luceneParams);
        queryMetrics.add(qm);
        
        // Valid query, unique query id, Jexl syntax QueryMetric
        qm = new QueryMetric();
        qm.setQueryId("special-snowflake-id");
        qm.setQuery("geowave:intersects(field2, 'POINT(2 2)')");
        qm.setParameters(jexlParams);
        queryMetrics.add(qm);
        
        QueryGeometryResponse resp = handler.getQueryGeometryResponse(commonId, queryMetrics);
        
        assertEquals(2, resp.getResult().size());
        
        QueryGeometry queryGeometry = resp.getResult().get(0);
        assertEquals("{\"type\":\"Point\",\"coordinates\":[1,1]}", queryGeometry.getGeometry());
        assertEquals("#COVERS(field1, 'POINT(1 1)')", queryGeometry.getFunction());
        
        queryGeometry = resp.getResult().get(1);
        assertEquals("{\"type\":\"Point\",\"coordinates\":[2,2]}", queryGeometry.getGeometry());
        assertEquals("geowave:intersects(field2, 'POINT(2 2)')", queryGeometry.getFunction());
        
        System.out.println("done!");
    }
}
