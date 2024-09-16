package datawave.query.map;

import static datawave.query.QueryParameters.QUERY_SYNTAX;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import datawave.microservice.query.QueryImpl;
import datawave.microservice.querymetric.QueryGeometry;
import datawave.microservice.querymetric.QueryGeometryResponse;
import datawave.microservice.querymetric.QueryMetric;
import datawave.webservice.query.exception.QueryExceptionType;

public class SimpleQueryGeometryHandlerTest {

    private SimpleQueryGeometryHandler handler;

    private String commonId;

    private Set<QueryImpl.Parameter> luceneParams;
    private Set<QueryImpl.Parameter> jexlParams;
    private Set<QueryImpl.Parameter> emptyParams;

    @Before
    public void setup() {
        handler = new SimpleQueryGeometryHandler();

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

        Assert.assertEquals(1, resp.getResult().size());
        Assert.assertNull(resp.getExceptions());

        QueryGeometry queryGeometry = resp.getResult().get(0);
        Assert.assertEquals(
                        "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[0.0,0.0]},\"properties\":{\"functions\":[\"geowave:contains(field1, 'POINT(0 0)')\"],\"fields\":[\"field1\"],\"wkt\":\"POINT (0 0)\"},\"id\":\"geowave:contains(field1, 'POINT(0 0)')\"}",
                        queryGeometry.getGeometry());
        Assert.assertEquals("geowave:contains(field1, 'POINT(0 0)')", queryGeometry.getFunction());
    }

    @Test
    public void validGeoQueryJexlTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "geo:within_bounding_box(field1, '0_0', '10_10')", jexlParams);

        Assert.assertEquals(1, resp.getResult().size());
        Assert.assertNull(resp.getExceptions());

        QueryGeometry queryGeometry = resp.getResult().get(0);
        Assert.assertEquals(
                        "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[0.0,0.0],[10,0.0],[10,10],[0.0,10],[0.0,0.0]]]},\"properties\":{\"functions\":[\"geo:within_bounding_box(field1, '0_0', '10_10')\"],\"fields\":[\"field1\"],\"wkt\":\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"},\"id\":\"geo:within_bounding_box(field1, '0_0', '10_10')\"}",
                        queryGeometry.getGeometry());
        Assert.assertEquals("geo:within_bounding_box(field1, '0_0', '10_10')", queryGeometry.getFunction());
    }

    @Test
    public void validQueryLuceneTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "#COVERS(field2, 'POINT(1 1)')", luceneParams);

        Assert.assertEquals(1, resp.getResult().size());
        Assert.assertNull(resp.getExceptions());

        QueryGeometry queryGeometry = resp.getResult().get(0);
        Assert.assertEquals(
                        "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[1,1]},\"properties\":{\"functions\":[\"geowave:covers(field2, 'POINT(1 1)')\"],\"fields\":[\"field2\"],\"wkt\":\"POINT (1 1)\"},\"id\":\"geowave:covers(field2, 'POINT(1 1)')\"}",
                        queryGeometry.getGeometry());
        Assert.assertEquals("geowave:covers(field2, 'POINT(1 1)')", queryGeometry.getFunction());
    }

    @Test
    public void validGeoBoxQueryLuceneTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "#GEO(bounding_box, field1, '0_0', '10_10')", luceneParams);

        Assert.assertEquals(1, resp.getResult().size());
        Assert.assertNull(resp.getExceptions());

        QueryGeometry queryGeometry = resp.getResult().get(0);
        Assert.assertEquals(
                        "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[0.0,0.0],[10,0.0],[10,10],[0.0,10],[0.0,0.0]]]},\"properties\":{\"functions\":[\"geo:within_bounding_box(field1, '0_0', '10_10')\"],\"fields\":[\"field1\"],\"wkt\":\"POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))\"},\"id\":\"geo:within_bounding_box(field1, '0_0', '10_10')\"}",
                        queryGeometry.getGeometry());
        Assert.assertEquals("geo:within_bounding_box(field1, '0_0', '10_10')", queryGeometry.getFunction());
    }

    @Test
    public void validGeoCircleQueryLuceneTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "#GEO(circle, field1, '0_0', 10)", luceneParams);

        Assert.assertEquals(1, resp.getResult().size());
        Assert.assertNull(resp.getExceptions());

        QueryGeometry queryGeometry = resp.getResult().get(0);
        Assert.assertEquals(
                        "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[[10,0.0],[9.945219,1.045285],[9.781476,2.079117],[9.510565,3.09017],[9.135455,4.067366],[8.660254,5],[8.09017,5.877853],[7.431448,6.691306],[6.691306,7.431448],[5.877853,8.09017],[5,8.660254],[4.067366,9.135455],[3.09017,9.510565],[2.079117,9.781476],[1.045285,9.945219],[0.0,10],[-1.045285,9.945219],[-2.079117,9.781476],[-3.09017,9.510565],[-4.067366,9.135455],[-5,8.660254],[-5.877853,8.09017],[-6.691306,7.431448],[-7.431448,6.691306],[-8.09017,5.877853],[-8.660254,5],[-9.135455,4.067366],[-9.510565,3.09017],[-9.781476,2.079117],[-9.945219,1.045285],[-10,0.0],[-9.945219,-1.045285],[-9.781476,-2.079117],[-9.510565,-3.09017],[-9.135455,-4.067366],[-8.660254,-5],[-8.09017,-5.877853],[-7.431448,-6.691306],[-6.691306,-7.431448],[-5.877853,-8.09017],[-5,-8.660254],[-4.067366,-9.135455],[-3.09017,-9.510565],[-2.079117,-9.781476],[-1.045285,-9.945219],[0.0,-10],[1.045285,-9.945219],[2.079117,-9.781476],[3.09017,-9.510565],[4.067366,-9.135455],[5,-8.660254],[5.877853,-8.09017],[6.691306,-7.431448],[7.431448,-6.691306],[8.09017,-5.877853],[8.660254,-5],[9.135455,-4.067366],[9.510565,-3.09017],[9.781476,-2.079117],[9.945219,-1.045285],[10,0.0]]]},\"properties\":{\"functions\":[\"geo:within_circle(field1, '0_0', 10)\"],\"fields\":[\"field1\"],\"wkt\":\"POLYGON ((10 0, 9.945219 1.045285, 9.781476 2.079117, 9.510565 3.09017, 9.135455 4.067366, 8.660254 5, 8.09017 5.877853, 7.431448 6.691306, 6.691306 7.431448, 5.877853 8.09017, 5 8.660254, 4.067366 9.135455, 3.09017 9.510565, 2.079117 9.781476, 1.045285 9.945219, 0 10, -1.045285 9.945219, -2.079117 9.781476, -3.09017 9.510565, -4.067366 9.135455, -5 8.660254, -5.877853 8.09017, -6.691306 7.431448, -7.431448 6.691306, -8.09017 5.877853, -8.660254 5, -9.135455 4.067366, -9.510565 3.09017, -9.781476 2.079117, -9.945219 1.045285, -10 0, -9.945219 -1.045285, -9.781476 -2.079117, -9.510565 -3.09017, -9.135455 -4.067366, -8.660254 -5, -8.09017 -5.877853, -7.431448 -6.691306, -6.691306 -7.431448, -5.877853 -8.09017, -5 -8.660254, -4.067366 -9.135455, -3.09017 -9.510565, -2.079117 -9.781476, -1.045285 -9.945219, 0 -10, 1.045285 -9.945219, 2.079117 -9.781476, 3.09017 -9.510565, 4.067366 -9.135455, 5 -8.660254, 5.877853 -8.09017, 6.691306 -7.431448, 7.431448 -6.691306, 8.09017 -5.877853, 8.660254 -5, 9.135455 -4.067366, 9.510565 -3.09017, 9.781476 -2.079117, 9.945219 -1.045285, 10 0))\"},\"id\":\"geo:within_circle(field1, '0_0', 10)\"}",
                        queryGeometry.getGeometry());
        Assert.assertEquals("geo:within_circle(field1, '0_0', 10)", queryGeometry.getFunction());
    }

    @Test
    public void validJexlQueryUndefinedSyntaxTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "geowave:covered_by(field3, 'POINT(2 2)')", emptyParams);

        Assert.assertEquals(1, resp.getResult().size());
        Assert.assertNull(resp.getExceptions());

        QueryGeometry queryGeometry = resp.getResult().get(0);
        Assert.assertEquals(
                        "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[2,2]},\"properties\":{\"functions\":[\"geowave:covered_by(field3, 'POINT(2 2)')\"],\"fields\":[\"field3\"],\"wkt\":\"POINT (2 2)\"},\"id\":\"geowave:covered_by(field3, 'POINT(2 2)')\"}",
                        queryGeometry.getGeometry());
        Assert.assertEquals("geowave:covered_by(field3, 'POINT(2 2)')", queryGeometry.getFunction());
    }

    @Test
    public void validLuceneQueryUndefinedSyntaxTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "#CROSSES(field4, 'POINT(3 3)')", emptyParams);

        Assert.assertEquals(0, resp.getResult().size());
        Assert.assertEquals(1, resp.getExceptions().size());

        QueryExceptionType queryExceptionType = resp.getExceptions().get(0);
        Assert.assertEquals("Unable to parse the geo features", queryExceptionType.getMessage());
    }

    @Test
    public void validMultiFunctionQueryJexlTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "geowave:intersects(field5, 'POINT(4 4)') || geowave:overlaps(field6, 'POINT(5 5)')",
                        jexlParams);

        Assert.assertEquals(2, resp.getResult().size());
        Assert.assertNull(resp.getExceptions());

        QueryGeometry queryGeometry = resp.getResult().get(0);
        Assert.assertEquals(
                        "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[4,4]},\"properties\":{\"functions\":[\"geowave:intersects(field5, 'POINT(4 4)')\"],\"fields\":[\"field5\"],\"wkt\":\"POINT (4 4)\"},\"id\":\"geowave:intersects(field5, 'POINT(4 4)')\"}",
                        queryGeometry.getGeometry());
        Assert.assertEquals("geowave:intersects(field5, 'POINT(4 4)')", queryGeometry.getFunction());

        queryGeometry = resp.getResult().get(1);
        Assert.assertEquals(
                        "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[5,5]},\"properties\":{\"functions\":[\"geowave:overlaps(field6, 'POINT(5 5)')\"],\"fields\":[\"field6\"],\"wkt\":\"POINT (5 5)\"},\"id\":\"geowave:overlaps(field6, 'POINT(5 5)')\"}",
                        queryGeometry.getGeometry());
        Assert.assertEquals("geowave:overlaps(field6, 'POINT(5 5)')", queryGeometry.getFunction());
    }

    @Test
    public void validMultiFunctionQueryLuceneTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "#INTERSECTS(field7, 'POINT(6 6)') || #WITHIN(field8, 'POINT(7 7)')", luceneParams);

        Assert.assertEquals(2, resp.getResult().size());
        Assert.assertNull(resp.getExceptions());

        QueryGeometry queryGeometry = resp.getResult().get(0);
        Assert.assertEquals(
                        "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[6,6]},\"properties\":{\"functions\":[\"geowave:intersects(field7, 'POINT(6 6)')\"],\"fields\":[\"field7\"],\"wkt\":\"POINT (6 6)\"},\"id\":\"geowave:intersects(field7, 'POINT(6 6)')\"}",
                        queryGeometry.getGeometry());
        Assert.assertEquals("geowave:intersects(field7, 'POINT(6 6)')", queryGeometry.getFunction());

        queryGeometry = resp.getResult().get(1);
        Assert.assertEquals(
                        "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[7,7]},\"properties\":{\"functions\":[\"geowave:within(field8, 'POINT(7 7)')\"],\"fields\":[\"field8\"],\"wkt\":\"POINT (7 7)\"},\"id\":\"geowave:within(field8, 'POINT(7 7)')\"}",
                        queryGeometry.getGeometry());
        Assert.assertEquals("geowave:within(field8, 'POINT(7 7)')", queryGeometry.getFunction());
    }

    @Test
    public void validNonGeoQueryLuceneTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "field9: 'term'", luceneParams);

        Assert.assertEquals(0, resp.getResult().size());
        Assert.assertNull(resp.getExceptions());
    }

    @Test
    public void invalidQueryJexlTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "geowave:intersects(field11, 3000)", jexlParams);

        Assert.assertEquals(0, resp.getResult().size());
        Assert.assertNull(resp.getExceptions());
    }

    @Test
    public void invalidQueryLuceneTest() {
        QueryGeometryResponse resp = generateResponse(commonId, "#INTERSECTS(field12, 5000)", luceneParams);

        Assert.assertEquals(0, resp.getResult().size());
        Assert.assertNull(resp.getExceptions());
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

        Assert.assertEquals(2, resp.getResult().size());

        QueryGeometry queryGeometry = resp.getResult().get(0);
        Assert.assertEquals(
                        "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[1,1]},\"properties\":{\"functions\":[\"geowave:covers(field1, 'POINT(1 1)')\"],\"fields\":[\"field1\"],\"wkt\":\"POINT (1 1)\"},\"id\":\"geowave:covers(field1, 'POINT(1 1)')\"}",
                        queryGeometry.getGeometry());
        Assert.assertEquals("geowave:covers(field1, 'POINT(1 1)')", queryGeometry.getFunction());

        queryGeometry = resp.getResult().get(1);
        Assert.assertEquals(
                        "{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[2,2]},\"properties\":{\"functions\":[\"geowave:intersects(field2, 'POINT(2 2)')\"],\"fields\":[\"field2\"],\"wkt\":\"POINT (2 2)\"},\"id\":\"geowave:intersects(field2, 'POINT(2 2)')\"}",
                        queryGeometry.getGeometry());
        Assert.assertEquals("geowave:intersects(field2, 'POINT(2 2)')", queryGeometry.getFunction());

        System.out.println("done!");
    }
}
