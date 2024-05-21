package datawave.microservice.map;

import java.util.ArrayList;
import java.util.List;

import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geojson.feature.FeatureJSON;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeatureType;

public class GeoJSONTest {
    
    @Test
    public void geoJsonTest() throws Exception {
        // @formatter:off
        String geoFeatureJSON = "{ \"type\": \"FeatureCollection\",\n" +
                "  \"features\": [\n" +
                "    { \"type\": \"Feature\",\n" +
                "      \"geometry\": {\"type\": \"Point\", \"coordinates\": [102.0, 0.5]},\n" +
                "      \"properties\": {\"prop0\": \"value0\"}\n" +
                "      },\n" +
                "    { \"type\": \"Feature\",\n" +
                "      \"geometry\": {\n" +
                "        \"type\": \"LineString\",\n" +
                "        \"coordinates\": [\n" +
                "          [102.0, 0.0], [103.0, 1.0], [104.0, 0.0], [105.0, 1.0]\n" +
                "          ]\n" +
                "        },\n" +
                "      \"properties\": {\n" +
                "        \"prop0\": \"value0\",\n" +
                "        \"prop1\": 0.0\n" +
                "        }\n" +
                "      },\n" +
                "    { \"type\": \"Feature\",\n" +
                "       \"geometry\": {\n" +
                "         \"type\": \"Polygon\",\n" +
                "         \"coordinates\": [\n" +
                "           [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],\n" +
                "             [100.0, 1.0], [100.0, 0.0] ]\n" +
                "           ]\n" +
                "\n" +
                "       },\n" +
                "       \"properties\": {\n" +
                "         \"prop0\": \"value0\",\n" +
                "         \"prop1\": {\"this\": \"that\"}\n" +
                "         }\n" +
                "       }\n" +
                "    ]\n" +
                "  }";
        // @formatter:on
        
        GeometryFactory geomFact = new GeometryFactory();
        List<Geometry> geometries = new ArrayList<>();
        FeatureJSON featureJSON = new FeatureJSON();
        SimpleFeatureType simpleFeatureType = featureJSON.readFeatureCollectionSchema(geoFeatureJSON, false);
        
        // fix the geometry type
        SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
        sftBuilder.init(simpleFeatureType);
        String defaultGeometry = sftBuilder.getDefaultGeometry();
        sftBuilder.remove(defaultGeometry);
        sftBuilder.add(defaultGeometry, Geometry.class);
        
        featureJSON.setFeatureType(sftBuilder.buildFeatureType());
        FeatureCollection<?,?> featColl = featureJSON.readFeatureCollection(geoFeatureJSON);
        FeatureIterator<?> featIter = featColl.features();
        while (featIter.hasNext()) {
            Feature feat = featIter.next();
            if (feat.getDefaultGeometryProperty().getValue() instanceof Geometry) {
                geometries.add((Geometry) feat.getDefaultGeometryProperty().getValue());
            }
        }
        String wkt = (geometries.size() > 1) ? geomFact.createGeometryCollection(geometries.toArray(new Geometry[0])).toText() : geometries.get(0).toText();
        
        System.out.println(wkt);
    }
}
