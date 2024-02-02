package datawave.query.attributes;

import org.apache.accumulo.core.data.Key;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.util.GeometricShapeFactory;

public class GeometryTest extends AttributeTest {

    @Test
    public void validateSerializationOfToKeepFlag() {
        String geometry = createGeometry();
        Key docKey = new Key("shard", "datatype\0uid");

        Geometry attr = new Geometry(geometry, docKey, false);
        testToKeep(attr, false);

        attr = new Geometry(geometry, docKey, true);
        testToKeep(attr, true);
    }

    private String createGeometry() {
        GeometricShapeFactory shapeFactory = new GeometricShapeFactory();
        shapeFactory.setNumPoints(32);
        shapeFactory.setCentre(new Coordinate(1, 2));
        shapeFactory.setSize(1.5 * 2);
        return String.valueOf(shapeFactory.createCircle());
    }
}
