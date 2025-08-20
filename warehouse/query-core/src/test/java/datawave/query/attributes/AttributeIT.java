package datawave.query.attributes;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.util.GeometricShapeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.data.type.LcType;
import datawave.next.stats.StatUtil;
import datawave.query.common.grouping.GroupingAttribute;

/**
 * This test uses {@link Document#put(String, Attribute)} to verify changes to implementations of {@link Attribute}
 */
public abstract class AttributeIT {

    private static final Logger log = LoggerFactory.getLogger(AttributeIT.class);

    protected final int documentSize = 100;
    protected final int numIterations = 100_000;

    protected Key docKey = new Key("row", "dt\0uid");

    /**
     * Provides a hook to tell which extending class is exercising the abstract test
     *
     * @return the context
     */
    protected abstract String getContext();

    /**
     * Create an attribute from an index
     *
     * @param index
     *            the index
     * @return an attribute
     */
    protected Attribute<?> createAttribute(int index) {
        return createAttribute("value-" + index);
    }

    /**
     * Create an attribute from a value
     *
     * @param value
     *            the value
     * @return an attribute
     */
    protected abstract Attribute<?> createAttribute(String value);

    @Test
    public void testDocumentPut() {
        testDocumentPut(documentSize, numIterations);
    }

    /**
     * Hook to allow certain tests to run with fewer iterations
     *
     * @param documentSize
     *            the document size
     * @param numIterations
     *            the number of iterations
     */
    protected void testDocumentPut(int documentSize, int numIterations) {
        List<Attribute<?>> attributes = new ArrayList<>();
        for (int i = 0; i < documentSize; i++) {
            attributes.add(createAttribute(i));
        }

        long total = 0;

        for (int i = 0; i < numIterations; i++) {
            final Document d = new Document();
            long start = System.nanoTime();
            for (Attribute<?> attribute : attributes) {
                d.put("FIELD", attribute);
            }
            total += System.nanoTime() - start;
        }

        log.info("{} took {}", getContext(), StatUtil.formatNanos(total));
    }

    static class ContentIT extends AttributeIT {

        @Override
        protected String getContext() {
            return "content";
        }

        @Override
        protected Attribute<?> createAttribute(String value) {
            return new Content(value, docKey, true);
        }
    }

    static class GeometryIT extends AttributeIT {

        private final GeometricShapeFactory shapeFactory = new GeometricShapeFactory();

        @Override
        protected String getContext() {
            return "geometry";
        }

        @Override
        protected Attribute<?> createAttribute(int index) {
            String geometry = createGeometry(index, index);
            return createAttribute(geometry);
        }

        @Override
        protected Attribute<?> createAttribute(String value) {
            return new Geometry(value, docKey, true);
        }

        private String createGeometry(int x, int y) {
            shapeFactory.setNumPoints(32);
            shapeFactory.setCentre(new Coordinate(x, y));
            shapeFactory.setSize(1.5 * 2);
            return String.valueOf(shapeFactory.createCircle());
        }
    }

    static class GroupingAttributeIT extends AttributeIT {

        @Override
        protected String getContext() {
            return "grouping";
        }

        @Override
        protected Attribute<?> createAttribute(String value) {
            LcType type = new LcType(value);
            return new GroupingAttribute<>(type, docKey, true);
        }
    }

    static class IpAddressIT extends AttributeIT {

        @Override
        protected String getContext() {
            return "ipaddress";
        }

        @Override
        protected Attribute<?> createAttribute(int index) {
            int third = index / 255;
            int fourth = index % 255;
            String ip = "192.168." + third + "." + fourth;
            return createAttribute(ip);
        }

        @Override
        protected Attribute<?> createAttribute(String value) {
            return new IpAddress(value, docKey, true);
        }
    }

    static class NumericIT extends AttributeIT {

        @Override
        protected String getContext() {
            return "numeric";
        }

        @Override
        protected Attribute<?> createAttribute(int index) {
            return createAttribute(String.valueOf(index));
        }

        @Override
        protected Attribute<?> createAttribute(String value) {
            return new Numeric(value, docKey, true);
        }
    }

    static class PreNormalizedAttributeIT extends AttributeIT {
        @Override
        protected String getContext() {
            return "prenormalized";
        }

        @Override
        protected Attribute<?> createAttribute(String value) {
            return new PreNormalizedAttribute(value, docKey, true);
        }
    }

    static class TypeAttributeIT extends AttributeIT {

        @Override
        protected String getContext() {
            return "typeattribute";
        }

        @Override
        protected Attribute<?> createAttribute(String value) {
            LcType type = new LcType(value);
            return new TypeAttribute<>(type, docKey, true);
        }
    }
}
