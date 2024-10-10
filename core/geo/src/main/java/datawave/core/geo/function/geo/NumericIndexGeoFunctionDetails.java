package datawave.core.geo.function.geo;

import static datawave.core.geo.function.geo.AbstractQueryGeometry.MAX_LON;
import static datawave.core.geo.function.geo.AbstractQueryGeometry.MIN_LON;
import static datawave.core.geo.utils.GeoUtils.GEO_NAMESPACE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import datawave.core.geo.function.AbstractGeoFunctionDetails;
import datawave.core.geo.function.geo.AbstractQueryGeometry.BoundingBox;
import datawave.core.geo.utils.GeoQueryConfig;
import datawave.core.query.jexl.JexlNodeFactory;
import datawave.data.type.Type;

public class NumericIndexGeoFunctionDetails extends AbstractGeoFunctionDetails {
    private final String name;
    private final String lonField;
    private final String latField;

    // 2 bounding boxes indicates a split over the antimeridian
    private final List<BoundingBox> boundingBoxes;
    private Set<String> fields;
    private List<Geometry> geometry;
    private List<Envelope> envelope;

    public NumericIndexGeoFunctionDetails(String name, String lonField, String latField, double minLon, double maxLon, double minLat, double maxLat) {
        this.name = name;
        this.lonField = lonField;
        this.latField = latField;

        // is the lower left longitude greater than the upper right longitude?
        // if so, we have crossed the anti-meridian and should split
        boundingBoxes = new ArrayList<>();
        if (minLon > maxLon) {
            boundingBoxes.add(new BoundingBox(minLon, MAX_LON, minLat, maxLat));
            boundingBoxes.add(new BoundingBox(MIN_LON, maxLon, minLat, maxLat));
        } else {
            boundingBoxes.add(new BoundingBox(minLon, maxLon, minLat, maxLat));
        }
    }

    @Override
    public String getNamespace() {
        return GEO_NAMESPACE;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Set<String> getFields() {
        if (fields == null) {
            fields = new HashSet<>();
            fields.add(lonField);
            fields.add(latField);
        }
        return fields;
    }

    @Override
    public List<Geometry> getGeometry() {
        if (geometry == null) {
            geometry = boundingBoxes.stream().map(AbstractQueryGeometry::getGeometry).collect(Collectors.toList());
        }
        return geometry;
    }

    @Override
    public List<Envelope> getEnvelope() {
        if (envelope == null) {
            envelope = boundingBoxes.stream().map(AbstractQueryGeometry::getEnvelope).collect(Collectors.toList());
        }
        return envelope;
    }

    @Override
    public JexlNode generateIndexNode(Map<String,Set<Type<?>>> typesByField, GeoQueryConfig config) {
        Map<String,List<String[]>> termsAndRangesByField = generateTermsAndRangesByField(typesByField, config);
        Map<String,List<JexlNode>> indexNodesByField = generateIndexNodesByField(termsAndRangesByField);

        List<JexlNode> indexNodes = new ArrayList<>();
        Iterator<JexlNode> lonRangeIter = indexNodesByField.get(lonField).iterator();
        Iterator<JexlNode> latRangeIter = indexNodesByField.get(latField).iterator();
        while (lonRangeIter.hasNext() && latRangeIter.hasNext()) {
            // @formatter:off
            indexNodes.add(JexlNodeFactory.createAndNode(Arrays.asList(
                    lonRangeIter.next(),
                    latRangeIter.next())));
            // @formatter:on
        }

        JexlNode finalNode;
        if (!indexNodes.isEmpty()) {
            finalNode = indexNodes.size() > 1 ? JexlNodeFactory.createOrNode(indexNodes) : indexNodes.get(0);
        } else {
            finalNode = new ASTTrueNode(ParserTreeConstants.JJTTRUENODE);
        }
        return finalNode;
    }

    @Override
    public Map<String,List<String[]>> generateTermsAndRangesByField(Map<String,Set<Type<?>>> typesByField, GeoQueryConfig config) {
        Map<String,List<String[]>> termsAndRangesByField = new HashMap<>();
        for (BoundingBox boundingBox : boundingBoxes) {
            addRange(termsAndRangesByField, lonField, boundingBox.getMinLon(), boundingBox.getMaxLon());
            addRange(termsAndRangesByField, latField, boundingBox.getMinLat(), boundingBox.getMaxLat());
        }
        return termsAndRangesByField;
    }

    private void addRange(Map<String,List<String[]>> termsAndRangesByField, String field, double min, double max) {
        // @formatter:off
        termsAndRangesByField.computeIfAbsent(field, k -> new ArrayList<>())
                .add(new String[]{
                        Double.toString(min),
                        Double.toString(max)});
        // @formatter:on
    }

    public String getLonField() {
        return lonField;
    }

    public String getLatField() {
        return latField;
    }

    public List<BoundingBox> getBoundingBoxes() {
        return boundingBoxes;
    }
}
