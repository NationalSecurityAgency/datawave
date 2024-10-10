package datawave.core.geo.function;

import static datawave.core.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import datawave.core.geo.utils.CommonGeoUtils;
import datawave.core.geo.utils.GeoQueryConfig;
import datawave.core.query.jexl.JexlNodeFactory;
import datawave.core.query.jexl.nodes.QueryPropertyMarker;
import datawave.data.type.GeoType;
import datawave.data.type.GeometryType;
import datawave.data.type.PointType;
import datawave.data.type.Type;

public abstract class AbstractGeoFunctionDetails {
    public abstract String getNamespace();

    public abstract String getName();

    public abstract Set<String> getFields();

    public abstract List<Geometry> getGeometry();

    public abstract List<Envelope> getEnvelope();

    public JexlNode generateIndexNode(Map<String,Set<Type<?>>> typesByField, GeoQueryConfig config) {
        Map<String,List<String[]>> termsAndRangesByField = generateTermsAndRangesByField(typesByField, config);
        Map<String,List<JexlNode>> indexNodesByField = generateIndexNodesByField(termsAndRangesByField);

        List<JexlNode> orNodes = new ArrayList<>();
        for (List<JexlNode> indexNodes : indexNodesByField.values()) {
            if (!indexNodes.isEmpty()) {
                orNodes.add(indexNodes.size() > 1 ? JexlNodeFactory.createOrNode(indexNodes) : indexNodes.get(0));
            }
        }

        JexlNode finalNode;
        if (!orNodes.isEmpty()) {
            finalNode = orNodes.size() > 1 ? JexlNodeFactory.createOrNode(orNodes) : orNodes.get(0);
        } else {
            finalNode = new ASTTrueNode(ParserTreeConstants.JJTTRUENODE);
        }

        return finalNode;
    }

    public Map<String,List<JexlNode>> generateIndexNodesByField(Map<String,List<String[]>> termsAndRangesByField) {
        Map<String,List<JexlNode>> indexNodesByField = new LinkedHashMap<>();
        for (String field : termsAndRangesByField.keySet()) {
            for (String[] termOrRange : termsAndRangesByField.get(field)) {
                if (termOrRange.length == 1 || (termOrRange.length == 2 && termOrRange[0].equals(termOrRange[1]))) {
                    // @formatter:off
                    indexNodesByField.computeIfAbsent(field, k -> new ArrayList<>())
                            .add(JexlNodeFactory.buildEQNode(field, termOrRange[0]));
                    // @formatter:on
                } else if (termOrRange.length == 2) {
                    // @formatter:off
                    indexNodesByField.computeIfAbsent(field, k -> new ArrayList<>())
                            .add(QueryPropertyMarker.create(JexlNodeFactory.createAndNode(Arrays.asList(
                                    JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), field, termOrRange[0]),
                                    JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTGENODE), field, termOrRange[1]))), BOUNDED_RANGE));
                        // @formatter:on
                }
            }
        }
        return indexNodesByField;
    }

    public Map<String,List<String[]>> generateTermsAndRangesByField(Map<String,Set<Type<?>>> typesByField, GeoQueryConfig config) {
        Map<String,List<String[]>> termsAndRangesByField = new LinkedHashMap<>();

        Geometry geometry = CommonGeoUtils.geometriesToGeometry(getGeometry());
        if (geometry != null) {

            // save these so we don't keep regenerating them for each field
            Map<Type<?>,List<String[]>> termsAndRangesByType = new LinkedHashMap<>();

            // determine the geo type for each field
            for (String field : getFields()) {
                Set<Type<?>> geoTypes = getGeoTypes(typesByField.get(field));

                List<String[]> allTermsAndRanges = new ArrayList<>();
                for (Type<?> geoType : geoTypes) {
                    allTermsAndRanges.addAll(
                                    termsAndRangesByType.computeIfAbsent(geoType, k -> CommonGeoUtils.generateTermsAndRanges(geometry, geoType, config)));
                }

                termsAndRangesByField.put(field, allTermsAndRanges);
            }
        }

        return termsAndRangesByField;
    }

    private Set<Type<?>> getGeoTypes(Set<Type<?>> types) {
        boolean containsGeometryType = false;
        PointType pointType = null;

        Set<Type<?>> geoTypes = new HashSet<>();
        for (Type<?> type : types) {
            if (type instanceof GeometryType) {
                geoTypes.add(type);
                containsGeometryType = true;
            } else if (type instanceof PointType) {
                pointType = (PointType) type;
            } else if (type instanceof GeoType) {
                geoTypes.add(type);
            }
        }

        // don't add point type unless geo type is not present
        if (!containsGeometryType && pointType != null) {
            geoTypes.add(pointType);
        }
        return geoTypes;
    }
}
