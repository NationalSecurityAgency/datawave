package datawave.query.jexl.functions;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.apache.log4j.Logger;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.util.GeometricShapeFactory;

import com.google.common.collect.Lists;

import datawave.data.normalizer.GeoNormalizer;
import datawave.data.normalizer.GeoNormalizer.GeoPoint;
import datawave.data.normalizer.GeoNormalizer.OutOfRangeException;
import datawave.data.normalizer.GeoNormalizer.ParseException;
import datawave.data.normalizer.Normalizer;
import datawave.data.type.AbstractGeometryType;
import datawave.data.type.GeoType;
import datawave.data.type.Type;
import datawave.query.attributes.AttributeFactory;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.functions.arguments.RebuildingJexlArgumentDescriptor;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.GeoUtils;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * This is the descriptor class for performing geo functions. It supports basic spatial relationships against points.
 *
 */
public class GeoFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory {

    private static final Logger log = Logger.getLogger(GeoFunctionsDescriptor.class);

    private static final int NUM_CIRCLE_POINTS = 60;
    private static final String WITHIN_BOUNDING_BOX = "within_bounding_box";
    private static final String WITHIN_CIRCLE = "within_circle";

    /**
     * This is the argument descriptor which can be used to normalize and optimize function node queries
     *
     * This rebuilding argument descriptor will ensure that if any of the query fields are GeoWave Geometry types that they are removed from the geo query
     * function and placed into the equivalent GeoWave query function.
     *
     */
    public static class GeoJexlArgumentDescriptor implements RebuildingJexlArgumentDescriptor {

        private final ASTFunctionNode node;
        private final String namespace, name;
        private final List<JexlNode> args;

        public GeoJexlArgumentDescriptor(ASTFunctionNode node, String namespace, String name, List<JexlNode> args) {
            this.node = node;
            this.namespace = namespace;
            this.name = name;
            this.args = args;
        }

        @Override
        public JexlNode getIndexQuery(ShardQueryConfiguration config, MetadataHelper helper, DateIndexHelper dateIndexHelper, Set<String> datatypeFilter) {
            // return the true node if unable to parse arguments
            JexlNode returnNode = TRUE_NODE;

            if (name.equals(WITHIN_BOUNDING_BOX)) {

                GeoNormalizer geoNormalizer = ((GeoNormalizer) Normalizer.GEO_NORMALIZER);

                // three arguments is the form within_bounding_box(fieldName, lowerLeft, upperRight)
                if (args.size() == 3) {
                    double[] ll = geoNormalizer.parseLatLon(JexlNodes.getIdentifierOrLiteralAsString(args.get(1)));
                    double[] ur = geoNormalizer.parseLatLon(JexlNodes.getIdentifierOrLiteralAsString(args.get(2)));
                    // is the lower left longitude greater than the upper right longitude?
                    // if so, we have crossed the anti-meridian and should split
                    Geometry geom;
                    List<Envelope> envs = new ArrayList<>();
                    if (ll[1] > ur[1]) {
                        Polygon poly1 = createRectangle(ll[1], 180.0, ll[0], ur[0]);
                        Polygon poly2 = createRectangle(-180.0, ur[1], ll[0], ur[0]);
                        geom = createGeometryCollection(poly1, poly2);
                        envs.add(poly1.getEnvelopeInternal());
                        envs.add(poly2.getEnvelopeInternal());
                    } else {
                        geom = createRectangle(ll[1], ur[1], ll[0], ur[0]);
                        envs.add(geom.getEnvelopeInternal());
                    }

                    returnNode = getIndexNode(geom, envs, getFieldNames(args.get(0)), config.getGeoMaxExpansion());
                } else {

                    double minLat, maxLat, minLon, maxLon;

                    try {
                        minLat = GeoNormalizer.parseLatOrLon(JexlNodes.getIdentifierOrLiteralAsString(args.get(3)));
                        maxLat = GeoNormalizer.parseLatOrLon(JexlNodes.getIdentifierOrLiteralAsString(args.get(5)));
                        minLon = GeoNormalizer.parseLatOrLon(JexlNodes.getIdentifierOrLiteralAsString(args.get(2)));
                        maxLon = GeoNormalizer.parseLatOrLon(JexlNodes.getIdentifierOrLiteralAsString(args.get(4)));
                    } catch (ParseException e) {
                        BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY,
                                        "Unable to parse latitude or longitude value");
                        throw new IllegalArgumentException(qe);
                    }

                    // is the min longitude greater than the max longitude?
                    // if so, we have crossed the anti-meridian and should split
                    if (minLon > maxLon) {
                        JexlNode geLonNode1 = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(0), Double.toString(minLon));
                        JexlNode leLonNode1 = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(0), "180");

                        JexlNode geLatNode1 = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(1), Double.toString(minLat));
                        JexlNode leLatNode1 = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(1), Double.toString(maxLat));

                        // now link em up
                        JexlNode andNode1 = JexlNodeFactory.createAndNode(Arrays.asList(
                                        QueryPropertyMarker.create(JexlNodeFactory.createAndNode(Arrays.asList(geLonNode1, leLonNode1)), BOUNDED_RANGE),
                                        QueryPropertyMarker.create(JexlNodeFactory.createAndNode(Arrays.asList(geLatNode1, leLatNode1)), BOUNDED_RANGE)));
                        JexlNode geLonNode2 = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(0), "-180");
                        JexlNode leLonNode2 = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(0), Double.toString(maxLon));

                        JexlNode geLatNode2 = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(1), Double.toString(minLat));
                        JexlNode leLatNode2 = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(1), Double.toString(maxLat));

                        // now link em up
                        JexlNode andNode2 = JexlNodeFactory.createAndNode(Arrays.asList(
                                        QueryPropertyMarker.create(JexlNodeFactory.createAndNode(Arrays.asList(geLonNode2, leLonNode2)), BOUNDED_RANGE),
                                        QueryPropertyMarker.create(JexlNodeFactory.createAndNode(Arrays.asList(geLatNode2, leLatNode2)), BOUNDED_RANGE)));
                        // link em up
                        returnNode = JexlNodeFactory.createOrNode(Arrays.asList(andNode1, andNode2));
                    } else {
                        JexlNode geLonNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(0), Double.toString(minLon));
                        JexlNode leLonNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(0), Double.toString(maxLon));

                        JexlNode geLatNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(1), Double.toString(minLat));
                        JexlNode leLatNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(1), Double.toString(maxLat));

                        // now link em up

                        returnNode = JexlNodeFactory.createAndNode(Arrays.asList(
                                        QueryPropertyMarker.create(JexlNodeFactory.createAndNode(Arrays.asList(geLonNode, leLonNode)), BOUNDED_RANGE),
                                        QueryPropertyMarker.create(JexlNodeFactory.createAndNode(Arrays.asList(geLatNode, leLatNode)), BOUNDED_RANGE)));
                    }
                }
            } else if (name.equals(WITHIN_CIRCLE)) {

                String center = JexlNodes.getIdentifierOrLiteralAsString(args.get(1));
                GeoType gn = new GeoType();

                if (!GeoNormalizer.isNormalized(center)) {
                    try {
                        center = gn.normalize(center);
                    } catch (IllegalArgumentException ne) {
                        BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY,
                                        "Unable to parse lat_lon value: " + center);
                        throw new IllegalArgumentException(qe);
                    }
                }
                GeoPoint c;
                try {
                    c = GeoPoint.decodeZRef(center);
                } catch (OutOfRangeException e) {
                    throw new IllegalArgumentException("Out of range center value " + center, e);
                } catch (ParseException e) {
                    BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY,
                                    "Unparseable range center value " + center);
                    throw new IllegalArgumentException(qe);
                }

                double radius;
                try {
                    radius = GeoNormalizer.parseDouble(JexlNodes.getIdentifierOrLiteralAsString(args.get(2)));
                } catch (ParseException pe) {
                    BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY,
                                    "Unable to parse radius " + JexlNodes.getIdentifierOrLiteral(args.get(2)));
                    throw new IllegalArgumentException(qe);
                }
                double lat = c.getLatitude();
                double lon = c.getLongitude();

                returnNode = getIndexNode(createCircle(lon, lat, radius), getFieldNames(args.get(0)), config.getGeoMaxExpansion());
            }
            return returnNode;
        }

        public static List<String> getFieldNames(JexlNode node) {
            List<String> fieldNames = new ArrayList<>();
            if (node.jjtGetNumChildren() > 1) {
                for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                    JexlNode kid = JexlASTHelper.dereference(node.jjtGetChild(i));
                    if (JexlNodes.getIdentifierOrLiteral(kid) != null) {
                        fieldNames.add(JexlNodes.getIdentifierOrLiteralAsString(kid));
                    }
                }
            } else {
                fieldNames.add(JexlNodes.getIdentifierOrLiteralAsString(node));
            }
            return fieldNames;
        }

        public static JexlNode getIndexNode(Geometry geometry, List<String> fieldNames, int maxExpansion) {
            return getIndexNode(geometry, Collections.singletonList(geometry.getEnvelopeInternal()), fieldNames, maxExpansion);
        }

        public static JexlNode getIndexNode(Geometry geometry, List<Envelope> envs, List<String> fieldNames, int maxExpansion) {
            List<String[]> indexRanges = GeoUtils.generateOptimizedIndexRanges(geometry, envs, maxExpansion);

            List<JexlNode> indexNodes = Lists.newArrayList();
            for (String fieldName : fieldNames) {
                if (fieldName != null) {
                    for (String[] indexRange : indexRanges) {
                        // @formatter:off
                        indexNodes.add(QueryPropertyMarker.create(JexlNodeFactory.createAndNode(Arrays.asList(
                                JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), fieldName, indexRange[0]),
                                JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), fieldName, indexRange[1]))), BOUNDED_RANGE));
                        // @formatter:on
                    }
                }
            }

            JexlNode indexNode;
            if (!indexNodes.isEmpty()) {
                if (indexNodes.size() > 1) {
                    indexNode = JexlNodeFactory.createOrNode(indexNodes);
                } else {
                    indexNode = indexNodes.get(0);
                }
            } else {
                throw new IllegalArgumentException("Unable to create index node for geo function.");
            }

            return indexNode;
        }

        @Override
        public void addFilters(AttributeFactory attributeFactory, Map<String,EventDataQueryExpressionVisitor.ExpressionFilter> filterMap) {
            // noop, covered by getIndexQuery (see comments on interface)
        }

        @Override
        public Set<String> fieldsForNormalization(MetadataHelper helper, Set<String> datatypeFilter, int arg) {
            if (arg > 0) {
                if (name.equals(WITHIN_BOUNDING_BOX)) {
                    if (args.size() == 6) {
                        if (arg == 2 || arg == 4) {
                            return JexlASTHelper.getIdentifierNames(args.get(0));
                        } else {
                            return JexlASTHelper.getIdentifierNames(args.get(1));
                        }
                    } else {
                        return JexlASTHelper.getIdentifierNames(args.get(0));
                    }
                } else if (arg == 1) { // within_circle
                    return JexlASTHelper.getIdentifierNames(args.get(0));
                }
            }
            return Collections.emptySet();
        }

        @Override
        public Set<String> fields(MetadataHelper helper, Set<String> datatypeFilter) {
            if (name.equals(WITHIN_BOUNDING_BOX) && args.size() == 6) {
                Set<String> fields = new HashSet<>();
                fields.addAll(JexlASTHelper.getIdentifierNames(args.get(0)));
                fields.addAll(JexlASTHelper.getIdentifierNames(args.get(1)));
                return fields;
            } else {
                return JexlASTHelper.getIdentifierNames(args.get(0));
            }
        }

        @Override
        public Set<Set<String>> fieldSets(MetadataHelper helper, Set<String> datatypeFilter) {
            if (name.equals(WITHIN_BOUNDING_BOX) && args.size() == 6) {
                // if we have an or node anywhere, then we need to produce a cartesion product
                return JexlArgumentDescriptor.Fields.product(args.get(0), args.get(1));
            } else {
                return JexlArgumentDescriptor.Fields.product(args.get(0));
            }
        }

        @Override
        public boolean useOrForExpansion() {
            return true;
        }

        @Override
        public boolean regexArguments() {
            return false;
        }

        @Override
        public boolean allowIvaratorFiltering() {
            return true;
        }

        public String getWkt() {
            String wkt = null;

            if (name.equals(WITHIN_BOUNDING_BOX)) {
                if (args.size() == 3) {
                    GeoNormalizer geoNormalizer = ((GeoNormalizer) Normalizer.GEO_NORMALIZER);
                    double[] ll = geoNormalizer.parseLatLon(JexlNodes.getIdentifierOrLiteralAsString(args.get(1)));
                    double[] ur = geoNormalizer.parseLatLon(JexlNodes.getIdentifierOrLiteralAsString(args.get(2)));

                    // is the lower left longitude greater than the upper right longitude?
                    // if so, we have crossed the anti-meridian and should split
                    if (ll[1] > ur[1]) {
                        wkt = createGeometryCollection(createRectangle(ll[1], 180.0, ll[0], ur[0]), createRectangle(-180.0, ur[1], ll[0], ur[0])).toText();
                    } else {
                        wkt = createRectangle(ll[1], ur[1], ll[0], ur[0]).toText();
                    }
                } else if (args.size() == 6) {
                    double minLat, maxLat, minLon, maxLon;

                    try {
                        minLat = GeoNormalizer.parseLatOrLon(JexlNodes.getIdentifierOrLiteralAsString(args.get(3)));
                        maxLat = GeoNormalizer.parseLatOrLon(JexlNodes.getIdentifierOrLiteralAsString(args.get(5)));
                        minLon = GeoNormalizer.parseLatOrLon(JexlNodes.getIdentifierOrLiteralAsString(args.get(2)));
                        maxLon = GeoNormalizer.parseLatOrLon(JexlNodes.getIdentifierOrLiteralAsString(args.get(4)));
                    } catch (ParseException e) {
                        BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY,
                                        "Unable to parse latitude or longitude value");
                        throw new IllegalArgumentException(qe);
                    }

                    // is the lower left longitude greater than the upper right longitude?
                    // if so, we have crossed the anti-meridian and should split
                    if (minLon > maxLon) {
                        wkt = createGeometryCollection(createRectangle(minLon, 180.0, minLat, maxLat), createRectangle(-180.0, maxLon, minLat, maxLat))
                                        .toText();
                    } else {
                        wkt = createRectangle(minLon, maxLon, minLat, maxLat).toText();
                    }
                }
            } else if (name.equals(WITHIN_CIRCLE)) {
                String center = JexlNodes.getIdentifierOrLiteralAsString(args.get(1));

                try {
                    GeoPoint c = GeoPoint.decodeZRef(new GeoType().normalize(center));
                    double radius = GeoNormalizer.parseDouble(JexlNodes.getIdentifierOrLiteralAsString(args.get(2)));

                    wkt = createCircle(c.getLongitude(), c.getLatitude(), radius).toText();
                } catch (IllegalArgumentException | OutOfRangeException | ParseException e) {
                    BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY,
                                    "Encountered an error while parsing Geo function");
                    throw new IllegalArgumentException(qe);
                }
            }

            return wkt;
        }

        @Override
        public JexlNode rebuildNode(ShardQueryConfiguration settings, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper,
                        Set<String> datatypeFilter, ASTFunctionNode node) {

            try {
                // 3 args if this is geo:intersects_bounding_box(latLonField, lowerLeft, upperRight) or geo:within_circle(latLonField, center, radius)
                if (args.size() == 3) {

                    Set<String> geoWaveFields = new HashSet<>();
                    Set<String> otherFields = new HashSet<>();

                    // split the fields into geowave and other fields
                    Set<String> fields = fields(metadataHelper, datatypeFilter);
                    for (String field : fields) {
                        if (isAbstractGeometryType(field, metadataHelper))
                            geoWaveFields.add(field);
                        else
                            otherFields.add(field);
                    }

                    // if there are geowave fields, create a geowave function
                    if (!geoWaveFields.isEmpty()) {
                        JexlNode geoWaveNode = toGeoWaveFunction(geoWaveFields);

                        if (geoWaveNode != null) {

                            String geoFields = getFieldParam(otherFields);
                            String arg1 = "'" + JexlNodes.getIdentifierOrLiteral(args.get(1)) + "'";

                            // handle the case where the second argument may be a numeric literal (pt/radius query)
                            String arg2 = (args.get(2) instanceof ASTNumberLiteral) ? JexlNodes.getIdentifierOrLiteralAsString(args.get(2))
                                            : "'" + JexlNodes.getIdentifierOrLiteral(args.get(2)) + "'";

                            // if there are other fields, recreate the geo function node
                            if (!otherFields.isEmpty()) {
                                // dereferencing the child node since we do not want the JexlASTScript nor the JexlASTReference parent nodes
                                JexlNode geoNode = JexlASTHelper.dereference(JexlASTHelper
                                                .parseJexlQuery(namespace + ":" + name + "(" + geoFields + ", " + arg1 + ", " + arg2 + ")").jjtGetChild(0));
                                return JexlNodeFactory.createOrNode(Arrays.asList(geoNode, geoWaveNode));
                            } else {
                                return geoWaveNode;
                            }
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("Unable to rebuild GeoFunctionsDescriptor", e);
            }

            return node;
        }

        private boolean isAbstractGeometryType(String field, MetadataHelper helper) {
            try {
                Set<Type<?>> dataTypes = helper.getDatatypesForField(field);
                return !dataTypes.isEmpty() && dataTypes.stream().allMatch(type -> type instanceof AbstractGeometryType);
            } catch (IllegalAccessException | InstantiationException | TableNotFoundException e) {
                return false;
            }
        }

        public JexlNode toGeoWaveFunction(Set<String> fields) throws Exception {
            String wkt = null;

            // only allow conversion to geowave function for the indexed geo types, not the individual lat/lon fields
            if ((name.equals(WITHIN_BOUNDING_BOX) && args.size() == 3) || name.equals(WITHIN_CIRCLE)) {
                wkt = getWkt();
            }

            if (wkt != null) {
                // dereferencing the child node since we do not want the JexlASTScript nor the JexlASTReference parent nodes
                return JexlASTHelper
                                .dereference(JexlASTHelper.parseJexlQuery("geowave:intersects(" + getFieldParam(fields) + ", '" + wkt + "')").jjtGetChild(0));
            }

            return null;
        }

        private String getFieldParam(Set<String> fields) {
            String fieldParam = String.join(" || ", fields);
            return (fields.size() > 1) ? "(" + fieldParam + ")" : fieldParam;
        }

        private Polygon createCircle(double lon, double lat, double radius) {
            GeometricShapeFactory shapeFactory = new GeometricShapeFactory();
            shapeFactory.setNumPoints(NUM_CIRCLE_POINTS);
            shapeFactory.setCentre(new Coordinate(lon, lat));
            shapeFactory.setSize(radius * 2);
            return shapeFactory.createCircle();
        }

        private Polygon createRectangle(double minLon, double maxLon, double minLat, double maxLat) {
            GeometryFactory geomFactory = new GeometryFactory();
            List<Coordinate> coordinates = new ArrayList<>();
            coordinates.add(new CoordinateXY(minLon, minLat));
            coordinates.add(new CoordinateXY(maxLon, minLat));
            coordinates.add(new CoordinateXY(maxLon, maxLat));
            coordinates.add(new CoordinateXY(minLon, maxLat));
            coordinates.add(new CoordinateXY(minLon, minLat));
            return geomFactory.createPolygon(coordinates.toArray(new Coordinate[0]));
        }

        private Geometry createGeometryCollection(Polygon poly1, Polygon poly2) {
            GeometryFactory geomFactory = new GeometryFactory();
            return geomFactory.createGeometryCollection(new Geometry[] {poly1, poly2});
        }
    }

    @Override
    public JexlArgumentDescriptor getArgumentDescriptor(ASTFunctionNode node) {
        FunctionJexlNodeVisitor fvis = new FunctionJexlNodeVisitor();
        fvis.visit(node, null);

        Class<?> functionClass = (Class<?>) ArithmeticJexlEngines.functions().get(fvis.namespace());

        if (!GeoFunctions.GEO_FUNCTION_NAMESPACE.equals(fvis.namespace())) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.JEXLNODEDESCRIPTOR_NAMESPACE_UNEXPECTED,
                            "Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with an unexpected namespace of " + fvis.namespace());
            throw new IllegalArgumentException(qe);
        }
        if (!functionClass.equals(GeoFunctions.class)) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.JEXLNODEDESCRIPTOR_NODE_FOR_FUNCTION,
                            "Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with node for a function in " + functionClass);
            throw new IllegalArgumentException(qe);
        }
        verify(fvis.name(), fvis.args().size());

        return new GeoJexlArgumentDescriptor(node, fvis.namespace(), fvis.name(), fvis.args());
    }

    private static void verify(String name, int numArgs) {
        if (name.equals(WITHIN_BOUNDING_BOX)) {
            // three arguments is the form within_bounding_box(fieldName, lowerLeft, upperRight)
            if (numArgs != 3 && numArgs != 6) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.WRONG_NUMBER_OF_ARGUMENTS,
                                "Wrong number of arguments to within_bounding_box function");
                throw new IllegalArgumentException(qe);
            }
        } else if (name.equals(WITHIN_CIRCLE)) {
            if (numArgs != 3) {
                BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.WRONG_NUMBER_OF_ARGUMENTS,
                                "Wrong number of arguments to within_circle function");
                throw new IllegalArgumentException(qe);
            }
        } else {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.FUNCTION_NOT_FOUND, "Unknown Geo function: " + name);
            throw new IllegalArgumentException(qe);
        }
    }
}
