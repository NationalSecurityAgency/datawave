package datawave.query.jexl.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.util.GeometricShapeFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import datawave.data.normalizer.GeoNormalizer;
import datawave.data.normalizer.GeoNormalizer.GeoPoint;
import datawave.data.normalizer.GeoNormalizer.OutOfRangeException;
import datawave.data.normalizer.GeoNormalizer.ParseException;
import datawave.data.normalizer.Normalizer;
import datawave.data.type.AbstractGeometryType;
import datawave.data.type.GeoType;
import datawave.data.type.Type;
import datawave.query.Constants;
import datawave.query.attributes.AttributeFactory;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.functions.arguments.RebuildingJexlArgumentDescriptor;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.GeoUtils;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

/**
 * This is the descriptor class for performing geo functions. It supports basic spatial relationships against points.
 */
public class GeoFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory {

    private static final Logger log = Logger.getLogger(GeoFunctionsDescriptor.class);

    private static final int NUM_CIRCLE_POINTS = 60;
    private static final String WITHIN_BOUNDING_BOX = "within_bounding_box";
    private static final String WITHIN_CIRCLE = "within_circle";

    /**
     * This is the argument descriptor which can be used to normalize and optimize function node queries
     * <p>
     * This rebuilding argument descriptor will ensure that if any of the query fields are GeoWave Geometry types that they are removed from the geo query
     * function and placed into the equivalent GeoWave query function.
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
            Set<String> allFields = null;
            try {
                allFields = (helper != null) ? helper.getAllFields(datatypeFilter) : null;
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_TABLE_FETCH_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            }

            if (name.equals(WITHIN_BOUNDING_BOX)) {
                GeoNormalizer geoNormalizer = ((GeoNormalizer) Normalizer.GEO_NORMALIZER);

                // three arguments is the form within_bounding_box(fieldName, lowerLeft, upperRight)
                if (args.size() == 3) {
                    double[] ll = geoNormalizer.parseLatLon(args.get(1).image);
                    double[] ur = geoNormalizer.parseLatLon(args.get(2).image);

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

                    returnNode = getIndexNode(geom, envs, getFieldNames(args.get(0), allFields), config.getGeoMaxExpansion());
                } else {

                    double minLat, maxLat, minLon, maxLon;

                    try {
                        minLat = GeoNormalizer.parseLatOrLon(args.get(3).image);
                        maxLat = GeoNormalizer.parseLatOrLon(args.get(5).image);
                        minLon = GeoNormalizer.parseLatOrLon(args.get(2).image);
                        maxLon = GeoNormalizer.parseLatOrLon(args.get(4).image);
                    } catch (ParseException e) {
                        throw new IllegalArgumentException(e);
                    }

                    // is the min longitude greater than the max longitude?
                    // if so, we have crossed the anti-meridian and should split
                    if (minLon > maxLon) {
                        JexlNode geLonNode1 = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(0), Double.toString(minLon));
                        JexlNode leLonNode1 = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(0), "180");

                        JexlNode geLatNode1 = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(1), Double.toString(minLat));
                        JexlNode leLatNode1 = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(1), Double.toString(maxLat));

                        // now link em up
                        JexlNode andNode1 = JexlNodeFactory
                                        .createAndNode(Arrays.asList(BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geLonNode1, leLonNode1))),
                                                        BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geLatNode1, leLatNode1)))));

                        JexlNode geLonNode2 = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(0), "-180");
                        JexlNode leLonNode2 = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(0), Double.toString(maxLon));

                        JexlNode geLatNode2 = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(1), Double.toString(minLat));
                        JexlNode leLatNode2 = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(1), Double.toString(maxLat));

                        // now link em up
                        JexlNode andNode2 = JexlNodeFactory
                                        .createAndNode(Arrays.asList(BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geLonNode2, leLonNode2))),
                                                        BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geLatNode2, leLatNode2)))));

                        // link em up
                        returnNode = JexlNodeFactory.createOrNode(Arrays.asList(andNode1, andNode2));
                    } else {
                        JexlNode geLonNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(0), Double.toString(minLon));
                        JexlNode leLonNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(0), Double.toString(maxLon));

                        JexlNode geLatNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(1), Double.toString(minLat));
                        JexlNode leLatNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(1), Double.toString(maxLat));

                        // now link em up

                        returnNode = JexlNodeFactory
                                        .createAndNode(Arrays.asList(BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geLonNode, leLonNode))),
                                                        BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geLatNode, leLatNode)))));
                    }
                }
            } else if (name.equals(WITHIN_CIRCLE)) {
                String center = args.get(1).image;
                GeoType gn = new GeoType();

                if (!GeoNormalizer.isNormalized(center)) {
                    try {
                        center = gn.normalize(center);
                    } catch (IllegalArgumentException ne) {
                        throw new IllegalArgumentException("Unable to parse lat_lon value: " + center);
                    }
                }
                GeoPoint c;
                try {
                    c = GeoPoint.decodeZRef(center);
                } catch (OutOfRangeException e) {
                    throw new IllegalArgumentException("Out of range center value " + center, e);
                } catch (ParseException e) {
                    throw new IllegalArgumentException("Unparseable range center value " + center, e);
                }

                double radius;
                try {
                    radius = GeoNormalizer.parseDouble(args.get(2).image);
                } catch (ParseException pe) {
                    throw new IllegalArgumentException("Unable to parse radius " + args.get(2).image, pe);
                }
                double lat = c.getLatitude();
                double lon = c.getLongitude();

                returnNode = getIndexNode(createCircle(lon, lat, radius), getFieldNames(args.get(0), allFields), config.getGeoMaxExpansion());
            }
            return returnNode;
        }

        public static List<String> getFieldNames(JexlNode node, Set<String> allFields) {
            List<String> fieldNames = new ArrayList<>();
            if (node.jjtGetNumChildren() > 1) {
                for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                    JexlNode kid = JexlASTHelper.dereference(node.jjtGetChild(i));
                    if (kid.image != null) {
                        fieldNames.add(kid.image);
                    }
                }
            } else {
                fieldNames.add(node.image);
            }

            if (allFields == null || allFields.contains(Constants.ANY_FIELD)) {
                return fieldNames;
            } else {
                return fieldNames.stream().distinct().filter(allFields::contains).collect(Collectors.toList());
            }
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
                        indexNodes.add(BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(
                                JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), fieldName, indexRange[0]),
                                JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), fieldName, indexRange[1])))));
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
            try {
                Set<String> allFields = helper.getAllFields(datatypeFilter);
                if (arg > 0) {
                    if (name.equals(WITHIN_BOUNDING_BOX)) {
                        if (args.size() == 6) {
                            if (arg == 2 || arg == 4) {
                                return filterSet(allFields, JexlASTHelper.getIdentifierNames(args.get(0)));
                            } else {
                                return filterSet(allFields, JexlASTHelper.getIdentifierNames(args.get(1)));
                            }
                        } else {
                            return filterSet(allFields, JexlASTHelper.getIdentifierNames(args.get(0)));
                        }
                    } else if (arg == 1) { // within_circle
                        return filterSet(allFields, JexlASTHelper.getIdentifierNames(args.get(0)));
                    }
                }
                return Collections.emptySet();
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_TABLE_FETCH_ERROR, e);
                log.error(qe);
                throw new DatawaveFatalQueryException(qe);
            }
        }

        @Override
        public Set<String> fields(MetadataHelper helper, Set<String> datatypeFilter) {
            try {
                Set<String> allFields = (helper != null) ? helper.getAllFields(datatypeFilter) : null;
                if (name.equals(WITHIN_BOUNDING_BOX) && args.size() == 6) {
                    Set<String> fields = new HashSet<>();
                    if (datatypeFilter != null) {
                        fields.addAll(filterSet(allFields, JexlASTHelper.getIdentifierNames(args.get(0))));
                        fields.addAll(filterSet(allFields, JexlASTHelper.getIdentifierNames(args.get(1))));
                    } else {
                        fields.addAll(JexlASTHelper.getIdentifierNames(args.get(0)));
                        fields.addAll(JexlASTHelper.getIdentifierNames(args.get(1)));
                    }
                    return fields;
                } else {
                    return datatypeFilter != null ? filterSet(allFields, JexlASTHelper.getIdentifierNames(args.get(0)))
                                    : JexlASTHelper.getIdentifierNames(args.get(0));
                }
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_TABLE_FETCH_ERROR, e);
                log.error(qe);
                throw new DatawaveFatalQueryException(qe);
            }
        }

        @Override
        public Set<Set<String>> fieldSets(MetadataHelper helper, Set<String> datatypeFilter) {
            try {
                Set<String> allFields = helper.getAllFields(datatypeFilter);
                Set<Set<String>> filteredSets = Sets.newHashSet(Sets.newHashSet());
                if (name.equals(WITHIN_BOUNDING_BOX) && args.size() == 6) {
                    // if we have an or node anywhere, then we need to produce a cartesion product
                    for (Set<String> aFieldSet : JexlArgumentDescriptor.Fields.product(args.get(0), args.get(1))) {
                        filteredSets.add(filterSet(allFields, aFieldSet));
                    }
                } else {
                    for (Set<String> aFieldSet : JexlArgumentDescriptor.Fields.product(args.get(0))) {
                        filteredSets.add(filterSet(allFields, aFieldSet));
                    }
                }
                return filteredSets;
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_TABLE_FETCH_ERROR, e);
                log.error(qe);
                throw new DatawaveFatalQueryException(qe);
            }
        }

        /**
         * Given a list of all possible fields, filters out fields based on the given datatype(s). If allFields is null, we assume this is a no-op and return
         * everything
         *
         * @param allFields
         * @param fields
         */
        private Set<String> filterSet(Set<String> allFields, Set<String> fields) {
            if (allFields != null) {
                Set<String> returnedFields = Sets.newHashSet();
                returnedFields.addAll(allFields);
                returnedFields.retainAll(fields);
                return returnedFields;
            } else {
                return fields;
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
                    double[] ll = geoNormalizer.parseLatLon(args.get(1).image);
                    double[] ur = geoNormalizer.parseLatLon(args.get(2).image);

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
                        minLat = GeoNormalizer.parseLatOrLon(args.get(3).image);
                        maxLat = GeoNormalizer.parseLatOrLon(args.get(5).image);
                        minLon = GeoNormalizer.parseLatOrLon(args.get(2).image);
                        maxLon = GeoNormalizer.parseLatOrLon(args.get(4).image);
                    } catch (ParseException e) {
                        throw new IllegalArgumentException(e);
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
                String center = args.get(1).image;

                try {
                    GeoPoint c = GeoPoint.decodeZRef(new GeoType().normalize(center));
                    double radius = GeoNormalizer.parseDouble(args.get(2).image);

                    wkt = createCircle(c.getLongitude(), c.getLatitude(), radius).toText();
                } catch (IllegalArgumentException | OutOfRangeException | ParseException e) {
                    log.warn("Encountered an error while parsing Geo function");
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
                            String arg1 = "'" + args.get(1).image + "'";

                            // handle the case where the second argument may be a numeric literal (pt/radius query)
                            String arg2 = (args.get(2) instanceof ASTNumberLiteral) ? args.get(2).image : "'" + args.get(2).image + "'";

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

        if (!GeoFunctions.GEO_FUNCTION_NAMESPACE.equals(node.jjtGetChild(0).image))
            throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with an unexpected namespace of "
                            + node.jjtGetChild(0).image);
        if (!functionClass.equals(GeoFunctions.class))
            throw new IllegalArgumentException(
                            "Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with node for a function in " + functionClass);

        verify(fvis.name(), fvis.args().size());

        return new GeoJexlArgumentDescriptor(node, fvis.namespace(), fvis.name(), fvis.args());
    }

    private static void verify(String name, int numArgs) {
        if (name.equals(WITHIN_BOUNDING_BOX)) {
            // three arguments is the form within_bounding_box(fieldName, lowerLeft, upperRight)
            if (numArgs != 3 && numArgs != 6) {
                throw new IllegalArgumentException("Wrong number of arguments to within_bounding_box function");
            }
        } else if (name.equals(WITHIN_CIRCLE)) {
            if (numArgs != 3) {
                throw new IllegalArgumentException("Wrong number of arguments to within_circle function");
            }
        } else {
            throw new IllegalArgumentException("Unknown Geo function: " + name);
        }
    }
}
