package datawave.query.jexl.functions;

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
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.util.GeometricShapeFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is the descriptor class for performing geo functions. It supports basic spatial relationships against points.
 *
 */
public class GeoFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory {
    
    private static final Logger log = Logger.getLogger(GeoFunctionsDescriptor.class);
    
    private static final int NUM_CIRCLE_POINTS = 60;
    
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
            
            if (name.equals("within_bounding_box")) {
                
                GeoNormalizer geoNormalizer = ((GeoNormalizer) Normalizer.GEO_NORMALIZER);
                
                // three arguments is the form within_bounding_box(fieldName, lowerLeft, upperRight)
                if (args.size() == 3) {
                    double[] ll = geoNormalizer.parseLatLon(args.get(1).image);
                    double[] ur = geoNormalizer.parseLatLon(args.get(2).image);
                    
                    // is the lower left longitude greater than the upper right longitude?
                    // if so, we have crossed the anti-meridian and should split
                    if (ll[1] > ur[1]) {
                        
                        char splitChar = args.get(1).image.charAt(geoNormalizer.findSplit(args.get(1).image));
                        
                        JexlNode geNode1 = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(0), Double.toString(ll[0])
                                        + splitChar + Double.toString(ll[1]));
                        JexlNode leNode1 = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(0), Double.toString(ur[0])
                                        + splitChar + "180");
                        
                        // now link em up
                        JexlNode andNode1 = BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geNode1, leNode1)));
                        
                        JexlNode geNode2 = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(0), Double.toString(ll[0])
                                        + splitChar + "-180");
                        JexlNode leNode2 = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(0), Double.toString(ur[0])
                                        + splitChar + Double.toString(ur[1]));
                        
                        // now link em up
                        JexlNode andNode2 = BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geNode2, leNode2)));
                        
                        // link em all up
                        returnNode = JexlNodeFactory.createAndNode(Arrays.asList(andNode1, andNode2));
                        
                    } else {
                        
                        JexlNode geNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(0), args.get(1).image);
                        JexlNode leNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(0), args.get(2).image);
                        
                        // now link em up
                        returnNode = BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geNode, leNode)));
                    }
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
                        JexlNode andNode1 = JexlNodeFactory.createAndNode(Arrays.asList(
                                        BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geLonNode1, leLonNode1))),
                                        BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geLatNode1, leLatNode1)))));
                        
                        JexlNode geLonNode2 = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(0), "-180");
                        JexlNode leLonNode2 = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(0), Double.toString(maxLon));
                        
                        JexlNode geLatNode2 = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(1), Double.toString(minLat));
                        JexlNode leLatNode2 = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(1), Double.toString(maxLat));
                        
                        // now link em up
                        JexlNode andNode2 = JexlNodeFactory.createAndNode(Arrays.asList(
                                        BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geLonNode2, leLonNode2))),
                                        BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geLatNode2, leLatNode2)))));
                        
                        // link em up
                        returnNode = JexlNodeFactory.createAndNode(Arrays.asList(andNode1, andNode2));
                    } else {
                        JexlNode geLonNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(0), Double.toString(minLon));
                        JexlNode leLonNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(0), Double.toString(maxLon));
                        
                        JexlNode geLatNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(1), Double.toString(minLat));
                        JexlNode leLatNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(1), Double.toString(maxLat));
                        
                        // now link em up
                        
                        returnNode = JexlNodeFactory.createAndNode(Arrays.asList(
                                        BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geLonNode, leLonNode))),
                                        BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geLatNode, leLatNode)))));
                    }
                }
            } else if (name.equals("within_circle")) {
                
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
                
                String sep = GeoNormalizer.separator;
                JexlNode leNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), args.get(0), String.valueOf(lat + radius) + sep
                                + String.valueOf(lon + radius));
                JexlNode geNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), args.get(0), String.valueOf(lat - radius) + sep
                                + String.valueOf(lon - radius));
                
                // now link em up
                
                returnNode = BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geNode, leNode)));
            }
            return returnNode;
        }
        
        @Override
        public void addFilters(AttributeFactory attributeFactory, Map<String,EventDataQueryExpressionVisitor.ExpressionFilter> filterMap) {
            // noop, covered by getIndexQuery (see comments on interface)
        }
        
        @Override
        public Set<String> fieldsForNormalization(MetadataHelper helper, Set<String> datatypeFilter, int arg) {
            if (arg > 0) {
                if (name.equals("within_bounding_box")) {
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
            if (name.equals("within_bounding_box") && args.size() == 6) {
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
            if (name.equals("within_bounding_box") && args.size() == 6) {
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
                                JexlNode geoNode = JexlASTHelper.dereference(JexlASTHelper.parseJexlQuery(
                                                namespace + ":" + name + "(" + geoFields + ", " + arg1 + ", " + arg2 + ")").jjtGetChild(0));
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
            
            if (name.equals("within_bounding_box")) {
                GeoNormalizer geoNormalizer = ((GeoNormalizer) Normalizer.GEO_NORMALIZER);
                double[] ll = geoNormalizer.parseLatLon(args.get(1).image);
                double[] ur = geoNormalizer.parseLatLon(args.get(2).image);
                
                // is the lower left longitude greater than the upper right longitude?
                // if so, we have crossed the anti-meridian and should split
                if (ll[1] > ur[1]) {
                    wkt = createMultiPolygon(createRectangle(ll[1], 180.0, ll[0], ur[0]), createRectangle(-180.0, ur[1], ll[0], ur[0])).toText();
                } else {
                    wkt = createRectangle(ll[1], ur[1], ll[0], ur[0]).toText();
                }
            } else if (name.equals("within_circle")) {
                String center = args.get(1).image;
                
                try {
                    GeoPoint c = GeoPoint.decodeZRef(new GeoType().normalize(center));
                    double radius = GeoNormalizer.parseDouble(args.get(2).image);
                    
                    wkt = createCircle(c.getLongitude(), c.getLatitude(), radius).toText();
                } catch (IllegalArgumentException | OutOfRangeException | ParseException e) {
                    log.warn("Encountered an error while parsing Geo function");
                }
            }
            
            if (wkt != null) {
                // dereferencing the child node since we do not want the JexlASTScript nor the JexlASTReference parent nodes
                return JexlASTHelper.dereference(JexlASTHelper.parseJexlQuery("geowave:intersects(" + getFieldParam(fields) + ", '" + wkt + "')")
                                .jjtGetChild(0));
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
            GeometricShapeFactory shapeFactory = new GeometricShapeFactory();
            shapeFactory.setEnvelope(new Envelope(minLon, maxLon, minLat, maxLat));
            shapeFactory.setNumPoints(4);
            return shapeFactory.createRectangle();
        }
        
        private MultiPolygon createMultiPolygon(Polygon poly1, Polygon poly2) {
            GeometryFactory geomFactory = new GeometryFactory();
            return geomFactory.createMultiPolygon(new Polygon[] {poly1, poly2});
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
            throw new IllegalArgumentException("Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with node for a function in "
                            + functionClass);
        
        verify(fvis.name(), fvis.args().size());
        
        return new GeoJexlArgumentDescriptor(node, fvis.namespace(), fvis.name(), fvis.args());
    }
    
    private static void verify(String name, int numArgs) {
        if (name.equals("within_bounding_box")) {
            // three arguments is the form within_bounding_box(fieldName, lowerLeft, upperRight)
            if (numArgs != 3 && numArgs != 6) {
                throw new IllegalArgumentException("Wrong number of arguments to within_bounding_box function");
            }
        } else if (name.equals("within_circle")) {
            if (numArgs != 3) {
                throw new IllegalArgumentException("Wrong number of arguments to within_circle function");
            }
        } else {
            throw new IllegalArgumentException("Unknown Geo function: " + name);
        }
    }
}
