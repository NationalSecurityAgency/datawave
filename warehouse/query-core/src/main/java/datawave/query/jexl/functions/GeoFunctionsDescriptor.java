package datawave.query.jexl.functions;

import static datawave.core.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
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

import datawave.core.geo.function.AbstractGeoFunctionDetails;
import datawave.core.geo.function.geo.NumericIndexGeoFunctionDetails;
import datawave.core.geo.function.geo.ZIndexGeoFunctionDetails;
import datawave.core.geo.utils.CommonGeoUtils;
import datawave.core.geo.utils.GeoUtils;
import datawave.core.query.jexl.JexlNodeFactory;
import datawave.core.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.core.query.jexl.nodes.QueryPropertyMarker;
import datawave.core.query.jexl.visitors.JexlStringBuildingVisitor;
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
import datawave.query.jexl.functions.arguments.GeoFunctionJexlArgumentDescriptor;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.functions.arguments.RebuildingJexlArgumentDescriptor;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;

/**
 * This is the descriptor class for performing geo functions. It supports basic spatial relationships against points.
 *
 */
public class GeoFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory {

    private static final Logger log = Logger.getLogger(GeoFunctionsDescriptor.class);

    /**
     * This is the argument descriptor which can be used to normalize and optimize function node queries
     *
     * This rebuilding argument descriptor will ensure that if any of the query fields are GeoWave Geometry types that they are removed from the geo query
     * function and placed into the equivalent GeoWave query function.
     *
     */
    public static class GeoJexlArgumentDescriptor implements RebuildingJexlArgumentDescriptor, GeoFunctionJexlArgumentDescriptor {

        private final String namespace, name;
        private final List<JexlNode> args;
        private final AbstractGeoFunctionDetails geoFunction;

        public GeoJexlArgumentDescriptor(ASTFunctionNode node, String namespace, String name, List<JexlNode> args, AbstractGeoFunctionDetails geoFunction) {
            this.namespace = namespace;
            this.name = name;
            this.args = args;
            this.geoFunction = geoFunction;
        }

        @Override
        public JexlNode getIndexQuery(ShardQueryConfiguration config, MetadataHelper helper, DateIndexHelper dateIndexHelper, Set<String> datatypeFilter) {
            try {
                Map<String,Set<Type<?>>> typesByField = new LinkedHashMap<>();
                for (String field : geoFunction.getFields()) {
                    typesByField.put(field, helper.getDatatypesForField(field, datatypeFilter));
                }

                return geoFunction.generateIndexNode(typesByField, config.getGeoQueryConfig());
            } catch (Exception e) {
                throw new IllegalArgumentException("Unable to create index node for geo function.", e);
            }
        }

        @Override
        public void addFilters(AttributeFactory attributeFactory, Map<String,EventDataQueryExpressionVisitor.ExpressionFilter> filterMap) {
            // noop, covered by getIndexQuery (see comments on interface)
        }

        @Override
        public Set<String> fieldsForNormalization(MetadataHelper helper, Set<String> datatypeFilter, int arg) {
            Set<String> fields = Collections.emptySet();
            if (geoFunction != null) {
                fields = geoFunction.getFields();
            }
            return fields;
        }

        @Override
        public Set<String> fields(MetadataHelper helper, Set<String> datatypeFilter) {
            Set<String> fields = Collections.emptySet();
            if (geoFunction != null) {
                fields = geoFunction.getFields();
            }
            return fields;
        }

        @Override
        public Set<Set<String>> fieldSets(MetadataHelper helper, Set<String> datatypeFilter) {
            if (geoFunction instanceof NumericIndexGeoFunctionDetails) {
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
        public AbstractGeoFunctionDetails getGeoFunction() {
            return geoFunction;
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
            if (geoFunction instanceof ZIndexGeoFunctionDetails) {
                wkt = CommonGeoUtils.geometriesToGeometry(geoFunction.getGeometry()).toText();
            }

            if (wkt != null) {
                // dereferencing the child node since we do not want the JexlASTScript nor the JexlASTReference parent nodes
                return JexlASTHelper.parseJexlQuery("geowave:intersects(" + getFieldParam(fields) + ", '" + wkt + "')").jjtGetChild(0);
            }

            return null;
        }

        private String getFieldParam(Set<String> fields) {
            String fieldParam = String.join(" || ", fields);
            return (fields.size() > 1) ? "(" + fieldParam + ")" : fieldParam;
        }
    }

    @Override
    public JexlArgumentDescriptor getArgumentDescriptor(ASTFunctionNode node) {
        FunctionJexlNodeVisitor fvis = new FunctionJexlNodeVisitor();
        fvis.visit(node, null);

        Class<?> functionClass = (Class<?>) ArithmeticJexlEngines.functions().get(fvis.namespace());

        if (!GeoFunctions.GEO_FUNCTION_NAMESPACE.equals(fvis.namespace()))
            throw new IllegalArgumentException(
                            "Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with an unexpected namespace of " + fvis.namespace());
        if (!functionClass.equals(GeoFunctions.class))
            throw new IllegalArgumentException(
                            "Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with node for a function in " + functionClass);

        AbstractGeoFunctionDetails geoFunction = GeoUtils.parseGeoFunction(fvis.name(), fvis.args());
        if (geoFunction == null) {
            throw new IllegalArgumentException("Unable to parse geo function from " + JexlStringBuildingVisitor.buildQuery(node));
        }

        return new GeoJexlArgumentDescriptor(node, fvis.namespace(), fvis.name(), fvis.args(), geoFunction);
    }
}
