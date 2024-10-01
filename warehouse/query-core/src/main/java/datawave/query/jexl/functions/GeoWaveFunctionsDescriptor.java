package datawave.query.jexl.functions;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.apache.log4j.Logger;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import datawave.data.normalizer.AbstractGeometryNormalizer;
import datawave.data.normalizer.GeometryNormalizer;
import datawave.data.normalizer.PointNormalizer;
import datawave.data.type.GeoType;
import datawave.data.type.GeometryType;
import datawave.data.type.PointType;
import datawave.data.type.Type;
import datawave.query.attributes.AttributeFactory;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.EventDataQueryExpressionVisitor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.GeoWaveUtils;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

/**
 * This is the descriptor class for performing geowave functions. It supports basic spatial relationships, and decomposes the bounding box of the relationship
 * geometry into a set of geowave ranges. It currently caps this range decomposition to 8 ranges per tier for GeometryType and 32 ranges total for PointType.
 *
 */
public class GeoWaveFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory {

    private enum IndexType {
        GEOWAVE_GEOMETRY, GEOWAVE_POINT, GEO_POINT, UNKNOWN
    }

    /**
     * This is the argument descriptor which can be used to normalize and optimize function node queries
     *
     */
    protected static final String[] SPATIAL_RELATION_OPERATIONS = new String[] {"contains", "covers", "covered_by", "crosses", "intersects", "overlaps",
            "within"};

    private static final Logger LOGGER = Logger.getLogger(GeoWaveFunctionsDescriptor.class);

    public static class GeoWaveJexlArgumentDescriptor implements JexlArgumentDescriptor {
        protected final String name;
        protected final List<JexlNode> args;

        public GeoWaveJexlArgumentDescriptor(ASTFunctionNode node, String name, List<JexlNode> args) {
            this.name = name;
            this.args = args;
        }

        @Override
        public JexlNode getIndexQuery(ShardQueryConfiguration config, MetadataHelper helper, DateIndexHelper dateIndexHelper, Set<String> datatypeFilter) {
            int maxEnvelopes = Math.max(1, config.getGeoWaveMaxEnvelopes());
            if (isSpatialRelationship(name)) {
                Geometry geom = AbstractGeometryNormalizer.parseGeometry(JexlNodes.getIdentifierOrLiteralAsString(args.get(1)));
                List<Envelope> envelopes = getSeparateEnvelopes(geom, maxEnvelopes);
                if (!envelopes.isEmpty()) {

                    JexlNode indexNode;
                    if (envelopes.size() == 1) {
                        indexNode = getIndexNode(args.get(0), geom, envelopes.get(0), config, helper);
                    } else {
                        indexNode = getIndexNode(args.get(0), geom, envelopes, config, helper);
                    }

                    return indexNode;
                }
            }
            // return the true node if unable to parse arguments
            return TRUE_NODE;
        }

        private static Set<IndexType> getIndexTypes(String field, MetadataHelper helper) {
            Set<IndexType> dataTypes = new HashSet<>();
            try {
                for (Type<?> type : helper.getDatatypesForField(field)) {
                    dataTypes.add(typeToIndexType(type));
                }
            } catch (IllegalAccessException | InstantiationException | TableNotFoundException e) {
                LOGGER.debug("Unable to retrieve data types for field " + field);
            }
            return dataTypes;
        }

        private static IndexType typeToIndexType(Type<?> type) {
            IndexType indexType = IndexType.UNKNOWN;
            if (type instanceof GeometryType) {
                indexType = IndexType.GEOWAVE_GEOMETRY;
            } else if (type instanceof PointType) {
                indexType = IndexType.GEOWAVE_POINT;
            } else if (type instanceof GeoType) {
                indexType = IndexType.GEO_POINT;
            }
            return indexType;
        }

        protected static JexlNode getIndexNode(JexlNode node, Geometry geometry, Envelope env, ShardQueryConfiguration config, MetadataHelper helper) {
            if (node.jjtGetNumChildren() > 0) {
                List<JexlNode> list = Lists.newArrayList();
                for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                    JexlNode kid = node.jjtGetChild(i);
                    if (JexlNodes.getIdentifierOrLiteralAsString(kid) != null) {
                        list.add(getIndexNode(JexlNodes.getIdentifierOrLiteralAsString(kid), geometry, env, config, helper));
                    }
                }
                if (!list.isEmpty()) {
                    return JexlNodeFactory.createOrNode(list);
                }
            } else if (JexlNodes.getIdentifierOrLiteralAsString(node) != null) {
                return getIndexNode(JexlNodes.getIdentifierOrLiteralAsString(node), geometry, env, config, helper);
            }
            return node;
        }

        protected static JexlNode getIndexNode(JexlNode node, Geometry geometry, List<Envelope> envs, ShardQueryConfiguration config, MetadataHelper helper) {
            if (node.jjtGetNumChildren() > 0) {
                List<JexlNode> list = Lists.newArrayList();
                for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                    JexlNode kid = node.jjtGetChild(i);
                    if (JexlNodes.getIdentifierOrLiteralAsString(kid) != null) {
                        list.add(getIndexNode(JexlNodes.getIdentifierOrLiteralAsString(kid), geometry, envs, config, helper));
                    }
                }
                if (!list.isEmpty()) {
                    return JexlNodeFactory.createOrNode(list);
                }
            } else if (JexlNodes.getIdentifierOrLiteralAsString(node) != null) {
                return getIndexNode(JexlNodes.getIdentifierOrLiteralAsString(node), geometry, envs, config, helper);
            }
            return node;
        }

        protected static JexlNode getIndexNode(String fieldName, Geometry geometry, Envelope env, ShardQueryConfiguration config, MetadataHelper helper) {
            List<Envelope> envs = new ArrayList<>();
            envs.add(env);
            return getIndexNode(fieldName, geometry, envs, config, helper);
        }

        protected static JexlNode getIndexNode(String fieldName, Geometry geometry, List<Envelope> envs, ShardQueryConfiguration config,
                        MetadataHelper helper) {
            List<JexlNode> indexNodes = new ArrayList<>();
            Set<IndexType> indexTypes = getIndexTypes(fieldName, helper);
            // generate ranges for geowave geometries and points
            if (indexTypes.remove(IndexType.GEOWAVE_GEOMETRY)) {
                // point index ranges are covered by geometry index
                // ranges so we can remove GEOWAVE_POINT
                indexTypes.remove(IndexType.GEOWAVE_POINT);

                indexNodes.add(generateGeoWaveRanges(fieldName, geometry, envs, config, GeometryNormalizer.getGeometryIndex(),
                                config.getGeometryMaxExpansion()));
            }
            // generate ranges for geowave points
            else if (indexTypes.remove(IndexType.GEOWAVE_POINT)) {
                indexNodes.add(generateGeoWaveRanges(fieldName, geometry, envs, config, PointNormalizer.getPointIndex(), config.getPointMaxExpansion()));
            }
            // generate ranges for geo points
            else if (indexTypes.remove(IndexType.GEO_POINT)) {
                indexNodes.add(generateGeoRanges(fieldName, geometry, envs, config.getGeoMaxExpansion()));
            }

            JexlNode indexNode;
            if (!indexNodes.isEmpty()) {
                if (indexNodes.size() > 1) {
                    indexNode = JexlNodeFactory.createOrNode(indexNodes);
                } else {
                    indexNode = indexNodes.get(0);
                }
            } else {
                throw new IllegalArgumentException("Unable to create index node for geowave function.");
            }

            return indexNode;
        }

        protected static JexlNode generateGeoWaveRanges(String fieldName, Geometry geometry, List<Envelope> envs, ShardQueryConfiguration config, Index index,
                        int maxExpansion) {
            Collection<ByteArrayRange> allRanges = new ArrayList<>();
            int maxRanges = maxExpansion / envs.size();
            for (Envelope env : envs) {
                for (MultiDimensionalNumericData range : GeometryUtils.basicConstraintsFromEnvelope(env).getIndexConstraints(index)) {
                    List<ByteArrayRange> byteArrayRanges = index.getIndexStrategy().getQueryRanges(range, maxRanges).getCompositeQueryRanges();
                    if (config.isOptimizeGeoWaveRanges()) {
                        byteArrayRanges = GeoWaveUtils.optimizeByteArrayRanges(geometry, byteArrayRanges, config.getGeoWaveRangeSplitThreshold(),
                                        config.getGeoWaveMaxRangeOverlap());
                    }
                    allRanges.addAll(byteArrayRanges);
                }
            }
            allRanges = ByteArrayRange.mergeIntersections(allRanges, ByteArrayRange.MergeOperation.UNION);

            Iterable<JexlNode> rangeNodes = Iterables.transform(allRanges, new ByteArrayRangeToJexlNode(fieldName));

            // now link em up
            return JexlNodeFactory.createOrNode(rangeNodes);
        }

        protected static JexlNode generateGeoRanges(String fieldName, Geometry geometry, List<Envelope> envs, int maxExpansion) {
            JexlNode indexNode;
            List<JexlNode> indexNodes = new ArrayList<>();
            for (Envelope env : envs) {
                // @formatter:off
                indexNodes.add(
                        GeoFunctionsDescriptor.GeoJexlArgumentDescriptor.getIndexNode(
                                geometry,
                                envs,
                                Collections.singletonList(fieldName),
                                maxExpansion));
                // @formatter:on
            }

            if (!indexNodes.isEmpty()) {
                if (indexNodes.size() > 1) {
                    indexNode = JexlNodeFactory.createOrNode(indexNodes);
                } else {
                    indexNode = indexNodes.get(0);
                }
            } else {
                throw new IllegalArgumentException("Failed to generate index nodes for geo field using geowave function.");
            }

            return indexNode;
        }

        @Override
        public void addFilters(AttributeFactory attributeFactory, Map<String,EventDataQueryExpressionVisitor.ExpressionFilter> filterMap) {
            // noop, covered by getIndexQuery (see comments on interface)
        }

        @Override
        public Set<String> fieldsForNormalization(MetadataHelper helper, Set<String> datatypeFilter, int arg) {
            // no normalization required
            return Collections.emptySet();
        }

        @Override
        public Set<String> fields(MetadataHelper helper, Set<String> datatypeFilter) {
            return JexlASTHelper.getIdentifierNames(args.get(0));
        }

        @Override
        public Set<Set<String>> fieldSets(MetadataHelper helper, Set<String> datatypeFilter) {
            return Fields.product(args.get(0));
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
            return false;
        }

        public String getWkt() {
            return JexlNodes.getIdentifierOrLiteralAsString(args.get(1));
        }
    }

    @Override
    public JexlArgumentDescriptor getArgumentDescriptor(ASTFunctionNode node) {
        FunctionJexlNodeVisitor fvis = new FunctionJexlNodeVisitor();
        fvis.visit(node, null);

        Class<?> functionClass = (Class<?>) ArithmeticJexlEngines.functions().get(fvis.namespace());

        if (!GeoWaveFunctions.GEOWAVE_FUNCTION_NAMESPACE.equals(fvis.namespace())) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.JEXLNODEDESCRIPTOR_NAMESPACE_UNEXPECTED,
                            "Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with an unexpected namespace of " + fvis.namespace());
            throw new IllegalArgumentException(qe);
        }
        if (!functionClass.equals(GeoWaveFunctions.class)) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.JEXLNODEDESCRIPTOR_NODE_FOR_FUNCTION,
                            "Calling " + this.getClass().getSimpleName() + ".getJexlNodeDescriptor with node for a function in " + functionClass);
            throw new IllegalArgumentException(qe);
        }
        verify(fvis.name(), fvis.args().size());

        return new GeoWaveJexlArgumentDescriptor(node, fvis.name(), fvis.args());
    }

    protected static void verify(String name, int numArgs) {
        if (isSpatialRelationship(name)) {
            // two arguments in the form <spatial_relation_function>(fieldName,
            // geometryString
            verify(name, numArgs, new String[] {"fieldName", "geometryString"});
        } else {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.FUNCTION_NOT_FOUND, "Unknown GeoWave function: " + name);
            throw new IllegalArgumentException(qe);
        }
    }

    private static boolean isSpatialRelationship(String name) {
        for (String op : SPATIAL_RELATION_OPERATIONS) {
            if (op.equals(name)) {
                return true;
            }
        }
        return false;
    }

    protected static void verify(String name, int numArgs, String[] parameterNames) {
        if (numArgs != parameterNames.length) {
            StringBuilder exception = new StringBuilder("Wrong number of arguments to GeoWave's ");
            exception.append(name).append(" function; correct use is '").append(name).append("(");
            if (parameterNames.length > 0) {
                exception.append(parameterNames[0]);
            }
            for (int i = 1; i < parameterNames.length; i++) {
                exception.append(", ").append(parameterNames[i]);
            }
            exception.append(")'");
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.WRONG_NUMBER_OF_ARGUMENTS, exception.toString());
            throw new IllegalArgumentException(qe);
        }
    }

    private static class ByteArrayRangeToJexlNode implements com.google.common.base.Function<ByteArrayRange,JexlNode> {
        private final String fieldName;

        public ByteArrayRangeToJexlNode(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public JexlNode apply(ByteArrayRange input) {
            if (Arrays.equals(input.getStart(), input.getEnd())) {
                return JexlNodeFactory.buildNode(new ASTEQNode(ParserTreeConstants.JJTEQNODE), fieldName,
                                AbstractGeometryNormalizer.getEncodedStringFromIndexBytes(input.getStart()));
            } else {
                JexlNode geNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), fieldName,
                                AbstractGeometryNormalizer.getEncodedStringFromIndexBytes(input.getStart()));
                JexlNode leNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), fieldName,
                                AbstractGeometryNormalizer.getEncodedStringFromIndexBytes(input.getEnd()));
                // now link em up
                return QueryPropertyMarker.create(JexlNodeFactory.createAndNode(Arrays.asList(geNode, leNode)), BOUNDED_RANGE);
            }
        }

    }

    protected static List<Geometry> getAllEnvelopeGeometries(Geometry geom) {
        List<Geometry> geometries = new ArrayList<>();
        if (geom.getNumGeometries() > 1)
            for (int geoIdx = 0; geoIdx < geom.getNumGeometries(); geoIdx++)
                geometries.addAll(getAllEnvelopeGeometries(geom.getGeometryN(geoIdx)));
        else
            geometries.add(geom.getEnvelope());
        return geometries;
    }

    protected static boolean combineIntersectedGeometries(List<Geometry> geometries, List<Geometry> combinedGeometries, int maxEnvelopes) {
        while (!geometries.isEmpty() && combinedGeometries.size() < maxEnvelopes)
            combinedGeometries.add(findIntersectedGeoms(geometries.remove(0), geometries));
        return geometries.isEmpty();
    }

    protected static Geometry findIntersectedGeoms(Geometry srcGeom, List<Geometry> geometries) {
        List<Geometry> intersected = new ArrayList<>();

        // find all geometries which intersect with with srcGeom
        Iterator<Geometry> geomIter = geometries.iterator();
        while (geomIter.hasNext()) {
            Geometry geom = geomIter.next();
            if (geom.intersects(srcGeom)) {
                geomIter.remove();
                intersected.add(geom);
            }
        }

        // compute the envelope for the intersected geometries and the source geometry, and look for more intersections
        if (!intersected.isEmpty()) {
            intersected.add(srcGeom);
            Geometry mergedGeom = new GeometryCollection(intersected.toArray(new Geometry[0]), new GeometryFactory()).getEnvelope();
            return findIntersectedGeoms(mergedGeom, geometries);
        }

        return srcGeom;
    }

    protected static List<Envelope> getSeparateEnvelopes(Geometry geom, int maxEnvelopes) {
        if (geom.getNumGeometries() > 0) {
            List<Geometry> geometries = getAllEnvelopeGeometries(geom);
            List<Geometry> intersectedGeometries = new ArrayList<>();

            if (combineIntersectedGeometries(geometries, intersectedGeometries, maxEnvelopes))
                return intersectedGeometries.stream().map(Geometry::getEnvelopeInternal).collect(Collectors.toList());
        }
        return Collections.singletonList(geom.getEnvelopeInternal());
    }

}
