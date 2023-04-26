package datawave.query.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.normalizer.GeometryNormalizer;
import datawave.data.type.AbstractGeometryType;
import datawave.data.type.GeoType;
import datawave.data.type.Type;
import datawave.marking.MarkingFunctions;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.GeoWaveFunctionsDescriptor;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.util.GeoUtils;
import datawave.query.util.GeoWaveUtils;
import datawave.query.util.MetadataHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;
import org.locationtech.jts.geom.Geometry;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.commons.jexl2.parser.JexlNodes.children;

/**
 * This visitor should be run after bounded ranges have been expanded in order to check for expanded GeoWave terms which do not intersect with the original
 * query geometry. This is a possibility as the GeoWave ranges start out being overly inclusive in order to minimize the number of ranges needed to run a geo
 * query.
 */
public class GeoWavePruningVisitor extends RebuildingVisitor {

    private static final Logger log = ThreadConfigurableLogger.getLogger(GeoWavePruningVisitor.class);

    private final Multimap<String, String> prunedTerms;
    private final MetadataHelper metadataHelper;

    private GeoWavePruningVisitor(Multimap<String, String> prunedTerms, MetadataHelper metadataHelper) {
        this.prunedTerms = prunedTerms;
        this.metadataHelper = metadataHelper;
    }

    public static <T extends JexlNode> T pruneTree(JexlNode node) {
        return pruneTree(node, null, null);
    }

    public static <T extends JexlNode> T pruneTree(JexlNode node, Multimap<String, String> prunedTerms, MetadataHelper metadataHelper) {
        GeoWavePruningVisitor pruningVisitor = new GeoWavePruningVisitor(prunedTerms, metadataHelper);
        return (T) node.jjtAccept(pruningVisitor, null);
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        Multimap<String, Geometry> fieldToGeometryMap = (data instanceof Multimap) ? (Multimap<String, Geometry>) data : HashMultimap.create();

        // if one of the anded nodes is a geowave function, pass down the geometry and field name in the multimap
        for (JexlNode child : children(node)) {
            child = JexlASTHelper.dereference(child);
            if (child instanceof ASTFunctionNode) {
                JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor((ASTFunctionNode) child);
                if (desc instanceof GeoWaveFunctionsDescriptor.GeoWaveJexlArgumentDescriptor) {
                    GeoWaveFunctionsDescriptor.GeoWaveJexlArgumentDescriptor geoWaveDesc = (GeoWaveFunctionsDescriptor.GeoWaveJexlArgumentDescriptor) desc;
                    try {
                        if (isPrunable(geoWaveDesc)) {
                            Geometry geom = GeometryNormalizer.parseGeometry(geoWaveDesc.getWkt());
                            Set<String> fields = geoWaveDesc.fields(metadataHelper, null);
                            if (fields != null) {
                                for (String field : fields) {
                                    fieldToGeometryMap.put(field, geom);
                                }
                            }
                        }
                    } catch (TableNotFoundException e) {
                        QueryException qe = new QueryException(DatawaveErrorCode.METADATA_TABLE_FETCH_ERROR, e);
                        log.error(qe);
                        throw new DatawaveFatalQueryException(qe);
                    } catch (ExecutionException | MarkingFunctions.Exception e) {
                        QueryException qe = new QueryException(DatawaveErrorCode.UNKNOWN_SERVER_ERROR, e);
                        log.error(qe);
                        throw new DatawaveFatalQueryException(qe);
                    }
                }
            }
        }
        return super.visit(node, (fieldToGeometryMap.isEmpty() ? null : fieldToGeometryMap));
    }

    private boolean isPrunable(GeoWaveFunctionsDescriptor.GeoWaveJexlArgumentDescriptor geoWaveDesc) throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        Set<String> fields = geoWaveDesc.fields(metadataHelper, null);
        // @formatter:off
        return fields.stream().anyMatch(
                        field -> getDatatypesForField(field).stream().anyMatch(
                                type -> (type instanceof AbstractGeometryType || type instanceof GeoType)));
        // @formatter:on
    }

    private boolean isGeoWaveType(String field) {
        return getDatatypesForField(field).stream().anyMatch(type -> type instanceof AbstractGeometryType);
    }

    private boolean isGeoType(String field) {
        return getDatatypesForField(field).stream().anyMatch(type -> type instanceof GeoType);
    }

    private Set<Type<?>> getDatatypesForField(String field) {
        Set<Type<?>> dataTypes = new HashSet<>();
        try {
            dataTypes.addAll(metadataHelper.getDatatypesForField(field));
        } catch (Exception e) {
            log.warn("Unable to determine types for field: " + field, e);
        }
        return dataTypes;
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        JexlNode copiedNode = (JexlNode) super.visit(node, data);

        // if all of the children were dropped, turn this into a false node
        if (copiedNode.jjtGetNumChildren() == 0) {
            copiedNode = new ASTFalseNode(ParserTreeConstants.JJTFALSENODE);
        }

        return copiedNode;
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        JexlNode rebuiltNode = (JexlNode) super.visit(node, data);
        return (rebuiltNode.jjtGetNumChildren() == 0) ? null : rebuiltNode;
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        JexlNode rebuiltNode = (JexlNode) super.visit(node, data);
        return (rebuiltNode.jjtGetNumChildren() == 0) ? null : rebuiltNode;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        if (data instanceof Multimap) {
            Multimap<String, Geometry> fieldToGeometryMap = (Multimap<String, Geometry>) data;

            String field = JexlASTHelper.getIdentifier(node);

            Collection<Geometry> queryGeometries = fieldToGeometryMap.get(field);

            if (queryGeometries != null && !queryGeometries.isEmpty()) {
                String value = (String) JexlASTHelper.getLiteralValue(node);
                if (value != null) {
                    Geometry nodeGeometry = null;
                    try {
                        if (isGeoWaveType(field)) {
                            nodeGeometry = GeoWaveUtils.positionToGeometry(value);
                        } else if (isGeoType(field)) {
                            nodeGeometry = GeoUtils.indexToGeometry(value);
                        }
                    } catch (Exception e) {
                        log.warn("Unable to extract geometry from geo term: " + value, e);
                    }
                    if (nodeGeometry != null) {
                        // if the node geometry doesn't intersect the query geometry, get rid of this node
                        if (fieldToGeometryMap.get(field).stream().noneMatch(nodeGeometry::intersects)) {
                            if (prunedTerms != null) {
                                prunedTerms.put(field, value);
                            }
                            return new ASTFalseNode(ParserTreeConstants.JJTFALSENODE);
                        }
                    }
                }
            }
        }

        return super.visit(node, data);
    }
}
