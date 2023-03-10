package datawave.query.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.normalizer.GeometryNormalizer;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.GeoWaveFunctionsDescriptor;
import datawave.query.jexl.functions.JexlFunctionArgumentDescriptorFactory;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.util.GeoWaveUtils;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.locationtech.jts.geom.Geometry;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static org.apache.commons.jexl2.parser.JexlNodes.children;

/**
 * This visitor should be run after bounded ranges have been expanded in order to check for expanded GeoWave terms which do not intersect with the original
 * query geometry. This is a possibility as the GeoWave ranges start out being overly inclusive in order to minimize the number of ranges needed to run a geo
 * query.
 */
public class GeoWavePruningVisitor extends RebuildingVisitor {
    
    private final Multimap<String,String> prunedTerms;
    private final MetadataHelper metadataHelper;
    
    private GeoWavePruningVisitor(Multimap<String,String> prunedTerms, MetadataHelper metadataHelper) {
        this.prunedTerms = prunedTerms;
        this.metadataHelper = metadataHelper;
    }
    
    public static <T extends JexlNode> T pruneTree(JexlNode node) {
        return pruneTree(node, null, null);
    }
    
    public static <T extends JexlNode> T pruneTree(JexlNode node, Multimap<String,String> prunedTerms, MetadataHelper metadataHelper) {
        GeoWavePruningVisitor pruningVisitor = new GeoWavePruningVisitor(prunedTerms, metadataHelper);
        return (T) node.jjtAccept(pruningVisitor, null);
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        Multimap<String,Geometry> fieldToGeometryMap = (data instanceof Multimap) ? (Multimap<String,Geometry>) data : HashMultimap.create();
        
        // if one of the anded nodes is a geowave function, pass down the geometry and field name in the multimap
        for (JexlNode child : children(node)) {
            child = JexlASTHelper.dereference(child);
            if (child instanceof ASTFunctionNode) {
                JexlArgumentDescriptor desc = JexlFunctionArgumentDescriptorFactory.F.getArgumentDescriptor((ASTFunctionNode) child);
                if (desc instanceof GeoWaveFunctionsDescriptor.GeoWaveJexlArgumentDescriptor) {
                    GeoWaveFunctionsDescriptor.GeoWaveJexlArgumentDescriptor geoWaveDesc = (GeoWaveFunctionsDescriptor.GeoWaveJexlArgumentDescriptor) desc;
                    Geometry geom = GeometryNormalizer.parseGeometry(geoWaveDesc.getWkt());
                    Set<String> fields = geoWaveDesc.fields(metadataHelper, Collections.emptySet());
                    for (String field : fields) {
                        fieldToGeometryMap.put(field, geom);
                    }
                }
            }
        }
        return super.visit(node, (fieldToGeometryMap.isEmpty() ? null : fieldToGeometryMap));
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
            Multimap<String,Geometry> fieldToGeometryMap = (Multimap<String,Geometry>) data;
            
            String field = JexlASTHelper.getIdentifier(node);
            
            Collection<Geometry> queryGeometries = fieldToGeometryMap.get(field);
            
            if (queryGeometries != null && !queryGeometries.isEmpty()) {
                String value = (String) JexlASTHelper.getLiteralValue(node);
                Geometry nodeGeometry = GeoWaveUtils.positionToGeometry(value);
                
                // if the node geometry doesn't intersect the query geometry, get rid of this node
                if (fieldToGeometryMap.get(field).stream().noneMatch(nodeGeometry::intersects)) {
                    if (prunedTerms != null) {
                        prunedTerms.put(field, value);
                    }
                    return null;
                }
            }
        }
        
        return super.visit(node, data);
    }
}
