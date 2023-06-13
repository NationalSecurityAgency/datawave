package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;

import java.util.Collection;

/**
 * Traverses the JexlNode tree, and extracts the highest and lowest granularity GeoWave tiers, assuming that this query contains GeoWave terms associated with
 * the supplied geoFields.
 */
public class GeoWaveQueryInfoVisitor extends ShortCircuitBaseVisitor {
    
    private final Collection<String> geoFields;
    
    public GeoWaveQueryInfoVisitor(Collection<String> geoFields) {
        this.geoFields = geoFields;
    }
    
    /**
     * @param script
     *            Jexl node
     * @return null if the query does not contain GeoWave terms associated with the supplied geoFields, otherwise returns GeoWaveQueryInfo which contains the
     *         highest and lowest granularity tiers for the query
     */
    public GeoWaveQueryInfo parseGeoWaveQueryInfo(JexlNode script) {
        GeoWaveQueryInfo queryInfo = new GeoWaveQueryInfo();
        script.childrenAccept(this, queryInfo);
        return queryInfo;
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        addTierInfo(node, data);
        return node.childrenAccept(this, data);
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        addTierInfo(node, data);
        return node.childrenAccept(this, data);
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        addTierInfo(node, data);
        return node.childrenAccept(this, data);
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        addTierInfo(node, data);
        return node.childrenAccept(this, data);
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        addTierInfo(node, data);
        return node.childrenAccept(this, data);
    }
    
    private void addTierInfo(JexlNode node, Object data) {
        GeoWaveQueryInfo queryInfo = (GeoWaveQueryInfo) data;
        
        JexlASTHelper.IdentifierOpLiteral op = JexlASTHelper.getIdentifierOpLiteral(node);
        if (op == null)
            return;
        
        final String fieldName = op.deconstructIdentifier();
        
        if (op.getLiteralValue() == null)
            return;
        
        String literal = op.getLiteralValue().toString();
        
        // GeoWave terms are hex encoded strings, where the
        // first two characters represent the numeric tier
        if (geoFields.contains(fieldName) && literal.length() >= 2)
            queryInfo.addTier(Integer.parseInt(literal.substring(0, 2), 16));
    }
    
    public static class GeoWaveQueryInfo implements Comparable<GeoWaveQueryInfo> {
        // the lowest granularity tier
        private int minTier;
        
        // the highest granularity tier
        private int maxTier;
        
        public GeoWaveQueryInfo() {
            this(Integer.MAX_VALUE, Integer.MIN_VALUE);
        }
        
        public GeoWaveQueryInfo(int minTier, int maxTier) {
            this.minTier = minTier;
            this.maxTier = maxTier;
        }
        
        public void addTier(int tier) {
            minTier = Math.min(minTier, tier);
            maxTier = Math.max(maxTier, tier);
        }
        
        public int getMinTier() {
            return minTier;
        }
        
        public int getMaxTier() {
            return maxTier;
        }
        
        /**
         * Sort by low granularity tier first, and by high granularity tier second
         */
        @Override
        public int compareTo(GeoWaveQueryInfo o) {
            if (this.minTier == o.minTier)
                return -(this.maxTier - o.maxTier);
            return -(this.minTier - o.minTier);
        }
    }
    
    // Recurse through these nodes
    @Override
    public Object visit(ASTOrNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }
    
}
