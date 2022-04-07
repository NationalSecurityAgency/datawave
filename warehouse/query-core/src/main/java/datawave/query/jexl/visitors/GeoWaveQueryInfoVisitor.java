package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
import org.apache.commons.jexl2.parser.ASTArrayLiteral;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTBlock;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFloatLiteral;
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTIntegerLiteral;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMapEntry;
import org.apache.commons.jexl2.parser.ASTMapLiteral;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTModNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTReturnStatement;
import org.apache.commons.jexl2.parser.ASTSizeFunction;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTernaryNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.ASTVar;
import org.apache.commons.jexl2.parser.ASTWhileStatement;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.SimpleNode;

import java.util.Collection;

/**
 * Traverses the JexlNode tree, and extracts the highest and lowest granularity GeoWave tiers, assuming that this query contains GeoWave terms associated with
 * the supplied geoFields.
 */
public class GeoWaveQueryInfoVisitor extends BaseVisitor {
    
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
    
    // Do not descend into these nodes
    @Override
    public Object visit(SimpleNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTBlock node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTAmbiguous node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTIfStatement node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTAdditiveNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTMulNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTDivNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTModNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTIdentifier node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTIntegerLiteral node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTFloatLiteral node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTMapEntry node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTSizeMethod node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTVar node, Object data) {
        return data;
    }
    
    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        return data;
    }
    
}
