package datawave.query.jexl.visitors;

import java.util.concurrent.atomic.AtomicBoolean;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.util.MetadataHelper;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.JexlNode;

public class BoundedRangeDetectionVisitor extends BaseVisitor {
    
    ShardQueryConfiguration config;
    MetadataHelper helper;
    
    public BoundedRangeDetectionVisitor(ShardQueryConfiguration config, MetadataHelper metadataHelper) {
        this.config = config;
        this.helper = metadataHelper;
    }
    
    @SuppressWarnings("unchecked")
    public static boolean mustExpandBoundedRange(ShardQueryConfiguration config, MetadataHelper metadataHelper, JexlNode script) {
        BoundedRangeDetectionVisitor visitor = new BoundedRangeDetectionVisitor(config, metadataHelper);
        
        AtomicBoolean hasBounded = new AtomicBoolean(false);
        script.jjtAccept(visitor, hasBounded);
        
        return hasBounded.get();
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        if (BoundedRange.instanceOf(node)) {
            LiteralRange range = JexlASTHelper.findRange().getRange(node);
            try {
                if (helper.getNonEventFields(config.getDatatypeFilter()).contains(range.getFieldName())) {
                    if (null != data) {
                        AtomicBoolean hasBounded = (AtomicBoolean) data;
                        hasBounded.set(true);
                    }
                }
            } catch (TableNotFoundException e) {
                throw new DatawaveFatalQueryException("Cannot access metadata", e);
            }
            
            return false;
        } else {
            return super.visit(node, data);
        }
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        try {
            if (helper.getNonEventFields(config.getDatatypeFilter()).contains(JexlASTHelper.getIdentifier(node))) {
                if (null != data) {
                    AtomicBoolean hasBounded = (AtomicBoolean) data;
                    hasBounded.set(true);
                }
            }
        } catch (TableNotFoundException e) {
            throw new DatawaveFatalQueryException("Cannot access metadata", e);
        }
        
        return false;
        
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        if (null != data) {
            AtomicBoolean hasBounded = (AtomicBoolean) data;
            hasBounded.set(true);
        }
        
        return false;
        
    }
    
}
