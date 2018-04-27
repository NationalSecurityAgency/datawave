package datawave.query.jexl.visitors;

import java.util.concurrent.atomic.AtomicBoolean;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.util.MetadataHelper;

import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.JexlNode;

public class BoundedRangeDetectionVisitor extends FunctionIndexQueryExpansionVisitor {
    
    public BoundedRangeDetectionVisitor(ShardQueryConfiguration config, MetadataHelper metadataHelper) {
        super(config, metadataHelper, null);
    }
    
    @SuppressWarnings("unchecked")
    public static boolean mustExpandBoundedRange(ShardQueryConfiguration config, MetadataHelper metadataHelper, JexlNode script) {
        BoundedRangeDetectionVisitor visitor = new BoundedRangeDetectionVisitor(config, metadataHelper);
        
        AtomicBoolean hasBounded = new AtomicBoolean(false);
        script.jjtAccept(visitor, hasBounded);
        
        return hasBounded.get();
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        if (null != data) {
            AtomicBoolean hasBounded = (AtomicBoolean) data;
            hasBounded.set(true);
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
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        if (null != data) {
            AtomicBoolean hasBounded = (AtomicBoolean) data;
            hasBounded.set(true);
        }
        
        return false;
        
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        if (null != data) {
            AtomicBoolean hasBounded = (AtomicBoolean) data;
            hasBounded.set(true);
        }
        
        return false;
        
    }
}
