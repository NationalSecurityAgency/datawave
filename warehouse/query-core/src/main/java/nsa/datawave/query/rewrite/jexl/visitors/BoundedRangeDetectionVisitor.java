package nsa.datawave.query.rewrite.jexl.visitors;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import nsa.datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import nsa.datawave.query.util.MetadataHelper;
import nsa.datawave.webservice.common.logging.ThreadConfigurableLogger;

import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

public class BoundedRangeDetectionVisitor extends FunctionIndexQueryExpansionVisitor {
    protected Set<String> indexOnlyFields;
    protected Set<String> termFrequencyFields;
    
    public BoundedRangeDetectionVisitor(RefactoredShardQueryConfiguration config, MetadataHelper metadataHelper, Set<String> indexOnlyFields,
                    Set<String> termFrequencyFields) {
        super(config, metadataHelper, null);
        this.indexOnlyFields = indexOnlyFields;
        this.termFrequencyFields = termFrequencyFields;
    }
    
    @SuppressWarnings("unchecked")
    public static boolean mustExpandBoundedRange(RefactoredShardQueryConfiguration config, MetadataHelper metadataHelper, Set<String> indexOnlyFields,
                    Set<String> termFrequencyFields, JexlNode script) {
        BoundedRangeDetectionVisitor visitor = new BoundedRangeDetectionVisitor(config, metadataHelper, indexOnlyFields, termFrequencyFields);
        
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
