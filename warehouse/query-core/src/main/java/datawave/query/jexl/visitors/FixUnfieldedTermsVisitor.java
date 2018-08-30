package datawave.query.jexl.visitors;

import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.CannotExpandUnfieldedTermFatalException;
import datawave.query.jexl.lookups.FieldNameLookup;
import datawave.query.jexl.lookups.IndexLookup;
import datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.Node;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Replace any node in the query which has an Identifier of {@link Constants#ANY_FIELD} with a conjunction (or) of discrete names for the given term from the
 * global index.
 * 
 * 
 */
public class FixUnfieldedTermsVisitor extends ParallelIndexExpansion {
    private static final Logger log = Logger.getLogger(FixUnfieldedTermsVisitor.class);
    
    protected JexlNode currentNode;
    
    public FixUnfieldedTermsVisitor(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper, Set<String> expansionFields)
                    throws InstantiationException, IllegalAccessException, TableNotFoundException {
        super(config, scannerFactory, helper, expansionFields, "Datawave Unfielded Lookup");
    }
    
    protected class FixUnfieldedTermsVisitorThreadFactory implements ThreadFactory {
        
        private ThreadFactory dtf = Executors.defaultThreadFactory();
        private int threadNum = 1;
        private String threadIdentifier;
        
        public FixUnfieldedTermsVisitorThreadFactory(Query query) {
            if (query == null || query.getId() == null) {
                this.threadIdentifier = "(unknown)";
            } else {
                this.threadIdentifier = query.getId().toString();
            }
        }
        
        public Thread newThread(Runnable r) {
            Thread thread = dtf.newThread(r);
            thread.setName("Datawave FixUnfieldedTermsVisitor Session " + threadIdentifier + " -" + threadNum++);
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler(config.getQuery().getUncaughtExceptionHandler());
            return thread;
        }
        
    }
    
    public static ASTJexlScript fixUnfieldedTree(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper, ASTJexlScript script)
                    throws InstantiationException, IllegalAccessException, TableNotFoundException {
        return fixUnfieldedTree(config, scannerFactory, helper, script, null);
    }
    
    public static ASTJexlScript fixUnfieldedTree(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper, ASTJexlScript script,
                    Set<String> expansionFields) throws InstantiationException, IllegalAccessException, TableNotFoundException {
        FixUnfieldedTermsVisitor visitor = new FixUnfieldedTermsVisitor(config, scannerFactory, helper, expansionFields);
        
        return (ASTJexlScript) script.jjtAccept(visitor, null);
    }
    
    @Override
    public Object visit(ASTJexlScript incomingNode, Object data) {
        
        setupThreadResources();
        
        ASTJexlScript node = new ASTJexlScript(ParserTreeConstants.JJTJEXLSCRIPT);
        
        node = (ASTJexlScript) RebuildingVisitor.copy(incomingNode);
        
        int newIndex = 0;
        
        try {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                Object objectReturn = node.jjtGetChild(i).jjtAccept(this, data);
                
                if (objectReturn instanceof Node) {
                    Node newChild = (Node) objectReturn;
                    if (newChild != null) {
                        // When we have an AND or OR
                        if ((newChild instanceof ASTOrNode || newChild instanceof ASTAndNode)) {
                            // Only add that node if it actually has children
                            if (0 < newChild.jjtGetNumChildren()) {
                                node.jjtAddChild(newChild, newIndex);
                                newIndex++;
                            }
                        } else {
                            // Otherwise, we want to add the child regardless
                            node.jjtAddChild(newChild, newIndex);
                            newIndex++;
                        }
                    }
                } else {
                    // Otherwise, we want to add the child regardless
                    node.jjtAddChild(newChild, newIndex);
                    newIndex++;
                }
                
            }
            concurrentExecution();
        } finally {
            log.debug("Shutting down executor");
            // no need for this anymore.
            if (executor != null) {
                executor.shutdownNow();
            }
        }
        
        LookupRemark remark = new LookupRemark();
        node = (ASTJexlScript) node.jjtAccept(remark, data);
        
        if (node.jjtGetNumChildren() == 0) {
            NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.NO_UNFIELDED_TERM_EXPANSION_MATCH);
            log.warn(qe);
            throw new CannotExpandUnfieldedTermFatalException(qe);
        }
        
        return node;
    }
    
    @Override
    public Object visit(ASTOrNode node, Object data) {
        
        ASTOrNode newNode = new ASTOrNode(ParserTreeConstants.JJTORNODE);
        newNode.image = node.image;
        
        visit(node, newNode, data);
        
        switch (newNode.jjtGetNumChildren()) {
            case 0:
                return null;
            case 1:
                JexlNode child = newNode.jjtGetChild(0);
                JexlNodes.promote(newNode, child);
                return child;
            default:
                return newNode;
        }
    }
    
    protected void visit(JexlNode node, JexlNode newNode, Object data) {
        
        int newIndex = 0;
        
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            Object objectReturn = node.jjtGetChild(i).jjtAccept(this, data);
            
            if (objectReturn instanceof IndexLookupCallback) {
                
                ((IndexLookupCallback) objectReturn).get().setParentId(newNode, newIndex);
                newNode.jjtAddChild(((IndexLookupCallback) objectReturn), newIndex);
                
                newIndex++;
            } else if (objectReturn instanceof Node) {
                
                Node newChild = (Node) objectReturn;
                if (newChild != null) {
                    // When we have an AND or OR
                    if ((newChild instanceof ASTOrNode || newChild instanceof ASTAndNode)) {
                        // Only add that node if it actually has children
                        if (0 < newChild.jjtGetNumChildren()) {
                            newNode.jjtAddChild(newChild, newIndex);
                            newIndex++;
                        }
                    } else {
                        // Otherwise, we want to add the child regardless
                        newNode.jjtAddChild(newChild, newIndex);
                        newIndex++;
                    }
                }
            } else {
                
                newNode.jjtAddChild(node.jjtGetChild(i), newIndex);
                newIndex++;
            }
        }
        
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        
        ASTAndNode newNode = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
        newNode.image = node.image;
        
        visit(node, newNode, data);
        
        switch (newNode.jjtGetNumChildren()) {
            case 0:
                return null;
            case 1:
                JexlNode child = newNode.jjtGetChild(0);
                JexlNodes.promote(newNode, child);
                return child;
            default:
                return newNode;
        }
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        return expandFieldNames(node, true);
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        
        concurrentExecution();
        try {
            Object obj = expandFieldNames(node, false);
            concurrentExecution();
            return obj;
        } catch (CannotExpandUnfieldedTermFatalException e) {
            log.error(e);
            ASTOrNode emptyOrNode = new ASTOrNode(ParserTreeConstants.JJTORNODE);
            emptyOrNode.jjtSetParent(node.jjtGetParent());
            return emptyOrNode;
        }
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        return expandFieldNames(node, true);
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        
        concurrentExecution();
        try {
            Object obj = expandFieldNames(node, false);
            concurrentExecution();
            return obj;
        } catch (CannotExpandUnfieldedTermFatalException e) {
            ASTOrNode emptyOrNode = new ASTOrNode(ParserTreeConstants.JJTORNODE);
            emptyOrNode.jjtSetParent(node.jjtGetParent());
            return emptyOrNode;
        }
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return expandFieldNames(node, true);
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return expandFieldNames(node, true);
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return expandFieldNames(node, true);
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return expandFieldNames(node, true);
    }
    
    @Override
    public Object visit(ASTNotNode node, Object data) {
        
        concurrentExecution();
        try {
            Object obj = super.visit(node, data);
            concurrentExecution();
            return obj;
        } catch (CannotExpandUnfieldedTermFatalException e) {
            log.error(e);
            ASTOrNode emptyOrNode = new ASTOrNode(ParserTreeConstants.JJTORNODE);
            emptyOrNode.jjtSetParent(node.jjtGetParent());
            return emptyOrNode;
        }
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        
        ASTReference ref = (ASTReference) super.visit(node, data);
        if (JexlNodes.children(ref).length == 0) {
            return null;
        } else {
            return ref;
        }
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        ASTReferenceExpression ref = (ASTReferenceExpression) super.visit(node, data);
        if (JexlNodes.children(ref).length == 0) {
            return null;
        } else {
            return ref;
        }
    }
    
    /**
     * Given a JexlNode, get all grandchildren which follow a path from ASTReference to ASTIdentifier, returning true if the image of the ASTIdentifier is equal
     * to {@link Constants#ANY_FIELD}
     * 
     * @param node
     *            The starting node to check
     * @return
     */
    protected boolean hasUnfieldedIdentifier(JexlNode node) {
        if (null == node || 2 != node.jjtGetNumChildren()) {
            return false;
        }
        
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);
            
            if (null != child && child instanceof ASTReference) {
                for (int j = 0; j < child.jjtGetNumChildren(); j++) {
                    JexlNode grandChild = child.jjtGetChild(j);
                    
                    // If the grandchild and its image is non-null and equal to
                    // the any-field identifier
                    if (null != grandChild && grandChild instanceof ASTIdentifier && null != grandChild.image && Constants.ANY_FIELD.equals(grandChild.image)) {
                        return true;
                    }
                }
            }
        }
        
        return false;
    }
    
    /**
     * Performs a lookup in the global index for an ANYFIELD term and returns the field names where the term is found
     * 
     * @param node
     * @param positive
     * @return set of field names from the global index for the nodes value
     * @throws TableNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    @Override
    protected IndexLookupCallable buildIndexLookup(JexlNode node, boolean positive) throws TableNotFoundException, IOException, InstantiationException,
                    IllegalAccessException {
        // Using the datatype filter when expanding this term isn't really
        // necessary
        IndexLookup lookup = ShardIndexQueryTableStaticMethods.normalizeQueryTerm(node, this.expansionFields, this.allTypes, helper);
        
        if (lookup instanceof FieldNameLookup && config.getLimitAnyFieldLookups()) {
            lookup.setLimitToTerms(true);
            ((FieldNameLookup) lookup).setTypeFilterSet(config.getDatatypeFilter());
        }
        return new IndexLookupCallable(lookup, node, positive, false, true);
    }
    
    @Override
    protected boolean isIdentifier(JexlNode node) {
        return hasUnfieldedIdentifier(node);
    }
    
}
