package datawave.query.jexl.visitors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import datawave.data.normalizer.IpAddressNormalizer;
import datawave.data.type.IpAddressType;
import datawave.data.type.Type;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlASTHelper.IdentifierOpLiteral;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.JexlNodeFactory.ContainerType;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
import datawave.query.util.MetadataHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.commons.jexl2.parser.JexlNodes.id;

/**
 * When more than one normalizer exists for a field, we want to transform the single term into a conjunction of the term with each normalizer applied to it. If
 * there is no difference between normalized versions of the term, only one term will be retained.
 *
 * This class will also normalize terms that have only 1 normalizer associated with their fields. For instance, if `TEXT` is associated with the
 * `LcNoDiacriticsType`, then a subtree of the form `TEXT == 'goOfBAlL'` will be transformed into `TEXT == 'goofball'`.
 *
 * 
 *
 */
public class ExpandMultiNormalizedTerms extends RebuildingVisitor {
    private static final Logger log = ThreadConfigurableLogger.getLogger(ExpandMultiNormalizedTerms.class);
    
    private final ShardQueryConfiguration config;
    private final HashSet<ASTAndNode> expandedNodes;
    private final MetadataHelper helper;
    
    public ExpandMultiNormalizedTerms(ShardQueryConfiguration config, MetadataHelper helper) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(helper);
        
        this.config = config;
        this.helper = helper;
        this.expandedNodes = Sets.newHashSet();
    }
    
    /**
     * Expand all nodes which have multiple dataTypes for the field.
     *
     * @param config
     * @param script
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T expandTerms(ShardQueryConfiguration config, MetadataHelper helper, T script) {
        ExpandMultiNormalizedTerms visitor = new ExpandMultiNormalizedTerms(config, helper);
        
        if (null == visitor.config.getQueryFieldsDatatypes()) {
            QueryException qe = new QueryException(DatawaveErrorCode.DATATYPESFORINDEXFIELDS_MULTIMAP_MISSING);
            throw new DatawaveFatalQueryException(qe);
        }
        
        return (T) script.jjtAccept(visitor, null);
    }
    
    @Override
    public Object visit(ASTEQNode node, Object data) {
        return expandNodeForNormalizers(node, data);
    }
    
    @Override
    public Object visit(ASTNENode node, Object data) {
        return expandNodeForNormalizers(node, data);
    }
    
    @Override
    public Object visit(ASTERNode node, Object data) {
        return expandNodeForNormalizers(node, data);
    }
    
    @Override
    public Object visit(ASTNRNode node, Object data) {
        return expandNodeForNormalizers(node, data);
    }
    
    @Override
    public Object visit(ASTLTNode node, Object data) {
        return expandNodeForNormalizers(node, data);
    }
    
    @Override
    public Object visit(ASTLENode node, Object data) {
        return expandNodeForNormalizers(node, data);
    }
    
    @Override
    public Object visit(ASTGTNode node, Object data) {
        return expandNodeForNormalizers(node, data);
    }
    
    @Override
    public Object visit(ASTGENode node, Object data) {
        return expandNodeForNormalizers(node, data);
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        /**
         * If we have a delayed predicate we can safely assume that expansion has occurred in the unfieldex expansion along with all types
         */
        if (isDelayedPredicate(node)) {
            return node;
        } else {
            return super.visit(node, data);
        }
        
    }
    
    /**
     * method to return if the current node is an instance of a delayed predicate
     * 
     * @param currNode
     * @return
     */
    protected boolean isDelayedPredicate(JexlNode currNode) {
        if (ASTDelayedPredicate.instanceOf(currNode) || ExceededOrThresholdMarkerJexlNode.instanceOf(currNode)
                        || ExceededValueThresholdMarkerJexlNode.instanceOf(currNode) || ExceededTermThresholdMarkerJexlNode.instanceOf(currNode)
                        || IndexHoleMarkerJexlNode.instanceOf(currNode) || ASTEvaluationOnly.instanceOf(currNode))
            return true;
        else
            return false;
    }
    
    @Override
    public Object visit(ASTAndNode node, Object data) {
        if (this.expandedNodes.contains(node)) {
            return node;
        }
        
        ASTAndNode smashed = TreeFlatteningRebuildingVisitor.flatten(node);
        HashMap<String,JexlNode> lowerBounds = Maps.newHashMap(), upperBounds = Maps.newHashMap();
        List<JexlNode> others = Lists.newArrayList();
        for (JexlNode child : JexlNodes.children(smashed)) {
            // if the child has a method attached, it is not eligible for ranges
            if (JexlASTHelper.HasMethodVisitor.hasMethod(child)) {
                others.add(child);
            } else {
                switch (id(child)) {
                    case ParserTreeConstants.JJTGENODE:
                    case ParserTreeConstants.JJTGTNODE:
                        String key = JexlASTHelper.getIdentifier(child);
                        // if the key is null, this is not eligible for ranges
                        // it may have model-expanded identifier (FOO|BAR) or it
                        // may have a method attached
                        if (key == null) {
                            others.add(child);
                        } else {
                            lowerBounds.put(key, child);
                        }
                        break;
                    case ParserTreeConstants.JJTLENODE:
                    case ParserTreeConstants.JJTLTNODE:
                        key = JexlASTHelper.getIdentifier(child);
                        // if the key is null, this is not eligible for ranges
                        // it may have model-expanded identifier (FOO|BAR) or it
                        // may have a method attached
                        if (key == null) {
                            others.add(child);
                        } else {
                            upperBounds.put(key, child);
                        }
                        break;
                    default:
                        others.add(child);
                }
            }
        }
        
        if (!lowerBounds.isEmpty() && !upperBounds.isEmpty()) {
            // this is the set of fields that have an upper and a lower bound operand
            Set<String> tightBounds = Sets.intersection(lowerBounds.keySet(), upperBounds.keySet()).immutableCopy();
            
            if (log.isDebugEnabled()) {
                log.debug("Found bounds to match: " + tightBounds);
            }
            
            for (String field : tightBounds) {
                List<ASTAndNode> aliasedBounds = Lists.newArrayList();
                for (Type<?> normalizer : config.getQueryFieldsDatatypes().get(field)) {
                    JexlNode lowerBound = lowerBounds.get(field), upperBound = upperBounds.get(field);
                    
                    JexlNode left = null;
                    try {
                        left = JexlASTHelper.applyNormalization(copy(lowerBound), normalizer);
                    } catch (Exception ne) {
                        if (log.isTraceEnabled()) {
                            log.trace("Could not normalize " + PrintingVisitor.formattedQueryString(lowerBound) + " using " + normalizer.getClass() + ". "
                                            + ne.getMessage());
                        }
                        continue;
                    }
                    
                    JexlNode right = null;
                    try {
                        right = JexlASTHelper.applyNormalization(copy(upperBound), normalizer);
                    } catch (Exception ne) {
                        if (log.isTraceEnabled()) {
                            log.trace("Could not normalize " + PrintingVisitor.formattedQueryString(upperBound) + " using " + normalizer.getClass() + ". "
                                            + ne.getMessage());
                        }
                        continue;
                    }
                    
                    aliasedBounds.add(JexlNodes.children(new ASTAndNode(ParserTreeConstants.JJTANDNODE), left, right));
                }
                if (!aliasedBounds.isEmpty()) {
                    lowerBounds.remove(field);
                    upperBounds.remove(field);
                    
                    this.expandedNodes.addAll(aliasedBounds);
                    
                    // Avoid extra parens around the expansion
                    if (1 == aliasedBounds.size()) {
                        others.add(JexlNodes.wrap(aliasedBounds.get(0)));
                    } else {
                        List<ASTReferenceExpression> var = JexlASTHelper.wrapInParens(aliasedBounds);
                        others.add(JexlNodes.wrap(JexlNodes.children(new ASTOrNode(ParserTreeConstants.JJTORNODE), var.toArray(new JexlNode[var.size()]))));
                    }
                }
            }
        }
        // we could have some unmatched bounds left over
        others.addAll(lowerBounds.values());
        others.addAll(upperBounds.values());
        
        /*
         * The rebuilding visitor adds whatever {visit()} returns to the parent's child list, so we shouldn't have some weird object graph that means old nodes
         * never get GC'd because {super.visit()} will reset the parent in the call to {copy()}
         */
        return super.visit(JexlNodes.children(smashed, others.toArray(new JexlNode[others.size()])), data);
    }
    
    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return FunctionNormalizationRebuildingVisitor.normalize(node, config.getQueryFieldsDatatypes(), helper, config.getDatatypeFilter());
    }
    
    /**
     * 
     * @param node
     * @param data
     * @return
     */
    protected JexlNode expandNodeForNormalizers(JexlNode node, Object data) {
        JexlNode nodeToReturn = node;
        
        IdentifierOpLiteral op = JexlASTHelper.getIdentifierOpLiteral(node);
        if (op != null) {
            
            final String fieldName = op.deconstructIdentifier();
            final Object literal = op.getLiteralValue();
            
            // Get all of the indexed or normalized dataTypes for the field name
            Set<Type<?>> dataTypes = Sets.newHashSet(config.getQueryFieldsDatatypes().get(fieldName));
            dataTypes.addAll(config.getNormalizedFieldsDatatypes().get(fieldName));
            
            // Catch the case of the user entering FIELD == null
            if (!dataTypes.isEmpty() && null != literal) {
                try {
                    String term = literal.toString();
                    Set<String> normalizedTerms = Sets.newHashSet();
                    
                    // Build up a set of normalized terms using each normalizer
                    for (Type<?> normalizer : dataTypes) {
                        try {
                            if (node instanceof ASTNRNode || node instanceof ASTERNode) {
                                normalizedTerms.add(normalizer.normalizeRegex(term));
                            } else {
                                normalizedTerms.add(normalizer.normalize(term));
                            }
                            log.debug("normalizedTerms=" + normalizedTerms);
                        } catch (IpAddressNormalizer.Exception ipex) {
                            try {
                                String[] lowHi = ((IpAddressType) normalizer).normalizeCidrToRange(term);
                                // node was FIELD == 'cidr'
                                // change to FIELD >= low and FIELD <= hi
                                JexlNode geNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), fieldName, lowHi[0]);
                                JexlNode leNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), fieldName, lowHi[1]);
                                
                                // now link em up
                                return JexlNodeFactory.createAndNode(Arrays.asList(geNode, leNode));
                            } catch (Exception ex) {
                                if (log.isTraceEnabled()) {
                                    log.trace("Could not normalize " + term + " as cidr notation with: " + normalizer.getClass());
                                }
                            }
                            // this could be CIDR notation, attempt to expand the node to the cidr range
                        } catch (Exception ne) {
                            if (log.isTraceEnabled()) {
                                log.trace("Could not normalize " + term + " using " + normalizer.getClass());
                            }
                        }
                    }
                    
                    if (normalizedTerms.size() > 1) {
                        
                        nodeToReturn = JexlNodeFactory.createNodeTreeFromFieldValues(ContainerType.OR_NODE, node, node, fieldName, normalizedTerms);
                        
                    } else if (1 == normalizedTerms.size()) {
                        // If there is only one term, we don't need to make an OR
                        nodeToReturn = JexlNodeFactory.buildUntypedNewLiteralNode(node, fieldName, normalizedTerms.iterator().next());
                    } else {
                        // If we couldn't map anything, return a copy
                        nodeToReturn = JexlNodeFactory.buildUntypedNewLiteralNode(node, fieldName, literal);
                    }
                    
                } catch (Exception e) {
                    QueryException qe = new QueryException(DatawaveErrorCode.NODE_EXPANSION_ERROR, e, MessageFormat.format("Node: {0}, Datatypes: {1}",
                                    PrintingVisitor.formattedQueryString(node), dataTypes));
                    log.error(qe);
                    throw new DatawaveFatalQueryException(qe);
                }
            }
        }
        return nodeToReturn;
    }
}
