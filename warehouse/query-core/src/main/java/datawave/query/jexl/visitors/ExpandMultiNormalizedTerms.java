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
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
import datawave.query.util.MetadataHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
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
    private final HashSet<JexlNode> expandedNodes;
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
        
        script = TreeFlatteningRebuildingVisitor.flatten(script);
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
    public Object visit(ASTFunctionNode node, Object data) {
        return FunctionNormalizationRebuildingVisitor.normalize(node, config.getQueryFieldsDatatypes(), helper, config.getDatatypeFilter());
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        /**
         * If we have a delayed predicate we can safely assume that expansion has occurred in the unfielded expansion along with all types
         */
        if (QueryPropertyMarkerVisitor.isDelayedPredicate(node) || this.expandedNodes.contains(node)) {
            return node;
        }
        
        LiteralRange<?> range = JexlASTHelper.findRange().getRange(node);
        if (range != null) {
            return expandRangeForNormalizers(range, node);
        } else {
            return super.visit(node, data);
        }
    }
    
    private Object expandRangeForNormalizers(LiteralRange<?> range, JexlNode node) {
        List<BoundedRange> aliasedBounds = Lists.newArrayList();
        String field = range.getFieldName();
        
        // Get all of the indexed or normalized dataTypes for the field name
        Set<Type<?>> dataTypes = Sets.newHashSet(config.getQueryFieldsDatatypes().get(field));
        dataTypes.addAll(config.getNormalizedFieldsDatatypes().get(field));
        
        for (Type<?> normalizer : dataTypes) {
            JexlNode lowerBound = range.getLowerNode(), upperBound = range.getUpperNode();
            
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
            
            aliasedBounds.add(new BoundedRange(JexlNodes.children(new ASTAndNode(ParserTreeConstants.JJTANDNODE), left, right)));
        }
        
        if (aliasedBounds.isEmpty()) {
            return node;
        } else {
            this.expandedNodes.addAll(aliasedBounds);
            
            // Avoid extra parens around the expansion
            if (1 == aliasedBounds.size()) {
                return aliasedBounds.get(0);
            } else {
                List<ASTReferenceExpression> var = JexlASTHelper.wrapInParens(aliasedBounds);
                return JexlNodes.wrap(JexlNodes.children(new ASTOrNode(ParserTreeConstants.JJTORNODE), var.toArray(new JexlNode[var.size()])));
            }
        }
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
                                return BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geNode, leNode)));
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
