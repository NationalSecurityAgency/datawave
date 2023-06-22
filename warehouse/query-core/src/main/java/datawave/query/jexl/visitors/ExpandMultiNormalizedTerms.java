package datawave.query.jexl.visitors;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.jexl2.parser.ASTAndNode;
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
import org.apache.commons.jexl2.parser.DroppedExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.LenientExpression;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import datawave.data.normalizer.IpAddressNormalizer;
import datawave.data.type.IpAddressType;
import datawave.data.type.Type;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlASTHelper.IdentifierOpLiteral;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.util.MetadataHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

/**
 * When more than one normalizer exists for a field, we want to transform the single term into a conjunction of the term with each normalizer applied to it. If
 * there is no difference between normalized versions of the term, only one term will be retained.
 * <p>
 * This class will also normalize terms that have only 1 normalizer associated with their fields. For instance, if `TEXT` is associated with the
 * `LcNoDiacriticsType`, then a subtree of the form `TEXT == 'goOfBAlL'` will be transformed into `TEXT == 'goofball'`.
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
     *            a config
     * @param script
     *            a script
     * @param <T>
     *            type of node
     * @param helper
     *            the metadata helper
     * @return a reference to the node
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
         * If we have an exceeded value or term predicate we can safely assume that expansion has occurred in the unfielded expansion along with all types
         */
        QueryPropertyMarker.Instance marker = QueryPropertyMarker.findInstance(node);
        if (marker.isAnyTypeOf(ExceededValueThresholdMarkerJexlNode.class, ExceededTermThresholdMarkerJexlNode.class) || this.expandedNodes.contains(node)) {
            return node;
        }

        // if we found a lenient expression, then pass it through
        boolean lenient = false;
        JexlNode originalSource = null;
        if (marker.isType(LenientExpression.class)) {
            lenient = true;
            data = marker;
            originalSource = copy(marker.getSource());
        }

        JexlNode returnNode = null;
        LiteralRange<?> range = JexlASTHelper.findRange().getRange(node);
        if (range != null) {
            returnNode = expandRangeForNormalizers(range, node, data);
        } else {
            returnNode = (JexlNode) (super.visit(node, data));
        }

        // if we had a lenient marker, then unwrap it and handle all dropped
        if (lenient) {
            QueryPropertyMarker.Instance newMarker = QueryPropertyMarker.findInstance(returnNode);
            returnNode = newMarker.getSource();

            // now we need to check if everything was dropped in which case we return the original node source eval only
            if (isAllDropped(returnNode)) {
                returnNode = ASTEvaluationOnly.create(originalSource);
            }
        }

        return returnNode;
    }

    private boolean isAllDropped(JexlNode node) {
        if (node instanceof ASTAndNode) {
            QueryPropertyMarker.Instance marker = QueryPropertyMarker.findInstance(node);
            if (marker.isAnyType()) {
                return (marker.isType(DroppedExpression.class));
            }
        }
        if (node instanceof ASTAndNode || node instanceof ASTOrNode || node instanceof ASTReferenceExpression || node instanceof ASTReference) {
            for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                JexlNode child = node.jjtGetChild(i);
                if (!isAllDropped(child)) {
                    return false;
                }
            }
            return true;
        }
        // all other nodes means we have a non-dropped expression
        return false;
    }

    private JexlNode expandRangeForNormalizers(LiteralRange<?> range, JexlNode node, Object data) {
        Set<BoundedRange> aliasedBounds = new HashSet<>();
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
                return aliasedBounds.iterator().next();
            } else {
                // ensure we wrap bounded ranges in parens for certain edge cases
                List<ASTReferenceExpression> var = JexlASTHelper.wrapInParens(new ArrayList(aliasedBounds));
                return JexlNodes.wrap(JexlNodes.children(new ASTOrNode(ParserTreeConstants.JJTORNODE), var.toArray(new JexlNode[var.size()])));
            }
        }
    }

    /**
     * @param node
     *            a jexl node
     * @param data
     *            the node data
     * @return a jexl node
     */
    protected JexlNode expandNodeForNormalizers(JexlNode node, Object data) {
        JexlNode nodeToReturn = node;

        IdentifierOpLiteral op = JexlASTHelper.getIdentifierOpLiteral(node);
        if (op != null) {

            final String fieldName = op.deconstructIdentifier();
            final Object literal = op.getLiteralValue();

            boolean lenient = !config.getStrictFields().contains(fieldName) && (config.getLenientFields().contains(fieldName)
                            || (data instanceof QueryPropertyMarker.Instance && ((QueryPropertyMarker.Instance) data).isType(LenientExpression.class)));

            // Get all the indexed or normalized dataTypes for the field name
            Set<Type<?>> dataTypes = Sets.newHashSet(config.getQueryFieldsDatatypes().get(fieldName));
            dataTypes.addAll(config.getNormalizedFieldsDatatypes().get(fieldName));

            // Catch the case of the user entering FIELD == null
            if (!dataTypes.isEmpty() && null != literal) {
                try {
                    String term = literal.toString();
                    Set<String> normalizedTerms = Sets.newHashSet();
                    List<JexlNode> normalizedNodes = Lists.newArrayList();
                    boolean failedNormalization = false;
                    // Build up a set of normalized terms using each normalizer
                    for (Type<?> normalizer : dataTypes) {
                        try {
                            String normTerm = ((node instanceof ASTNRNode || node instanceof ASTERNode) ? normalizer.normalizeRegex(term)
                                            : normalizer.normalize(term));
                            if (!normalizedTerms.contains(normTerm)) {
                                if (log.isDebugEnabled()) {
                                    log.debug("normalizedTerm = " + normTerm);
                                }
                                normalizedTerms.add(normTerm);
                                normalizedNodes.add(JexlNodeFactory.buildUntypedNode(node, fieldName, normTerm));
                            }
                        } catch (IpAddressNormalizer.Exception ipex) {
                            if (!(node instanceof ASTNRNode || node instanceof ASTERNode)) {
                                try {
                                    // this could be CIDR notation, attempt to expand the node to the cidr range
                                    String[] lowHi = ((IpAddressType) normalizer).normalizeCidrToRange(term);
                                    String normTerm = Arrays.asList(lowHi).toString();
                                    if (!normalizedTerms.contains(normTerm)) {
                                        if (log.isDebugEnabled()) {
                                            log.debug("normalizedTerm = (" + lowHi[0] + ", " + lowHi[1] + ")");
                                        }
                                        normalizedTerms.add(normTerm);
                                        // node was FIELD == 'cidr'
                                        // change to FIELD >= low and FIELD <= hi
                                        JexlNode geNode = JexlNodeFactory.buildNode(new ASTGENode(ParserTreeConstants.JJTGENODE), fieldName, lowHi[0]);
                                        JexlNode leNode = JexlNodeFactory.buildNode(new ASTLENode(ParserTreeConstants.JJTLENODE), fieldName, lowHi[1]);

                                        // now link em up
                                        normalizedNodes.add(BoundedRange.create(JexlNodeFactory.createAndNode(Arrays.asList(geNode, leNode))));
                                    }
                                } catch (Exception ex) {
                                    if (log.isTraceEnabled()) {
                                        log.trace("Could not normalize " + term + " as cidr notation with: " + normalizer.getClass());
                                    }
                                    failedNormalization = true;
                                }
                            } else {
                                failedNormalization = true;
                            }
                        } catch (Exception ne) {
                            if (log.isTraceEnabled()) {
                                log.trace("Could not normalize " + term + " using " + normalizer.getClass());
                            }
                            failedNormalization = true;
                        }
                    }

                    // determine if we are marking this term as dropped or evaluation only
                    boolean droppedExpression = false;
                    boolean evaluationOnly = false;
                    if (failedNormalization) {
                        // if we are not being lenient then add the original term into the mix and make it eval only
                        if (!lenient) {
                            if (!normalizedTerms.contains(term)) {
                                normalizedTerms.add(term);
                                normalizedNodes.add(JexlNodeFactory.buildUntypedNode(node, fieldName, term));
                            }
                            evaluationOnly = true;
                        }
                        // else if we are being lenient and we have no successful normalizations, then drop the original term
                        else if (normalizedNodes.isEmpty()) {
                            if (!normalizedTerms.contains(term)) {
                                normalizedTerms.add(term);
                                normalizedNodes.add(JexlNodeFactory.buildUntypedNode(node, fieldName, term));
                            }
                            droppedExpression = true;
                        }
                    }

                    // build an expression from the normalized terms
                    if (normalizedNodes.isEmpty()) {
                        // If we couldn't map anything, return a copy
                        nodeToReturn = JexlNodeFactory.buildUntypedNewLiteralNode(node, fieldName, literal);
                    } else if (1 == normalizedNodes.size()) {
                        // If there is only one term, we don't need to make an OR
                        nodeToReturn = normalizedNodes.iterator().next();
                    } else {
                        // if it is a negated node, then and the possibilities
                        if (node instanceof ASTNRNode || node instanceof ASTNENode) {
                            nodeToReturn = JexlNodeFactory.createAndNode(normalizedNodes);
                        } else {
                            nodeToReturn = JexlNodeFactory.createOrNode(normalizedNodes);
                        }
                    }

                    // wrap the node if required
                    if (evaluationOnly) {
                        nodeToReturn = ASTEvaluationOnly.create(nodeToReturn);
                    } else if (droppedExpression) {
                        nodeToReturn = DroppedExpression.create(nodeToReturn, "Normalizations failed and lenient");
                    }

                } catch (Exception e) {
                    QueryException qe = new QueryException(DatawaveErrorCode.NODE_EXPANSION_ERROR, e,
                                    MessageFormat.format("Node: {0}, Datatypes: {1}", PrintingVisitor.formattedQueryString(node), dataTypes));
                    log.error(qe);
                    throw new DatawaveFatalQueryException(qe);
                }
            }
        }
        return nodeToReturn;
    }

}
