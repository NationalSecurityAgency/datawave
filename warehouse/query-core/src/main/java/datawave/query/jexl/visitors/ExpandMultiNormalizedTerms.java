package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.DROPPED;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EVALUATION_ONLY;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_TERM;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_VALUE;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.LENIENT;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.STRICT;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.data.normalizer.IpAddressNormalizer;
import datawave.data.type.IpAddressType;
import datawave.data.type.OneToManyNormalizerType;
import datawave.data.type.Type;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlASTHelper.IdentifierOpLiteral;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.DroppedExpression;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.util.MetadataHelper;
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
    public Object visit(ASTAndNode node, Object data) {
        /*
         * If we have an exceeded value or term predicate we can safely assume that expansion has occurred in the unfielded expansion along with all types
         */
        QueryPropertyMarker.Instance marker = QueryPropertyMarker.findInstance(node);
        if (marker.isAnyTypeOf(EXCEEDED_VALUE, EXCEEDED_TERM) || this.expandedNodes.contains(node)) {
            return node;
        }

        // if we found a strict or lenient expression, then pass it through
        boolean strict = false;
        boolean lenient = false;
        JexlNode originalSource = null;
        if (marker.isType(STRICT)) {
            strict = true;
            data = marker;
            originalSource = copy(marker.getSource());
        } else if (marker.isType(LENIENT)) {
            lenient = true;
            data = marker;
            originalSource = copy(marker.getSource());
        }

        JexlNode returnNode = null;
        LiteralRange<?> range = JexlASTHelper.findRange().getRange(node);
        if (range != null) {
            returnNode = expandRangeForNormalizers(range, node);
        } else {
            returnNode = (JexlNode) (super.visit(node, data));
        }

        // if we have a strict or lenient marker, then unwrap it and handle all dropped
        if (strict || lenient) {
            QueryPropertyMarker.Instance newMarker = QueryPropertyMarker.findInstance(returnNode);
            returnNode = newMarker.getSource();

            // if everything was dropped and we are strict, then wrap the original node with eval only
            if (strict && isAllDropped(returnNode)) {
                returnNode = QueryPropertyMarker.create(originalSource, EVALUATION_ONLY);
            }
        }

        return returnNode;
    }

    private boolean isAllDropped(JexlNode node) {
        if (node instanceof ASTAndNode) {
            QueryPropertyMarker.Instance marker = QueryPropertyMarker.findInstance(node);
            if (marker.isAnyType()) {
                return (marker.isType(DROPPED));
            }
        }
        if (node instanceof ASTAndNode || node instanceof ASTOrNode || node instanceof ASTReferenceExpression) {
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

    private JexlNode expandRangeForNormalizers(LiteralRange<?> range, JexlNode node) {
        Map<String,JexlNode> aliasedBounds = new HashMap<>();
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

            // create the range node
            JexlNode rangeNode = JexlNodes.setChildren(new ASTAndNode(ParserTreeConstants.JJTANDNODE), left, right);
            rangeNode.jjtSetParent(node.jjtGetParent());

            // create a marked bounded range node
            JexlNode boundedRange = QueryPropertyMarker.create(rangeNode, BOUNDED_RANGE);

            String rangeString = JexlStringBuildingVisitor.buildQueryWithoutParse(boundedRange);
            aliasedBounds.putIfAbsent(rangeString, boundedRange);
        }

        if (aliasedBounds.isEmpty()) {
            return node;
        } else {
            this.expandedNodes.addAll(aliasedBounds.values());

            // Avoid extra parens around the expansion
            if (1 == aliasedBounds.size()) {
                return aliasedBounds.values().iterator().next();
            } else {
                // ensure we wrap bounded ranges in parens for certain edge cases
                List<ASTReferenceExpression> var = JexlASTHelper.wrapInParens(new ArrayList(aliasedBounds.values()));
                JexlNode finalNode = JexlNodes.setChildren(new ASTOrNode(ParserTreeConstants.JJTORNODE), var.toArray(new JexlNode[0]));
                return (node.jjtGetParent() instanceof ASTReferenceExpression) ? finalNode : JexlNodes.wrap(finalNode);
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

            // Determine whether this was explicitly marked as strict.
            boolean strict = config.getStrictFields().contains(fieldName)
                            || (data instanceof QueryPropertyMarker.Instance && ((QueryPropertyMarker.Instance) data).isType(STRICT));
            // Determine whether this was explicitly marked as lenient.
            boolean lenient = config.getLenientFields().contains(fieldName)
                            || (data instanceof QueryPropertyMarker.Instance && ((QueryPropertyMarker.Instance) data).isType(LENIENT));

            if (strict && lenient) {
                log.warn("Field " + fieldName + " marked both as strict and lenient.  Applying neither");
                strict = false;
                lenient = false;
            }

            // Get all the indexed or normalized dataTypes for the field name
            Set<Type<?>> dataTypes = Sets.newHashSet(config.getQueryFieldsDatatypes().get(fieldName));
            dataTypes.addAll(config.getNormalizedFieldsDatatypes().get(fieldName));

            // all normalizers must be applied to an ANYFIELD term
            if (fieldName.equals(Constants.ANY_FIELD)) {
                try {
                    dataTypes.addAll(helper.getAllDatatypes());
                } catch (InstantiationException | IllegalAccessException | TableNotFoundException e) {
                    log.error("Could not fetch all DataTypes while expanding unfielded term");
                    throw new RuntimeException(e);
                }
            }

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
                            if (normalizer instanceof OneToManyNormalizerType && ((OneToManyNormalizerType<?>) normalizer).expandAtQueryTime()) {
                                List<String> normTerms = ((OneToManyNormalizerType<?>) normalizer).normalizeToMany(term);
                                if (normTerms.size() == 1) {
                                    String normTerm = normTerms.iterator().next();
                                    normalizedTerms.add(normTerm);
                                    normalizedNodes.add(JexlNodeFactory.buildUntypedNode(node, fieldName, normTerm));
                                } else {
                                    List<JexlNode> normalizedOneToManyNodes = Lists.newArrayList();
                                    Iterator<String> iter = normTerms.iterator();
                                    while (iter.hasNext()) {
                                        normalizedOneToManyNodes.add(JexlNodeFactory.buildUntypedNode(node, fieldName, iter.next()));
                                    }

                                    JexlNode oneToManyNode = JexlNodeFactory.createAndNode(normalizedOneToManyNodes);
                                    normalizedNodes.add(oneToManyNode);
                                }

                            } else {
                                String normTerm = ((node instanceof ASTNRNode || node instanceof ASTERNode) ? normalizer.normalizeRegex(term)
                                                : normalizer.normalize(term));
                                if (!normalizedTerms.contains(normTerm)) {
                                    if (log.isDebugEnabled()) {
                                        log.debug("normalizedTerm = " + normTerm);
                                    }
                                    normalizedTerms.add(normTerm);
                                    normalizedNodes.add(JexlNodeFactory.buildUntypedNode(node, fieldName, normTerm));
                                }
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
                                        normalizedNodes.add(QueryPropertyMarker.create(JexlNodeFactory.createAndNode(Arrays.asList(geNode, leNode)),
                                                        BOUNDED_RANGE));
                                    }
                                } catch (Exception ex) {
                                    if (log.isTraceEnabled()) {
                                        log.trace("Could not normalize " + term + " as cidr notation with: " + normalizer.getClass());
                                    }
                                    // normalization failures do not matter for ANYFIELD terms
                                    failedNormalization = !fieldName.equals(Constants.ANY_FIELD);
                                }
                            } else {
                                // normalization failures do not matter for ANYFIELD terms
                                failedNormalization = !fieldName.equals(Constants.ANY_FIELD);
                            }
                        } catch (Exception ne) {
                            if (log.isTraceEnabled()) {
                                log.trace("Could not normalize " + term + " using " + normalizer.getClass());
                            }
                            // normalization failures do not matter for ANYFIELD terms
                            failedNormalization = !fieldName.equals(Constants.ANY_FIELD);
                        }
                    }

                    // determine if we are marking this term as dropped or evaluation only
                    boolean droppedExpression = false;
                    boolean evaluationOnly = false;
                    boolean isRegex = (node instanceof ASTNRNode || node instanceof ASTERNode);
                    if (failedNormalization) {
                        // if we are being strict then add the original term into the mix and make it eval only
                        if (strict) {
                            if (!normalizedTerms.contains(term)) {
                                normalizedTerms.add(term);
                                normalizedNodes.add(JexlNodeFactory.buildUntypedNode(node, fieldName, term));
                            }
                            evaluationOnly = true;
                        }
                        // else if we are being lenient and we have no successful normalizations, then drop the original term
                        else if (lenient) {
                            if (normalizedNodes.isEmpty()) {
                                normalizedTerms.add(term);
                                normalizedNodes.add(JexlNodeFactory.buildUntypedNode(node, fieldName, term));
                                droppedExpression = true;
                            }
                        }
                        // else we use the original methodology which is to keep the original term
                        // and push it down to evaluation only IFF a regex
                        else {
                            if (isRegex) {
                                if (!normalizedTerms.contains(term)) {
                                    normalizedTerms.add(term);
                                    normalizedNodes.add(JexlNodeFactory.buildUntypedNode(node, fieldName, term));
                                }
                                evaluationOnly = true;
                            }
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
                        nodeToReturn = QueryPropertyMarker.create(nodeToReturn, EVALUATION_ONLY);
                    } else if (droppedExpression) {
                        nodeToReturn = QueryPropertyMarker.create(
                                        new DroppedExpression(JexlStringBuildingVisitor.buildQuery(nodeToReturn), "Normalizations failed and not strict")
                                                        .getJexlNode(),
                                        DROPPED);
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
