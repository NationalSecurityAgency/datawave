package datawave.query.jexl.visitors;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.DroppedExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.IllegalRangeArgumentException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.lookups.IndexLookup;
import datawave.query.jexl.lookups.IndexLookupMap;
import datawave.query.jexl.lookups.ShardIndexQueryTableStaticMethods;
import datawave.query.jexl.nodes.BoundedRange;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededTermThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.ExceededValueThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.IndexHoleMarkerJexlNode;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;

/**
 * Visits a Jexl tree, looks for bounded ranges, and replaces them with concrete values from the index
 */
public class BoundedRangeIndexExpansionVisitor extends BaseIndexExpansionVisitor {
    private static final Logger log = ThreadConfigurableLogger.getLogger(BoundedRangeIndexExpansionVisitor.class);

    private final JexlASTHelper.RangeFinder rangeFinder;

    // The constructor should not be made public so that we can ensure that the executor is setup and shutdown correctly
    protected BoundedRangeIndexExpansionVisitor(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper)
                    throws TableNotFoundException {
        super(config, scannerFactory, helper, "BoundedRangeIndexExpansion");

        rangeFinder = JexlASTHelper.findRange().indexedOnly(this.config.getDatatypeFilter(), this.helper).notDelayed();
    }

    /**
     * Visits the Jexl script, looks for bounded ranges, and replaces them with concrete values from the index
     *
     * @param config
     *            the query configuration, not null
     * @param scannerFactory
     *            the scanner factory, not null
     * @param helper
     *            the metadata helper, not null
     * @param script
     *            the Jexl script to expand, not null
     * @param <T>
     *            the Jexl node type
     * @return a rebuilt Jexl tree with it's bounded ranges expanded
     * @throws TableNotFoundException
     *             if we fail to retrieve fields from the metadata helper
     */
    public static <T extends JexlNode> T expandBoundedRanges(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper, T script)
                    throws TableNotFoundException {
        // if not expanding fields or values, then this is a noop
        if (config.isExpandFields() || config.isExpandValues()) {
            BoundedRangeIndexExpansionVisitor visitor = new BoundedRangeIndexExpansionVisitor(config, scannerFactory, helper);
            return visitor.expand(script);
        } else {
            return script;
        }
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);

        // don't traverse delayed nodes
        if (instance.isAnyTypeOf(IndexHoleMarkerJexlNode.class, ASTEvaluationOnly.class, DroppedExpression.class, ExceededValueThresholdMarkerJexlNode.class,
                        ExceededTermThresholdMarkerJexlNode.class, ExceededOrThresholdMarkerJexlNode.class)) {
            return RebuildingVisitor.copy(node);
        }
        // handle bounded range
        else if (instance.isType(BoundedRange.class)) {
            LiteralRange<?> range = rangeFinder.getRange(node);
            if (range != null) {
                try {
                    return buildIndexLookup(node, true, false, () -> createLookup(range));
                } catch (IllegalRangeArgumentException e) {
                    log.error("Cannot expand [" + JexlStringBuildingVisitor.buildQuery(node)
                                    + "] because it creates an invalid Accumulo Range. This is likely due to bad user input or failed normalization. This range will be ignored.",
                                    e);
                }
            }
        }

        return super.visit(node, data);
    }

    protected IndexLookup createLookup(LiteralRange<?> range) {
        return ShardIndexQueryTableStaticMethods.expandRange(config, scannerFactory, range, executor);
    }

    @Override
    protected void rebuildFutureJexlNode(FutureJexlNode futureJexlNode) {
        JexlNode currentNode = futureJexlNode.getOrigNode();
        IndexLookupMap fieldsToTerms = futureJexlNode.getLookup().lookup();

        futureJexlNode.setRebuiltNode(JexlNodeFactory.createNodeTreeFromFieldsToValues(JexlNodeFactory.ContainerType.OR_NODE, false, currentNode, fieldsToTerms,
                        expandFields, expandValues, futureJexlNode.isKeepOriginalNode()));
    }
}
