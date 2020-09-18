package datawave.query.jexl.visitors;

import com.google.common.collect.Lists;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.IllegalRangeArgumentException;
import datawave.query.index.stats.IndexStatsClient;
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
import datawave.query.planner.pushdown.CostEstimator;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Visits an JexlNode tree, removing bounded ranges (a pair consisting of one GT or GE and one LT or LE node), and replacing them with concrete equality nodes.
 * The concrete equality nodes will be replaced with normalized values because the TextNormalizer interface can only normalize a value and cannot un-normalize a
 * value.
 *
 * 
 *
 */
public class RangeConjunctionRebuildingVisitor extends RebuildingVisitor {
    private static final Logger log = ThreadConfigurableLogger.getLogger(RangeConjunctionRebuildingVisitor.class);
    
    private final ShardQueryConfiguration config;
    private final ScannerFactory scannerFactory;
    private final IndexStatsClient stats;
    protected CostEstimator costAnalysis;
    protected Set<String> indexOnlyFields;
    protected Set<String> allFields;
    protected MetadataHelper helper;
    protected boolean expandFields;
    protected boolean expandValues;
    
    public RangeConjunctionRebuildingVisitor(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper, boolean expandFields,
                    boolean expandValues) throws TableNotFoundException, ExecutionException {
        this.config = config;
        this.helper = helper;
        this.indexOnlyFields = helper.getIndexOnlyFields(config.getDatatypeFilter());
        this.allFields = helper.getAllFields(config.getDatatypeFilter());
        this.scannerFactory = scannerFactory;
        stats = new IndexStatsClient(this.config.getConnector(), this.config.getIndexStatsTableName());
        costAnalysis = new CostEstimator(config, scannerFactory, helper);
        this.expandFields = expandFields;
        this.expandValues = expandValues;
    }
    
    /**
     * Expand all regular expression nodes into a conjunction of discrete terms mapping to that regular expression. For regular expressions that match nothing
     * in the global index, the regular expression node is left intact.
     *
     * @param config
     * @param helper
     * @param script
     * @return
     * @throws TableNotFoundException
     * @throws ExecutionException
     */
    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T expandRanges(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper, T script,
                    boolean expandFields, boolean expandValues) throws TableNotFoundException, ExecutionException {
        // if not expanding fields or values, then this is a noop
        if (expandFields || expandValues) {
            RangeConjunctionRebuildingVisitor visitor = new RangeConjunctionRebuildingVisitor(config, scannerFactory, helper, expandFields, expandValues);
            
            if (null == visitor.config.getQueryFieldsDatatypes()) {
                QueryException qe = new QueryException(DatawaveErrorCode.DATATYPESFORINDEXFIELDS_MULTIMAP_MISSING);
                throw new DatawaveFatalQueryException(qe);
            }
            
            return (T) (script.jjtAccept(visitor, null));
        } else {
            return script;
        }
    }
    
    @Override
    public Object visit(ASTReference node, Object data) {
        List<Class<? extends QueryPropertyMarker>> markers = Lists.newArrayList(new Class[] {IndexHoleMarkerJexlNode.class, ASTEvaluationOnly.class,
                ExceededValueThresholdMarkerJexlNode.class, ExceededTermThresholdMarkerJexlNode.class, ExceededOrThresholdMarkerJexlNode.class});
        if (QueryPropertyMarkerVisitor.instanceOf(node, markers, null)) {
            return node;
        } else if (BoundedRange.instanceOf(node)) {
            LiteralRange range = JexlASTHelper.findRange().indexedOnly(this.config.getDatatypeFilter(), this.helper).notDelayed().getRange(node);
            if (range != null) {
                return expandIndexBoundedRange(range, node);
            } else {
                return super.visit(node, data);
            }
        } else {
            return super.visit(node, data);
        }
    }
    
    protected JexlNode expandIndexBoundedRange(LiteralRange range, ASTReference currentNode) {
        // Sanity check to ensure that we found a range (redundant since we couldn't have made a bounded LiteralRange in the first
        // place if we had found no range nodes)
        if (range == null) {
            log.debug("Cannot find range operator nodes that encompass this query. Not proceeding with range expansion for this node.");
            return currentNode;
        }
        
        IndexLookup lookup = ShardIndexQueryTableStaticMethods.expandRange(range);
        
        IndexLookupMap fieldsToTerms = null;
        
        try {
            fieldsToTerms = lookup.lookup(config, scannerFactory, config.getMaxIndexScanTimeMillis());
        } catch (IllegalRangeArgumentException e) {
            log.info("Cannot expand "
                            + range
                            + " because it creates an invalid Accumulo Range. This is likely due to bad user input or failed normalization. This range will be ignored.");
            return RebuildingVisitor.copy(currentNode);
        }
        
        JexlNode orNode = JexlNodeFactory.createNodeTreeFromFieldsToValues(JexlNodeFactory.ContainerType.OR_NODE, new ASTEQNode(ParserTreeConstants.JJTEQNODE),
                        currentNode, fieldsToTerms, expandFields, expandValues, false);
        
        return orNode;
    }
    
}
