package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.BOUNDED_RANGE;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.INDEX_HOLE;

import java.text.SimpleDateFormat;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.log4j.Logger;

import datawave.query.config.IndexValueHole;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.LiteralRange;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.parser.JavaRegexAnalyzer;
import datawave.query.util.MetadataHelper;

/**
 * Visitor meant to 'push down' predicates for expressions that are not executable against the index because of missing data in the global index.
 */
public class PushdownMissingIndexRangeNodesVisitor extends RebuildingVisitor {

    private static final Logger log = Logger.getLogger(PushdownMissingIndexRangeNodesVisitor.class);

    // a metadata helper
    protected MetadataHelper helper;
    // the begin and end dates for the query
    protected String beginDate;
    protected String endDate;
    // datatype filter
    protected Set<String> dataTypeFilter;
    // the set of holes known to exist in the index
    protected SortedSet<IndexValueHole> indexHoles = new TreeSet<>();

    /**
     * Construct the visitor
     *
     * @param config
     *            the logic configuration
     * @param helper
     *            the metadata helper
     */
    public PushdownMissingIndexRangeNodesVisitor(ShardQueryConfiguration config, MetadataHelper helper) {
        this.helper = helper;
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        this.beginDate = format.format(config.getBeginDate());
        this.endDate = format.format(config.getEndDate());
        this.dataTypeFilter = config.getDatatypeFilter();
        this.indexHoles.addAll(config.getIndexValueHoles());
    }

    /**
     * helper method that constructs and applies the visitor.
     *
     * @param config
     *            a config
     * @param helper
     *            the metadata helper
     * @param queryTree
     *            the query tree
     * @param <T>
     *            type of the query tree
     * @return a reference to the node
     */
    public static <T extends JexlNode> T pushdownPredicates(T queryTree, ShardQueryConfiguration config, MetadataHelper helper) {
        PushdownMissingIndexRangeNodesVisitor visitor = new PushdownMissingIndexRangeNodesVisitor(config, helper);
        return (T) (queryTree.jjtAccept(visitor, null));
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        // if not already delayed somehow
        if (!QueryPropertyMarker.findInstance(node).isAnyTypeExcept(BOUNDED_RANGE)) {
            LiteralRange range = JexlASTHelper.findRange().indexedOnly(this.dataTypeFilter, this.helper).notDelayed().getRange(node);

            if (range != null) {
                return delayBoundedIndexHole(range, node, data);
            } else {
                JexlNode andNode = JexlNodes.newInstanceOfType(node);
                JexlNodes.copyIdentifierOrLiteral(node, andNode);
                andNode.jjtSetParent(node.jjtGetParent());

                // We have no bounded range to replace, just proceed as normal
                JexlNodes.ensureCapacity(andNode, node.jjtGetNumChildren());
                for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                    JexlNode newChild = (JexlNode) node.jjtGetChild(i).jjtAccept(this, data);
                    andNode.jjtAddChild(newChild, i);
                    newChild.jjtSetParent(andNode);
                }
                return andNode;
            }
        } else {
            return node;
        }
    }

    /**
     * Delay the ranges that overlap holes. The range map is expected to only be indexed ranges.
     *
     * @param data
     *            the node data
     * @param range
     *            the range
     * @param currentNode
     *            the current node
     * @return a jexl node
     */
    protected JexlNode delayBoundedIndexHole(LiteralRange range, ASTAndNode currentNode, Object data) {

        if (missingIndexRange(range)) {
            return QueryPropertyMarker.create(currentNode, INDEX_HOLE);
        } else {
            return currentNode;
        }

    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        // if not already delayed somehow
        if (!QueryPropertyMarker.findInstance(node).isAnyTypeExcept(BOUNDED_RANGE)) {
            return super.visit(node, data);
        }
        return node;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        if (isIndexed(node) && missingIndexRange(node)) {
            return QueryPropertyMarker.create(node, INDEX_HOLE);
        }
        return node;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        if (isIndexed(node) && missingIndexRange(node)) {
            return QueryPropertyMarker.create(node, INDEX_HOLE);
        }
        return node;
    }

    public boolean isIndexed(JexlNode node) {
        String field = JexlASTHelper.getIdentifier(node);
        try {
            return (field != null && this.helper.isIndexed(field, this.dataTypeFilter));
        } catch (TableNotFoundException e) {
            throw new IllegalStateException("Unable to find metadata table", e);
        }
    }

    private boolean missingIndexRange(ASTEQNode node) {
        Object literal = JexlASTHelper.getLiteralValue(node);
        if (literal != null) {
            String strLiteral = String.valueOf(literal);
            for (IndexValueHole hole : this.indexHoles) {
                if (hole.overlaps(this.beginDate, this.endDate, strLiteral)) {
                    return true;
                } else if (hole.after(strLiteral)) {
                    return false;
                }
            }
        }
        return false;
    }

    private boolean missingIndexRange(ASTERNode node) {
        // TODO: need implementation for FieldIndexHole? Need field name, not values...
        // why is FieldIndexHole not related to IndexHole?
        Object literal = JexlASTHelper.getLiteralValue(node);
        if (literal != null) {
            String strLiteral = String.valueOf(literal);
            JavaRegexAnalyzer analyzer = null;
            try {
                analyzer = new JavaRegexAnalyzer(strLiteral);
                if (analyzer.isLeadingLiteral()) {
                    String leadingLiteral = analyzer.getLeadingLiteral();
                    StringBuilder endRange = new StringBuilder().append(leadingLiteral);
                    char lastChar = leadingLiteral.charAt(leadingLiteral.length() - 1);
                    if (lastChar < Character.MAX_VALUE) {
                        lastChar++;
                        endRange.setCharAt(endRange.length() - 1, lastChar);
                    } else {
                        endRange.append((char) 0);
                    }

                    for (IndexValueHole hole : indexHoles) {
                        // TODO: add overlaps method to FieldIndexHole...seriously what's up with the values
                        if (hole.overlaps(this.beginDate, this.endDate, leadingLiteral, endRange.toString())) {
                            return true;
                        } else if (hole.after(strLiteral)) {
                            return false;
                        }
                    }
                }
            } catch (JavaRegexAnalyzer.JavaRegexParseException e) {
                log.error("Unable to parse regex " + strLiteral, e);
                throw new DatawaveFatalQueryException("Unable to parse regex " + strLiteral, e);
            }
        }
        return false;
    }

    private boolean missingIndexRange(LiteralRange range) {
        String strUpper = String.valueOf(range.getUpper());
        String strLower = String.valueOf(range.getLower());
        for (IndexValueHole hole : indexHoles) {
            if (hole.overlaps(this.beginDate, this.endDate, strLower, strUpper)) {
                return true;
            } else if (hole.after(strLower)) {
                return false;
            }
        }
        return false;
    }

}
