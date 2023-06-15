package datawave.query.planner.pushdown;

import static org.apache.commons.jexl2.parser.JexlNodes.children;
import static org.apache.commons.jexl2.parser.JexlNodes.id;

import java.util.NoSuchElementException;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.parser.JavaRegexAnalyzer;
import datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;
import datawave.query.Constants;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

/**
 *
 */
public class CostEstimator {

    private static final Logger log = Logger.getLogger(CostEstimator.class);
    protected ShardQueryConfiguration config;
    protected MetadataHelper helper;
    protected ScannerFactory scannerFactory;

    public CostEstimator(PushDownVisitor visitor) {
        this.config = visitor.getConfiguration();
        this.helper = visitor.getHelper();
        this.scannerFactory = visitor.getScannerFactory();
    }

    public CostEstimator(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper) {
        this.config = config;
        this.helper = helper;
        this.scannerFactory = scannerFactory;

    }

    public Cost computeCostForSubtree(JexlNode node) {

        switch (id(node)) {
            case ParserTreeConstants.JJTERNODE: {
                try {
                    String fieldName = JexlASTHelper.getIdentifier(node);
                    String pattern = JexlASTHelper.getLiteralValue(node).toString();

                    // if the term is _ANYFIELD_ (could not expand), then treat as 0 cost
                    if (fieldName.equals(Constants.ANY_FIELD) || fieldName.equals(Constants.NO_FIELD)) {
                        return new Cost(0l, 0l);
                    }

                    // If this is a disallowed pattern, it has infinite cost
                    if (config.getDisallowedRegexPatterns().contains(pattern)) {
                        return new Cost(Long.MAX_VALUE, 0l);
                    }

                    try {
                        JavaRegexAnalyzer regex = new JavaRegexAnalyzer(pattern);

                        // If we can't run this query against our indices, it has infinite cost
                        if (regex.isNgram()) {
                            return new Cost(Long.MAX_VALUE, 0l);
                        }
                    } catch (JavaRegexParseException e) {
                        if (log.isTraceEnabled()) {
                            log.trace("Couldn't parse regex from ERNode: " + pattern);
                        }
                    }

                    return new Cost(helper.getCountsByFieldForDays(fieldName, config.getBeginDate(), config.getEndDate(), config.getDatatypeFilter())
                                    * Cost.ER_COST_MULTIPLIER, 0l);
                } catch (NoSuchElementException e) {
                    log.trace("Could not find field name for ER node, ignoring for cost");
                    return new Cost();
                }
            }
            case ParserTreeConstants.JJTEQNODE: {
                try {
                    String fieldName = JexlASTHelper.getIdentifier(node);

                    // if the term is _ANYFIELD_ (could not expand), then treat as 0 cost
                    if (fieldName.equals(Constants.ANY_FIELD) || fieldName.equals(Constants.NO_FIELD)) {
                        return new Cost(0l, 0l);
                    }

                    // If the term is not indexed (could be _ANYFIELD_), treat it as infinite cost
                    try {
                        if (!helper.isIndexed(fieldName, config.getDatatypeFilter())) {
                            return new Cost(0l, Long.MAX_VALUE);
                        }
                    } catch (TableNotFoundException e) {
                        log.error("Could not find metadata table", e);
                    }

                    return new Cost(0l, helper.getCountsByFieldForDays(fieldName, config.getBeginDate(), config.getEndDate(), config.getDatatypeFilter()));
                } catch (NoSuchElementException e) {
                    log.trace("Could not find field name for EQ node, ignoring for cost");
                    return new Cost();
                }
            }
            case ParserTreeConstants.JJTANDNODE: {
                Cost andCost = null;
                for (JexlNode child : children(node)) {
                    Cost childCost = computeCostForSubtree(child);

                    // Retain the least-costly child in an AND
                    if (null == andCost) {
                        andCost = childCost;
                    } else if (0l == childCost.getERCost() && 0l == childCost.getOtherCost()) {
                        // We don't care to do anything if we got the default costs
                    } else if (andCost.totalCost() > childCost.totalCost()) {
                        andCost = childCost;
                    }
                }

                if (null == andCost) {
                    return new Cost();
                }

                return andCost;
            }
            case ParserTreeConstants.JJTORNODE: {
                Cost orCost = new Cost();
                for (JexlNode child : children(node)) {
                    Cost childCost = computeCostForSubtree(child);

                    orCost.incrementBy(childCost);
                }

                return orCost;
            }
            default: {
                if (1 == node.jjtGetNumChildren()) {
                    return computeCostForSubtree(node.jjtGetChild(0));
                } else {
                    return new Cost();
                }
            }
        }
    }

}
