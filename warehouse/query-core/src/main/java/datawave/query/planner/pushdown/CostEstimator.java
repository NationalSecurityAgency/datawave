package datawave.query.planner.pushdown;

import static org.apache.commons.jexl3.parser.JexlNodes.id;

import java.util.NoSuchElementException;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.parser.JavaRegexAnalyzer;
import datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;

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
                        return Cost.zero();
                    }

                    // If this is a disallowed pattern, it has infinite cost
                    if (config.getDisallowedRegexPatterns().contains(pattern)) {
                        return Cost.infiniteRegex();
                    }

                    try {
                        JavaRegexAnalyzer regex = new JavaRegexAnalyzer(pattern);

                        // If we can't run this query against our indices, it has infinite cost
                        if (regex.isNgram()) {
                            return Cost.infiniteRegex();
                        }
                    } catch (JavaRegexParseException e) {
                        if (log.isTraceEnabled()) {
                            log.trace("Couldn't parse regex from ERNode: " + pattern);
                        }
                    }

                    long regexCost = helper.getCountsByFieldForDays(fieldName, config.getBeginDate(), config.getEndDate(), config.getDatatypeFilter());
                    return Cost.regex(regexCost);
                } catch (NoSuchElementException e) {
                    log.trace("Could not find field name for ER node, ignoring for cost");
                    return Cost.zero();
                }
            }
            case ParserTreeConstants.JJTEQNODE: {
                try {
                    String fieldName = JexlASTHelper.getIdentifier(node);

                    // if the term is _ANYFIELD_ (could not expand), then treat as 0 cost
                    if (fieldName.equals(Constants.ANY_FIELD) || fieldName.equals(Constants.NO_FIELD)) {
                        return Cost.zero();
                    }

                    // If the term is not indexed (could be _ANYFIELD_), treat it as infinite cost
                    try {
                        if (!helper.isIndexed(fieldName, config.getDatatypeFilter())) {
                            return Cost.infiniteOther();
                        }
                    } catch (TableNotFoundException e) {
                        log.error("Could not find metadata table", e);
                    }

                    long cost = helper.getCountsByFieldForDays(fieldName, config.getBeginDate(), config.getEndDate(), config.getDatatypeFilter());
                    return Cost.other(cost);
                } catch (NoSuchElementException e) {
                    log.trace("Could not find field name for EQ node, ignoring for cost");
                    return Cost.zero();
                }
            }
            case ParserTreeConstants.JJTANDNODE: {
                Cost total = null;
                for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                    Cost childCost = computeCostForSubtree(node.jjtGetChild(i));

                    // Retain the least-costly child in an AND
                    if (null == total) {
                        total = childCost;
                    } else if (childCost.isZeroCost()) {
                        // We don't care to do anything if we got the default costs
                    } else if (total.totalCost() > childCost.totalCost()) {
                        total = childCost;
                    }
                }

                if (null == total) {
                    return Cost.zero();
                }

                return total;
            }
            case ParserTreeConstants.JJTORNODE: {
                Cost total = Cost.zero();
                for (int i = 0; i < node.jjtGetNumChildren(); i++) {
                    Cost childCost = computeCostForSubtree(node.jjtGetChild(i));
                    total.incrementBy(childCost);
                }
                return total;
            }
            default: {
                if (1 == node.jjtGetNumChildren()) {
                    return computeCostForSubtree(node.jjtGetChild(0));
                } else {
                    return Cost.zero();
                }
            }
        }
    }

}
