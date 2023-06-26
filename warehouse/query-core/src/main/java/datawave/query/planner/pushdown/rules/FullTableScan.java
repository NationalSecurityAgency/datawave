package datawave.query.planner.pushdown.rules;

import java.util.List;

import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.parser.JavaRegexAnalyzer;
import datawave.query.parser.JavaRegexAnalyzer.JavaRegexParseException;
import datawave.query.planner.pushdown.Cost;

/**
 * Purpose: Delays scanning of leading and trailing wild cards when we have a top level and whose other children are indexed.
 *
 * Assumptions: Same as parent
 */
public class FullTableScan extends PushDownRule {

    private static final Logger log = Logger.getLogger(FullTableScan.class);

    @Override
    public Object visit(ASTAndNode node, Object data) {

        JexlNode returnNode = null;

        List<JexlNode> rewrittenNodes = Lists.newArrayList();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);
            JexlNode rewrittenNode = (JexlNode) child.jjtAccept(this, data);
            rewrittenNodes.add(rewrittenNode);

        }

        returnNode = JexlNodeFactory.createAndNode(rewrittenNodes);

        return returnNode;

    }

    protected JexlNode reverseDepth(JexlNode parentNode, List<JexlNode> delayedPredicates) {
        JexlNode returnNode = ASTDelayedPredicate.create(parentNode);
        JexlNode newAnd = new ASTAndNode(ParserTreeConstants.JJTANDNODE);
        int i = 0;
        for (JexlNode delayedNode : delayedPredicates) {
            newAnd.jjtAddChild(delayedNode.jjtGetChild(0), i);
            i++;
        }
        newAnd.jjtSetParent(returnNode);

        returnNode.jjtAddChild(newAnd, 0);

        return returnNode;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {

        /**
         * Only perform this action is we have an AND as a parent and the cost of our node is INFINITE. our getCost method will only return INFINITE if we have
         * a trailing and leading wildcard
         */
        if (isParent(node, ASTAndNode.class) && getCost(node) == Cost.INFINITE) {

            return ASTDelayedPredicate.create(node);

        }

        return super.visit(node, data);
    }

    @Override
    public Object visit(ASTJexlScript node, Object data) {
        return (ASTJexlScript) super.visit(node, data);
    }

    /*
     * (non-Javadoc)
     *
     * @see datawave.query.planner.pushdown.PushDown#getCost(org.apache.commons.jexl2.parser.JexlNode)
     */
    @Override
    public Cost getCost(JexlNode node) {
        String pattern = JexlASTHelper.getLiteralValue(node).toString();
        JavaRegexAnalyzer regex;
        try {
            regex = new JavaRegexAnalyzer(pattern);
            if (regex.isLeadingRegex() && regex.isTrailingRegex())
                return Cost.INFINITE;
        } catch (JavaRegexParseException e) {
            log.warn("Couldn't parse regex from ERNode: " + pattern);
        }
        return Cost.UNEVALUATED;

    }
}
