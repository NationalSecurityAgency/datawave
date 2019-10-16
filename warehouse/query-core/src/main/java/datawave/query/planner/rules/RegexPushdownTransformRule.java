package datawave.query.planner.rules;

import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.ASTDelayedPredicate;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class RegexPushdownTransformRule implements NodeTransformRule {
    private static final Logger log = Logger.getLogger(RegexPushdownTransformRule.class);
    private List<Pattern> patterns = null;
    
    @Override
    public JexlNode apply(JexlNode node, ShardQueryConfiguration config) {
        if (node instanceof ASTERNode) {
            final String regex = String.valueOf(JexlASTHelper.getLiteralValue(node));
            if (patterns.stream().anyMatch(p -> p.matcher(regex).matches())) {
                if (JexlASTHelper.getIdentifierNames(node).contains(Constants.ANY_FIELD)) {
                    log.error("RegexPushdownTransformRule.apply: Not allowing _ANYFIELD =~ " + regex);
                    throw new DatawaveFatalQueryException("Not allowing _ANYFIELD =~ \" + regex");
                } else {
                    log.error("RegexPushdownTransformRule.apply: Forcing evaluation only for " + regex);
                    return ASTEvaluationOnly.create(node);
                }
            }
        }
        return node;
    }
    
    public void setRegexPatterns(List<String> patterns) {
        this.patterns = patterns.stream().map(Pattern::compile).collect(Collectors.toList());
    }
    
    public List<String> getPatterns() {
        return this.patterns.stream().map(p -> p.toString()).collect(Collectors.toList());
    }
}
