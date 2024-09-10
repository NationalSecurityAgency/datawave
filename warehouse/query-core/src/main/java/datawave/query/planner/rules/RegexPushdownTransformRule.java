package datawave.query.planner.rules;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EVALUATION_ONLY;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.log4j.Logger;

import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.util.MetadataHelper;

public class RegexPushdownTransformRule implements NodeTransformRule {
    private static final Logger log = Logger.getLogger(RegexPushdownTransformRule.class);
    private List<Pattern> patterns = null;

    @Override
    public JexlNode apply(JexlNode node, ShardQueryConfiguration config, MetadataHelper helper) {
        try {
            if (node instanceof ASTERNode || node instanceof ASTNRNode) {
                final String regex = String.valueOf(JexlASTHelper.getLiteralValue(node));
                if (patterns.stream().anyMatch(p -> p.matcher(regex).matches())) {
                    String identifier = JexlASTHelper.getIdentifier(node);
                    if (identifier.equals(Constants.ANY_FIELD) || helper.getNonEventFields(config.getDatatypeFilter()).contains(identifier)) {
                        log.error("RegexPushdownTransformRule.apply: Not allowing " + identifier + " =~ " + regex);
                        throw new DatawaveFatalQueryException("Not allowing " + identifier + " =~ " + regex);
                    } else {
                        log.error("RegexPushdownTransformRule.apply: Forcing evaluation only for " + regex);
                        return QueryPropertyMarker.create(node, EVALUATION_ONLY);
                    }
                }
            }
            return node;
        } catch (TableNotFoundException tnfe) {
            throw new DatawaveFatalQueryException("Failure to apply node transform rule", tnfe);
        }
    }

    public void setRegexPatterns(List<String> patterns) {
        this.patterns = patterns.stream().map(Pattern::compile).collect(Collectors.toList());
    }

    public List<String> getPatterns() {
        return this.patterns.stream().map(p -> p.toString()).collect(Collectors.toList());
    }
}
