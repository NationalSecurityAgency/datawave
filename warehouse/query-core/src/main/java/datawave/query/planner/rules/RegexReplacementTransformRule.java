package datawave.query.planner.rules;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.functions.EvaluationPhaseFilterFunctions;
import datawave.query.jexl.functions.EvaluationPhaseFilterFunctionsDescriptor;
import datawave.query.jexl.functions.FunctionJexlNodeVisitor;
import datawave.query.util.MetadataHelper;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.JexlNode;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class is use to replace expressions within regex strings within a query tree.
 */
public class RegexReplacementTransformRule implements NodeTransformRule {
    private final Pattern pattern;
    private final String replacement;

    public RegexReplacementTransformRule(String pattern, String replacement) {
        this.pattern = Pattern.compile(pattern);
        this.replacement = replacement;
    }

    @Override
    public JexlNode apply(JexlNode node, ShardQueryConfiguration config, MetadataHelper helper) {
        if (node instanceof ASTERNode || node instanceof ASTNRNode) {
            JexlNode literal = JexlASTHelper.getLiteral(node);
            literal.image = processPattern(literal.image);
        } else if (node instanceof ASTFunctionNode) {
            FunctionJexlNodeVisitor functionMetadata = new FunctionJexlNodeVisitor();
            node.jjtAccept(functionMetadata, null);
            if (functionMetadata.namespace().equals(EvaluationPhaseFilterFunctions.EVAL_PHASE_FUNCTION_NAMESPACE)
                            && EvaluationPhaseFilterFunctionsDescriptor.EvaluationPhaseFilterJexlArgumentDescriptor.regexFunctions
                                            .contains(functionMetadata.name())) {
                JexlNode literal = JexlASTHelper.getLiteral(functionMetadata.args().get(1));
                literal.image = processPattern(literal.image);
            }
        }
        return node;
    }

    private String processPattern(String regex) {
        boolean changed = false;
        Matcher matcher = pattern.matcher(regex);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(sb, replacement);
            changed = true;
        }
        if (changed) {
            matcher.appendTail(sb);
            return sb.toString();
        }
        return regex;
    }
}
